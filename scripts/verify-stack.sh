#!/usr/bin/env bash
#
# Assert that the running stack actually produces the telemetry this project
# claims it does. Run against a stack brought up with `docker compose up -d`.
#
#   ./scripts/verify-stack.sh
#
# The in-memory span tests (go test ./..., npm test) prove the propagation
# logic. This script proves the *wiring*: that the spans leave the processes,
# survive the collector's tail sampler, land in Tempo as one connected trace,
# that the log lines land in Loki carrying the same trace id, and that the
# latency histogram in Prometheus carries a clickable exemplar.
#
# Every check asserts on content the stack produced -- a span named X from
# service Y, a log line whose trace_id equals the one we were handed -- never
# merely that an endpoint answered. An HTTP 200 from Tempo with an empty trace
# is the exact failure this script exists to catch.
set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8080}"
TEMPO="${TEMPO:-http://localhost:3200}"
LOKI="${LOKI:-http://localhost:3100}"
PROMETHEUS="${PROMETHEUS:-http://localhost:9090}"

# Tempo needs trace_idle_period (2s) plus a flush before a trace is queryable,
# and the collector's tail sampler holds every trace for decision_wait (5s).
# 90s of polling is generous but a cold CI runner pulling images is slow.
TRACE_TIMEOUT="${TRACE_TIMEOUT:-90}"
LOG_TIMEOUT="${LOG_TIMEOUT:-90}"
EXEMPLAR_TIMEOUT="${EXEMPLAR_TIMEOUT:-90}"

failures=0

pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; failures=$((failures + 1)); }
info() { printf '        %s\n' "$1"; }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }

need() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'verify-stack.sh needs %s on PATH\n' "$1" >&2
    exit 2
  }
}
need curl
need jq

# ---------------------------------------------------------------------------
step '1. Waiting for the stack'
# ---------------------------------------------------------------------------
wait_for() {
  local name="$1" url="$2" deadline=$((SECONDS + 120))
  while ((SECONDS < deadline)); do
    if curl -fsS -o /dev/null --max-time 3 "$url" 2>/dev/null; then
      pass "$name is up"
      return 0
    fi
    sleep 2
  done
  fail "$name did not become ready at $url within 120s"
  return 1
}

wait_for 'gateway' "$GATEWAY/healthz" || exit 1
wait_for 'tempo' "$TEMPO/ready" || exit 1
wait_for 'loki' "$LOKI/ready" || exit 1
wait_for 'prometheus' "$PROMETHEUS/-/ready" || exit 1
wait_for 'fulfillment worker' "${FULFILLMENT:-http://localhost:8083}/healthz" || exit 1

# ---------------------------------------------------------------------------
step '2. Driving one successful checkout'
# ---------------------------------------------------------------------------
# X-Sampling-Priority: high is not optional here. The collector's tail sampler
# keeps only 20% of fast, successful traces, so asserting on *this* trace
# without it is an 80% chance of a red build that is reporting correct
# behaviour. The header sets sampling.priority.high on the gateway's server
# span, which the `explicitly-requested` policy matches.
checkout() {
  local sku="$1" qty="${2:-1}"
  curl -sS -X POST "$GATEWAY/checkout" \
    -H 'Content-Type: application/json' \
    -H 'X-Sampling-Priority: high' \
    -D /tmp/verify-headers -o /tmp/verify-body -w '%{http_code}' \
    -d "{\"sku\":\"$sku\",\"quantity\":$qty,\"customer\":\"verify\"}"
}

status="$(checkout SKU-TEA 2)"
trace_id="$(tr -d '\r' < /tmp/verify-headers | awk 'tolower($1) == "x-trace-id:" { print $2 }' | head -n1)"

if [[ "$status" == "202" ]]; then
  pass "gateway accepted the checkout (202)"
else
  fail "gateway returned $status, want 202"
  info "body: $(cat /tmp/verify-body)"
fi

if [[ "$trace_id" =~ ^[0-9a-f]{32}$ ]]; then
  pass "response carries X-Trace-Id: $trace_id"
else
  fail "X-Trace-Id is missing or malformed: '${trace_id:-<empty>}'"
  exit 1
fi

body_trace="$(jq -r '.trace_id // ""' < /tmp/verify-body)"
if [[ "$body_trace" == "$trace_id" ]]; then
  pass 'response body trace_id matches the header'
else
  fail "body trace_id '$body_trace' does not match header '$trace_id'"
fi

# A second checkout that fails inside the Node worker, on the far side of the
# queue, so the error-trace assertion exercises the async boundary too.
worker_fail_status="$(checkout FAIL-CRATE 1)"
worker_fail_trace="$(tr -d '\r' < /tmp/verify-headers | awk 'tolower($1) == "x-trace-id:" { print $2 }' | head -n1)"
if [[ "$worker_fail_status" == "202" ]]; then
  pass "checkout of a FAIL- order was accepted (202); it fails later, in the worker"
else
  fail "FAIL- checkout returned $worker_fail_status, want 202"
fi

# A third that fails synchronously in the Go inventory service.
inv_fail_status="$(checkout ERR-BROKEN 1)"
inv_fail_trace="$(tr -d '\r' < /tmp/verify-headers | awk 'tolower($1) == "x-trace-id:" { print $2 }' | head -n1)"
if [[ "$inv_fail_status" == "500" ]]; then
  pass 'checkout of an ERR- order failed with 500, as designed'
else
  fail "ERR- checkout returned $inv_fail_status, want 500"
fi

# ---------------------------------------------------------------------------
step '3. The trace arrives in Tempo, connected, with every service in it'
# ---------------------------------------------------------------------------

# Tempo serves OTLP JSON when asked for it; the protobuf default is unhelpful in
# a shell script.
#
# The envelope has changed across Tempo versions -- v1 returns {"batches": [...]},
# v2 returns {"trace": {"resourceSpans": [...]}} -- so the fetch tries v2 first
# and falls back, and JQ_GROUPS below normalises whichever arrives. Hard-coding one
# shape means a working stack reports zero spans, which is indistinguishable
# from real data loss.
# iserror exists because protojson renders an enum by NAME rather than by
# number: an Error span comes back as {"status":{"code":"STATUS_CODE_ERROR"}},
# so `.status.code == 2` silently matches nothing and the check reports "no
# error spans" for a trace full of them. Accept both forms.
JQ_GROUPS='def groups: (.trace.resourceSpans // .resourceSpans // .batches // []);
def iserror: (.status.code // 0) | (. == 2 or . == "STATUS_CODE_ERROR");
groups[]'

fetch_trace() {
  local id="$1" body
  for path in "api/v2/traces" "api/traces"; do
    body="$(curl -sS -H 'Accept: application/json' --max-time 10 "$TEMPO/$path/$id" 2>/dev/null || true)"
    if [[ -n "$body" ]] && printf '%s' "$body" | jq -e "[${JQ_GROUPS}] | length > 0" >/dev/null 2>&1; then
      printf '%s' "$body"
      return 0
    fi
  done
  printf '%s' "${body:-}"
}

# The `groups[]` iteration is baked into JQ_GROUPS rather than appended at each
# use site, so no expansion is ever followed by a `[` -- which both the shell
# and shellcheck would read as an array subscript.

# Poll until the trace has at least the expected number of spans. Polling for
# "the endpoint returned 200" is not enough: Tempo answers with an empty or
# partial trace while the ingester is still receiving spans, and a check that
# accepted that would pass before the worker had done anything.
poll_trace() {
  local id="$1" want_spans="$2" deadline=$((SECONDS + TRACE_TIMEOUT)) spans=0
  while ((SECONDS < deadline)); do
    local json
    json="$(fetch_trace "$id")"
    if [[ -n "$json" ]] && printf '%s' "$json" | jq -e "[${JQ_GROUPS}] | length > 0" >/dev/null 2>&1; then
      spans="$(printf '%s' "$json" | jq "[${JQ_GROUPS} | .scopeSpans[]? | .spans[]?] | length")"
      if ((spans >= want_spans)); then
        printf '%s' "$json" > /tmp/verify-trace.json
        return 0
      fi
    fi
    sleep 3
  done
  printf '%s' "$(fetch_trace "$id")" > /tmp/verify-trace.json
  info "last seen: $spans spans after ${TRACE_TIMEOUT}s"
  return 1
}

# The full topology for one checkout:
#   gateway   POST /checkout, POST /orders (client)
#   orders    POST /orders, POST /reserve (client), orders.insert,
#             orders.fulfil publish
#   inventory POST /reserve, inventory.reserve
#   worker    orders.fulfil process, fulfillment.pick_and_pack, POST /commit
#   inventory POST /commit
# = 13 spans. Asserting on 10 leaves slack for Tempo returning the trace mid
# flush, while still being impossible to satisfy without the worker's spans.
if poll_trace "$trace_id" 10; then
  pass "Tempo returned trace $trace_id"
else
  fail "trace $trace_id never reached Tempo with at least 10 spans"
  info "collector and tempo logs follow:"
  docker compose logs --tail 60 otel-collector tempo 2>/dev/null || true
  exit 1
fi

span_count="$(jq "[${JQ_GROUPS} | .scopeSpans[]? | .spans[]?] | length" < /tmp/verify-trace.json)"
info "$span_count spans in the trace"

services="$(jq -r "
  [${JQ_GROUPS} | .resource.attributes[]?
   | select(.key == \"service.name\") | .value.stringValue] | sort | unique | .[]
" < /tmp/verify-trace.json)"

for want in gateway orders inventory fulfillment; do
  if printf '%s\n' "$services" | grep -qx "$want"; then
    pass "service.name=$want is in the trace"
  else
    fail "service.name=$want is NOT in the trace; found: $(printf '%s' "$services" | tr '\n' ' ')"
  fi
done

span_names="$(jq -r "${JQ_GROUPS} | .scopeSpans[]? | .spans[]? | .name" < /tmp/verify-trace.json | sort)"
for want in \
  'POST /checkout' \
  'POST /orders' \
  'POST /reserve' \
  'inventory.reserve' \
  'orders.insert' \
  'orders.fulfil publish' \
  'orders.fulfil process' \
  'fulfillment.pick_and_pack' \
  'POST /commit'
do
  if printf '%s\n' "$span_names" | grep -qxF "$want"; then
    pass "span '$want' is present"
  else
    fail "span '$want' is missing; spans: $(printf '%s' "$span_names" | tr '\n' '|')"
  fi
done

# The headline assertion: the Node worker's consumer span must be parented on
# the Go publish span. If the async hop lost the context this is where it shows,
# because the worker's spans would be in a different trace entirely and the
# per-service check above would already have failed -- but this pins the exact
# parent link rather than mere co-location.
publish_span_id="$(jq -r "
  [${JQ_GROUPS} | .scopeSpans[]? | .spans[]? | select(.name == \"orders.fulfil publish\")][0].spanId // \"\"
" < /tmp/verify-trace.json)"
process_parent_id="$(jq -r "
  [${JQ_GROUPS} | .scopeSpans[]? | .spans[]? | select(.name == \"orders.fulfil process\")][0].parentSpanId // \"\"
" < /tmp/verify-trace.json)"

if [[ -n "$publish_span_id" && "$publish_span_id" == "$process_parent_id" ]]; then
  pass "the Node consumer span's parent is the Go publish span ($publish_span_id)"
else
  fail "consumer parent '$process_parent_id' is not the publish span '$publish_span_id': the queue hop lost the parent"
fi

# The sampling request must be visible on the span, or the trace above was kept
# by luck rather than by the policy -- and the next run would be a coin flip.
priority="$(jq -r "
  [${JQ_GROUPS} | .scopeSpans[]? | .spans[]? | .attributes[]?
   | select(.key == \"sampling.priority.high\") | .value.boolValue] | .[0] // empty
" < /tmp/verify-trace.json)"
if [[ "$priority" == "true" ]]; then
  pass 'sampling.priority.high=true is on the trace, so the tail sampler kept it deliberately'
else
  fail "sampling.priority.high is not set on the trace (got '${priority:-<absent>}'); this trace survived 20% probability by luck"
fi

# Resource attributes the dashboards group by.
for attr in service.version deployment.environment.name; do
  count="$(jq --arg k "$attr" "
    [${JQ_GROUPS} | .resource.attributes[]? | select(.key == \$k)] | length
  " < /tmp/verify-trace.json)"
  batches="$(jq "[${JQ_GROUPS}] | length" < /tmp/verify-trace.json)"
  if ((count >= batches)); then
    pass "every resource in the trace carries $attr"
  else
    fail "$attr is on only $count of $batches resources"
  fi
done

# ---------------------------------------------------------------------------
step '4. The error trace records an exception, not just a string'
# ---------------------------------------------------------------------------
if poll_trace "$inv_fail_trace" 5; then
  pass "Tempo returned the failing trace $inv_fail_trace"

  error_spans="$(jq "[${JQ_GROUPS} | .scopeSpans[]? | .spans[]? | select(iserror)] | length" < /tmp/verify-trace.json)"
  if ((error_spans > 0)); then
    pass "$error_spans span(s) carry status=ERROR, so Tempo's error filter and the tail sampler see it"
  else
    fail 'no span in the failing trace has status=ERROR'
  fi

  exception_stacks="$(jq "
    [${JQ_GROUPS} | .scopeSpans[]? | .spans[]? | (.events // [])[]
     | select(.name == \"exception\")
     | .attributes[]? | select(.key == \"exception.stacktrace\")
     | select(.value.stringValue | length > 0)] | length
  " < /tmp/verify-trace.json)"
  if ((exception_stacks > 0)); then
    pass 'an exception event carries a non-empty exception.stacktrace'
  else
    fail 'no exception event with a stack trace in the failing trace'
  fi
else
  fail "the failing trace $inv_fail_trace never reached Tempo"
fi

# The worker-side failure: the error is on the far side of the queue.
if poll_trace "$worker_fail_trace" 8; then
  worker_error="$(jq "
    [${JQ_GROUPS} as \$g
     | \$g.resource.attributes[]? | select(.key == \"service.name\" and .value.stringValue == \"fulfillment\")
     | \$g.scopeSpans[]? | .spans[]? | select(iserror)] | length
  " < /tmp/verify-trace.json)"
  if ((worker_error > 0)); then
    pass 'the Node worker recorded its failure as an ERROR span inside the gateway trace'
  else
    fail 'the worker-side failure is not an ERROR span in the original trace'
  fi
else
  fail "the worker-failure trace $worker_fail_trace never reached Tempo"
fi

# ---------------------------------------------------------------------------
step '5. Loki has log lines carrying the same trace id'
# ---------------------------------------------------------------------------
loki_query() {
  local query="$1" start end
  start="$(( ($(date +%s) - 900) * 1000000000 ))"
  end="$(( ($(date +%s) + 60) * 1000000000 ))"
  curl -sS --max-time 10 -G "$LOKI/loki/api/v1/query_range" \
    --data-urlencode "query=$query" \
    --data-urlencode "start=$start" \
    --data-urlencode "end=$end" \
    --data-urlencode 'limit=200' 2>/dev/null || true
}

poll_loki() {
  local query="$1" deadline=$((SECONDS + LOG_TIMEOUT))
  while ((SECONDS < deadline)); do
    local json entries
    json="$(loki_query "$query")"
    entries="$(printf '%s' "$json" | jq '[.data.result[]?.values[]?] | length' 2>/dev/null || echo 0)"
    if [[ "$entries" =~ ^[0-9]+$ ]] && ((entries > 0)); then
      printf '%s' "$json" > /tmp/verify-logs.json
      return 0
    fi
    sleep 3
  done
  printf '%s' "$(loki_query "$query")" > /tmp/verify-logs.json
  return 1
}

# The filter is on structured metadata, which is where Loki's OTLP endpoint puts
# trace_id. A line matching this query proves the whole chain: the service put
# the trace id on the record, the collector forwarded it, and Loki kept it.
if poll_loki "{service_name=\"orders\"} | trace_id=\`$trace_id\`"; then
  lines="$(jq '[.data.result[].values[]] | length' < /tmp/verify-logs.json)"
  pass "$lines log line(s) from orders are tagged with trace_id=$trace_id"
else
  fail "no orders log line in Loki carries trace_id=$trace_id"
  info "all orders labels: $(curl -sS "$LOKI/loki/api/v1/labels" 2>/dev/null | jq -c '.data // []')"
  docker compose logs --tail 40 loki otel-collector 2>/dev/null || true
fi

# The same, from the Node worker, which proves the correlation is implemented in
# both runtimes and not only in Go.
if poll_loki "{service_name=\"fulfillment\"} | trace_id=\`$trace_id\`"; then
  lines="$(jq '[.data.result[].values[]] | length' < /tmp/verify-logs.json)"
  pass "$lines log line(s) from the Node worker are tagged with the same trace_id"
else
  fail "no fulfillment log line in Loki carries trace_id=$trace_id"
fi

# And the labels the dashboards group by must be real Loki labels, not metadata.
labels="$(curl -sS --max-time 10 "$LOKI/loki/api/v1/labels" 2>/dev/null | jq -r '.data[]?' || true)"
for want in service_name service_version deployment_environment_name; do
  if printf '%s\n' "$labels" | grep -qx "$want"; then
    pass "Loki label '$want' exists"
  else
    fail "Loki has no '$want' label; found: $(printf '%s' "$labels" | tr '\n' ' ')"
  fi
done

# ---------------------------------------------------------------------------
step '6. Prometheus kept an exemplar pointing at a real trace'
# ---------------------------------------------------------------------------
poll_exemplars() {
  local deadline=$((SECONDS + EXEMPLAR_TIMEOUT))
  while ((SECONDS < deadline)); do
    local json ids
    json="$(curl -sS --max-time 10 -G "$PROMETHEUS/api/v1/query_exemplars" \
      --data-urlencode 'query=http_request_duration_seconds_bucket' \
      --data-urlencode "start=$(( $(date +%s) - 900 ))" \
      --data-urlencode "end=$(( $(date +%s) + 60 ))" 2>/dev/null || true)"
    ids="$(printf '%s' "$json" | jq -r '[.data[]?.exemplars[]?.labels.trace_id] | length' 2>/dev/null || echo 0)"
    if [[ "$ids" =~ ^[0-9]+$ ]] && ((ids > 0)); then
      printf '%s' "$json" > /tmp/verify-exemplars.json
      return 0
    fi
    sleep 3
  done
  return 1
}

if poll_exemplars; then
  count="$(jq '[.data[].exemplars[]] | length' < /tmp/verify-exemplars.json)"
  pass "$count exemplar(s) on http_request_duration_seconds_bucket"

  # An exemplar is only useful if the trace it names is in Tempo. An exemplar
  # pointing at a trace the tail sampler dropped is a link to a 404, which
  # looks like data loss.
  exemplar_trace="$(jq -r '[.data[].exemplars[].labels.trace_id] | .[0]' < /tmp/verify-exemplars.json)"
  if [[ "$exemplar_trace" =~ ^[0-9a-f]{32}$ ]]; then
    pass "exemplar trace id is well-formed: $exemplar_trace"
  else
    fail "exemplar trace id is malformed: '$exemplar_trace'"
  fi

  resolvable=0
  for candidate in $(jq -r '[.data[].exemplars[].labels.trace_id] | unique | .[0:10][]' < /tmp/verify-exemplars.json); do
    if fetch_trace "$candidate" | jq -e "[${JQ_GROUPS}] | length > 0" >/dev/null 2>&1; then
      resolvable=1
      pass "exemplar $candidate resolves to a trace in Tempo"
      break
    fi
  done
  if ((resolvable == 0)); then
    fail 'no exemplar resolves to a trace in Tempo: the metric-to-trace link is broken'
  fi
else
  fail 'Prometheus has no exemplars; is --enable-feature=exemplar-storage set and is /metrics serving OpenMetrics?'
  info "$(curl -sS --max-time 5 "$PROMETHEUS/api/v1/targets?state=active" 2>/dev/null | jq -c '[.data.activeTargets[] | {job: .labels.job, health}]' || true)"
fi

# The collector's spanmetrics series, which is the only metrics path the Node
# worker has. Polled, because the connector flushes on its own 15s interval and
# Prometheus then has to scrape it.
spanmetrics=''
poll_spanmetrics() {
  local deadline=$((SECONDS + EXEMPLAR_TIMEOUT))
  while ((SECONDS < deadline)); do
    spanmetrics="$(curl -sS --max-time 10 -G "$PROMETHEUS/api/v1/query" \
      --data-urlencode 'query=count by (service_name) (traces_span_metrics_calls_total)' 2>/dev/null || true)"
    local n
    n="$(printf '%s' "$spanmetrics" | jq -r '
      [.data.result[]? | select(.metric.service_name == "fulfillment")] | length
    ' 2>/dev/null || echo 0)"
    if [[ "$n" =~ ^[0-9]+$ ]] && ((n > 0)); then
      return 0
    fi
    sleep 5
  done
  return 1
}

if poll_spanmetrics; then
  pass 'the collector derived spanmetrics for the Node worker'
else
  fail 'no traces_span_metrics_calls_total series for service_name=fulfillment'
  info "series found: $(printf '%s' "$spanmetrics" | jq -c '[.data.result[]?.metric.service_name]' 2>/dev/null || echo '[]')"
fi

# ---------------------------------------------------------------------------
step '7. The collector dropped nothing'
# ---------------------------------------------------------------------------
dropped="$(curl -sS --max-time 10 -G "$PROMETHEUS/api/v1/query" \
  --data-urlencode 'query=sum(otelcol_exporter_send_failed_spans_total)' 2>/dev/null |
  jq -r '.data.result[0].value[1] // "0"')"
if [[ "${dropped%%.*}" == "0" ]]; then
  pass 'otelcol_exporter_send_failed_spans_total is 0'
else
  fail "the collector failed to export $dropped spans"
fi

# ---------------------------------------------------------------------------
if ((failures == 0)); then
  printf '\n\033[32mAll stack checks passed.\033[0m\n'
  exit 0
fi
printf '\n\033[31m%s check(s) failed.\033[0m\n' "$failures"
exit 1
