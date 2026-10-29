#!/usr/bin/env bash
#
# Inject latency and failures, then drive traffic through the degraded stack, so
# the traces in Tempo show something worth looking at.
#
# Without this, every trace is a flat 5ms bar: the latency histogram has one
# occupied bucket, the tail sampler never sees an error to keep, and the
# exemplar you click leads to a boring trace.
#
#   ./scripts/chaos.sh                    # 25% slow, 10% failing, 60 requests
#   ./scripts/chaos.sh --latency 50 --faults 25 --count 120
#   ./scripts/chaos.sh --off              # restore and leave the stack healthy
#
# Everything the injector does is recorded on the affected span as
# chaos.injected_latency_ms / chaos.injected_fault, so injected pain is
# distinguishable from real pain in Grafana. The collector keeps 100% of traces
# carrying chaos.injected_fault, so a demo run is always fully visible.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

LATENCY_PCT=25
FAULT_PCT=10
LATENCY_MIN_MS=150
LATENCY_MAX_MS=1500
COUNT=60
OFF=0

usage() {
  sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//; $d'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --latency) LATENCY_PCT="$2"; shift 2 ;;
    --faults) FAULT_PCT="$2"; shift 2 ;;
    --min-ms) LATENCY_MIN_MS="$2"; shift 2 ;;
    --max-ms) LATENCY_MAX_MS="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    --off) OFF=1; shift ;;
    -h | --help) usage 0 ;;
    *) printf 'unknown argument: %s\n\n' "$1" >&2; usage 1 ;;
  esac
done

compose() { docker compose "$@"; }

# The injectors read their configuration from the environment, so turning the
# dials means recreating the three Go services -- not rebuilding them. `up -d`
# with changed environment does exactly that and leaves the backends alone, so
# the traces already in Tempo survive for comparison.
apply() {
  CHAOS_LATENCY_PCT="$1" \
  CHAOS_FAULT_PCT="$2" \
  CHAOS_LATENCY_MIN_MS="$3" \
  CHAOS_LATENCY_MAX_MS="$4" \
    compose up -d --no-deps --force-recreate gateway orders inventory >/dev/null
}

wait_healthy() {
  local deadline=$((SECONDS + 90))
  while ((SECONDS < deadline)); do
    if curl -fsS -o /dev/null "http://localhost:8080/healthz" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  printf 'gateway did not come back up within 90s\n' >&2
  return 1
}

if ((OFF)); then
  printf 'Disabling chaos and recreating the application services...\n'
  apply 0 0 "$LATENCY_MIN_MS" "$LATENCY_MAX_MS"
  wait_healthy
  printf 'Chaos is off. The stack is healthy again.\n'
  exit 0
fi

cat <<EOF
Injecting chaos into gateway, orders and inventory:

  latency   ${LATENCY_PCT}% of requests, ${LATENCY_MIN_MS}-${LATENCY_MAX_MS}ms
  faults    ${FAULT_PCT}% of requests fail with 503
  traffic   ${COUNT} requests

EOF

apply "$LATENCY_PCT" "$FAULT_PCT" "$LATENCY_MIN_MS" "$LATENCY_MAX_MS"
wait_healthy
printf 'Services recreated with chaos enabled. Driving traffic...\n\n'

./scripts/traffic.sh "$COUNT"

cat <<EOF

Chaos is still enabled. Run './scripts/chaos.sh --off' to restore the stack.

What to look at:
  * Latency panel on http://localhost:3000/d/otel-red -- the p99 should have
    moved into the 1-2s buckets. Click an exemplar diamond on a slow point.
  * Filter Tempo for errors:  { status = error }
  * Exclude the injected ones to see only real failures:
      { status = error && span.chaos.injected_fault = nil }
  * A FAIL- order fails *after* the queue, in the Node worker: the error span is
    a child of the Go publish span, several seconds later in the waterfall.
EOF
