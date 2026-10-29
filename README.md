# otel-microservices

Four services, two languages, one trace — with the plumbing that usually gets
half-done written out and tested.

Adding an OpenTelemetry SDK takes an afternoon. What takes longer, and what
this repository is actually about, is the handful of things that have to be
right before the three signals are worth anything:

| What | Where | Why it is usually broken |
|---|---|---|
| W3C context across an **async** hop | [`internal/queue`](internal/queue/queue.go), [`src/queue.ts`](services/fulfillment/src/queue.ts) | A queue has no headers unless you put them there, so the worker starts a root span and you get one orphan trace per job |
| `trace_id` on **every log line** | [`internal/obs/slog.go`](internal/obs/slog.go), [`src/logger.ts`](services/fulfillment/src/logger.ts) | Traces and logs end up as two separate products, and an incident means reading both and guessing |
| **Exemplars**: latency metric → example trace | [`internal/metrics`](internal/metrics/metrics.go) | Three things have to line up (`ObserveWithExemplar`, OpenMetrics negotiation, `--enable-feature=exemplar-storage`); miss any one and it silently does nothing |
| **Tail** sampling, not head | [`deploy/collector/config.yaml`](deploy/collector/config.yaml) | Head sampling at 10% keeps 10% of your errors, which are the only traces anyone looks for |
| Errors as **exception events** | [`internal/spanerr`](internal/spanerr/spanerr.go) | `attribute.String("error", err.Error())` leaves the span status Unset, so Tempo's error filter and the tail sampler both miss it |
| Resource attributes from **one place** | [`internal/obs/resource.go`](internal/obs/resource.go), [`src/telemetry.ts`](services/fulfillment/src/telemetry.ts) | Set in four files they drift, and "group by `service.version`" shows four series for one deploy |

Each row is asserted by tests that run without Docker — see
[Testing](#testing), and the honest list of
[what is *not* verified](#limitations).

---

## Architecture

```
                      ┌──────────────────────────────────────────────┐
   POST /checkout      │                                              │
  ─────────────────▶ gateway ──http──▶ orders ──http──▶ inventory      │
        (Go)            │   (Go)         │    (Go)         ▲          │
                        │                │                 │          │
                        │                │ RPUSH           │ POST     │
                        │                ▼                 │ /commit  │
                        │            ┌───────┐             │          │
                        │            │ redis │             │          │
                        │            │ list  │             │          │
                        │            └───┬───┘             │          │
                        │         BLPOP  │                 │          │
                        │                ▼                 │          │
                        │          fulfillment ────────────┘          │
                        │          (Node / TS)                        │
                        └──────────────────┬───────────────────────────┘
                                           │ OTLP/HTTP  (traces + logs)
                                           ▼
                                  ┌─────────────────┐
                                  │ otel-collector  │
                                  │                 │
                                  │ tail_sampling   │  keep 100% of errors
                                  │ spanmetrics     │  and slow traces,
                                  │ batch           │  20% of the rest
                                  └────┬───┬────┬───┘
                        traces ◀───────┘   │    └───────▶ :8889 scraped
                             │             │ logs              by prometheus
                             ▼             ▼
                        ┌────────┐    ┌────────┐    ┌────────────┐
                        │ tempo  │    │  loki  │    │ prometheus │
                        └───┬────┘    └───┬────┘    └──────┬─────┘
                            └─────────────┼────────────────┘
                                          ▼
                                     ┌─────────┐
                                     │ grafana │  trace ⇄ logs ⇄ metrics
                                     └─────────┘
```

### Why this topology

A trace is only interesting if it has something in it. One checkout produces
**13 spans** across four processes and two runtimes:

| # | Span | Service | Kind | Note |
|--:|---|---|---|---|
| 1 | `POST /checkout` | gateway | server | starts the trace |
| 2 | `POST /orders` | gateway | client | injects `traceparent` |
| 3 | `POST /orders` | orders | server | remote parent = 2 |
| 4 | `POST /reserve` | orders | client | |
| 5 | `POST /reserve` | inventory | server | remote parent = 4 |
| 6 | `inventory.reserve` | inventory | client | the datastore access |
| 7 | `orders.insert` | orders | client | the datastore write |
| 8 | `orders.fulfil publish` | orders | **producer** | injects into the message |
| 9 | `orders.fulfil process` | fulfillment | **consumer** | **remote parent = 8, in Node** |
| 10 | `fulfillment.pick_and_pack` | fulfillment | internal | |
| 11 | `POST /commit` | fulfillment | client | injects, from Node |
| 12 | `POST /commit` | inventory | server | remote parent = 11, back in Go |
| 13 | *(queue gap)* | | | visible as the time between 8 and 9 |

Span 9 is the one that matters. It is the hop where a naive implementation
loses the parent, and it crosses a runtime boundary at the same time, which is
the realistic case: nobody's estate is one language.

---

## Quickstart

Requires Docker with Compose v2. Nothing else.

```bash
docker compose up --build --wait
```

Then generate traffic — the script prints a trace id per request:

```bash
./scripts/traffic.sh 20
```

```
n      sku          status trace_id
-----  -----------  ------ --------------------------------
1      SKU-TEA      202    4bf92f3577b34da6a3ce929d0e0e4736
2      OOS-MATCHA   409    7c1e8a0b3d5f4e2a9b8c7d6e5f4a3b2c
3      ERR-BROKEN   500    1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d
...
```

Open Grafana at <http://localhost:3000> (anonymous admin, no login):

1. **[Trace and log correlation](http://localhost:3000/d/otel-correlation)** —
   paste a trace id into the box at the top. You get the whole waterfall *and*
   every log line from every service that touched the request, in one view.
   Click a span → *Logs for this span*. Click a log line's `TraceID` → back to
   the trace.
2. **[RED metrics and exemplars](http://localhost:3000/d/otel-red)** — turn on
   exemplars on the latency panel (the dot icon in the legend) and click a
   diamond. It opens a trace of a request in that bucket.

Then make it interesting:

```bash
./scripts/chaos.sh --latency 40 --faults 15 --count 100
```

and look at the latency panel again. To turn it off: `./scripts/chaos.sh --off`.

To assert the whole stack is actually wired up (this is what CI runs):

```bash
./scripts/verify-stack.sh
```

### Endpoints

| Service | URL | |
|---|---|---|
| gateway | <http://localhost:8080> | `POST /checkout`, `GET /orders/{id}` |
| orders | <http://localhost:8081> | `POST /orders`, `GET /orders/{id}` |
| inventory | <http://localhost:8082> | `POST /reserve`, `POST /commit`, `GET /stock/{sku}` |
| fulfillment | <http://localhost:8083> | `/healthz` only; it is a worker |
| Grafana | <http://localhost:3000> | |
| Tempo | <http://localhost:3200> | `/api/traces/{id}`, `/api/search?q=...` |
| Loki | <http://localhost:3100> | |
| Prometheus | <http://localhost:9090> | |
| Collector zpages | <http://localhost:55679/debug/tracez> | |

Every service also serves `/healthz`, `/readyz` and `/metrics`. Those three are
mounted *outside* the tracing middleware — a container probe every two seconds
would otherwise be the overwhelming majority of spans in Tempo.

### SKUs that do interesting things

The catalogue is deliberately full of trapdoors, so one `traffic.sh` run
produces every trace shape:

| SKU | Outcome | What it demonstrates |
|---|---|---|
| `SKU-TEA`, `SKU-COFFEE` | 202 | the happy path, 13 spans |
| `SKU-COCOA` | 202 then 409 | only 8 in stock, so it runs out |
| `OOS-MATCHA` | 409 | **client** fault: exception recorded, span status left **Unset** |
| `ERR-BROKEN` | 500 | a panic inside `inventory.reserve`, recovered **in the span that failed**, with a stack trace |
| `FAIL-CRATE` | 202, then fails | fails in the **Node worker**, on the far side of the queue — the error span is a child of the Go publish span, seconds later |
| `SKU-NOPE` | 404 | unknown SKU |

---

## The six mechanisms, in detail

### 1. W3C trace context across the async hop

HTTP propagation is free: every instrumentation library knows to read the
`traceparent` header. A queue knows nothing, so you have to carry it yourself.
There are three parts, and missing any one produces a differently-broken trace.

**Inject from inside the producer span, not from the caller's context**
([`internal/queue/queue.go`](internal/queue/queue.go)):

```go
ctx, span := p.tracer.Start(ctx, topic+" publish", trace.WithSpanKind(trace.SpanKindProducer), ...)
defer span.End()

// After Start, so the traceparent on the wire names *this* span.
p.propagator.Inject(ctx, CarrierFor(&msg))
```

Inject before `Start` and the consumer becomes a sibling of the publish rather
than its child, and the queue wait disappears from the waterfall.

**Extract into a fresh root, carrying over cancellation**:

```go
func (c *Consumer) ConsumeContext(parent context.Context, msg Message) context.Context {
	root := trace.ContextWithSpanContext(parent, trace.SpanContext{})
	return c.propagator.Extract(root, CarrierFor(&msg))
}
```

Both halves matter. Extract into `context.Background()` and the worker's
`SIGTERM` never reaches the handler, so shutdown becomes a timeout and the
unended spans look exactly like a propagation bug. Pass the loop's context
without extracting — the default if you do nothing — and every job in the
worker's lifetime hangs off its startup span.

**Refuse to ship an uncorrelated message.** If the propagator produced no
`traceparent`, `Publish` fails rather than enqueueing:

```go
if _, ok := msg.Headers["traceparent"]; !ok {
	return msg, spanerr.RecordServer(span, errors.New("queue: propagator produced no traceparent; was obs.Setup called?"))
}
```

The Node side is the mirror image
([`src/queue.ts`](services/fulfillment/src/queue.ts)) and needs two more
things that are easy to forget: `AsyncLocalStorageContextManager`, without
which `context.active()` returns the root after the first `await`; and a
`ParentBasedSampler`, without which the worker re-rolls a decision the Go
gateway already made.

### 2. Trace and log correlation

This is the single most valuable thing in the stack and the most commonly
half-done. Both runtimes put `trace_id`, `span_id` and `trace_flags` on every
record logged inside a span, in identical keys.

Go, as a `slog.Handler` decorator so it composes with `JSONHandler`,
`TextHandler` and the OTLP bridge alike:

```go
func (h *TraceHandler) Handle(ctx context.Context, rec slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec = rec.Clone()
		rec.AddAttrs(
			slog.String(LogKeyTraceID, sc.TraceID().String()),
			slog.String(LogKeySpanID, sc.SpanID().String()),
			slog.String(LogKeyFlags, sc.TraceFlags().String()),
		)
	}
	return h.inner.Handle(ctx, rec)
}
```

Note the `IsValid()` guard. A line logged outside any span gets **no** ids:
writing `trace_id="00000000000000000000000000000000"` makes every unrelated
startup line look like one enormous trace.

One `slog` call reaches two sinks — the container's stdout and the OTLP log
pipeline — via a fanout handler, so the call site does not know there are two
destinations.

In Loki, the ids live in **structured metadata**, not labels. A trace id as a
label would mean one Loki stream per trace. That is why
[`loki.yaml`](deploy/loki/loki.yaml) sets `allow_structured_metadata: true` and
promotes exactly three resource attributes to labels, and why the Grafana link
filters on metadata:

```
{service_name="$${__span.tags["service.name"]}"} | trace_id="$${__span.traceId}"
```

Both directions are provisioned in
[`datasources.yaml`](deploy/grafana/provisioning/datasources/datasources.yaml):
`tracesToLogsV2` for span → logs, and `derivedFields` for log → trace.

### 3. Exemplars: from a latency metric to a slow trace

An exemplar is a trace id attached to one observation in a histogram bucket. It
turns "p99 is 3 seconds" into "here is a trace of one of those requests".
**Three** things have to line up, and each fails silently on its own:

```go
// 1. ObserveWithExemplar, not Observe. Observe discards the trace id, no error.
eo.ObserveWithExemplar(v, prometheus.Labels{"trace_id": sc.TraceID().String()})

// 2. OpenMetrics negotiation. Exemplars have no representation in the original
//    Prometheus text format, so promhttp drops them without this.
promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
```

```yaml
# 3. docker-compose.yml — without this Prometheus parses them and throws them away.
- --enable-feature=exemplar-storage
```

There is a fourth subtlety: an exemplar is only attached when the trace was
**sampled**. A link to a trace the tail sampler dropped is a link to a 404,
which looks like data loss:

```go
if !ok || !sc.IsValid() || !sc.IsSampled() {
	o.Observe(v) // count it, but do not promise a trace that does not exist
	return
}
```

`verify-stack.sh` closes the loop by taking an exemplar out of Prometheus and
fetching that exact trace from Tempo.

### 4. Tail sampling, and why not head sampling

Head sampling means each service decides at span creation, before anything has
happened. At 10% head sampling you keep 10% of your errors and 10% of your slow
requests — the only traces anyone ever looks for. Worse, you cannot tell
whether an error is rare or merely unsampled.

Tail sampling moves the decision to the collector, which buffers spans by trace
id for `decision_wait` and then judges the finished trace. The policies in
[`deploy/collector/config.yaml`](deploy/collector/config.yaml):

| Policy | Keeps |
|---|---|
| `errors` | every trace containing a span with status `ERROR` |
| `slow` | every trace over 500ms, successful or not |
| `chaos-marked` | every trace the chaos script touched, so a demo is always visible |
| `explicitly-requested` | anything carrying `sampling.priority.high` |
| `baseline` | 20% of ordinary, fast, successful traffic |

So the services sample **everything** at the head
(`OTEL_TRACES_SAMPLER_ARG=1.0`) and let the collector decide.

The costs are real and worth stating:

- The collector must see every span of a trace. More than one replica needs a
  trace-id-aware load balancer in front (the `loadbalancing` exporter) or
  decisions are made on partial traces.
- `decision_wait` is latency before a trace is queryable, and a trace longer
  than that window is judged on what arrived in time.
- Buffering costs memory, bounded by `num_traces`.

One more trap, which is why there are two trace pipelines: **`spanmetrics` is
fed from the unsampled stream**. Deriving RED metrics from sampled spans gives
a request rate that is 20% of reality and an error rate skewed by the very
policy that keeps all the errors — a reliable way to build a dashboard that
reports 100% of requests failing.

### 5. The deliberate error path

`inventory` fails three ways on purpose, and the distinction between them is
the point ([`internal/spanerr`](internal/spanerr/spanerr.go)).

A **client** fault (404, 409) records an exception event for debuggability but
leaves the span status `Unset`. A **server** fault (500) sets `codes.Error`.
Counting a caller's bad input as a server error makes an SLO meaningless.

```go
span.RecordError(err, trace.WithStackTrace(true), ...)   // exception.type/message/stacktrace
if kind == KindServer {
	span.SetStatus(codes.Error, err.Error())             // what Tempo's filter and the sampler see
}
```

`WithStackTrace(true)` is the whole point: an exception event without
`exception.stacktrace` says a thing failed but not where.

The panic is recovered **inside the span that panicked**, not in the HTTP
middleware — a recover in the outer middleware attributes the failure to the
server span with the real frames already unwound:

```go
defer func() {
	if rec := recover(); rec != nil {
		err = spanerr.RecordServer(span, fmt.Errorf("inventory: reserve panicked: %v", rec))
	}
}()
```

### 6. Resource attributes from one place

`service.name`, `service.version`, `deployment.environment.name` and
`service.instance.id` are built once per runtime —
[`internal/obs/resource.go`](internal/obs/resource.go) and
[`src/telemetry.ts`](services/fulfillment/src/telemetry.ts) — and reused by the
trace pipeline, the log pipeline and the Prometheus const labels. The two files
must agree on the strings, which is what the resource tests in each language
pin down.

Two gotchas met along the way:

- `resource.Merge` fails with *conflicting Schema URL* unless the semconv
  version you import matches the one `resource.Default()` was built against.
  A service whose resource build failed starts with **no** resource at all.
- `deployment.environment` was deprecated in semconv 1.27 in favour of
  `deployment.environment.name`. Emitting the old one gives you dashboards that
  match nothing, silently.

Prometheus label names cannot contain dots, so the same facts are spelled
`service_name` / `service_version` / `deployment_environment_name` there.
`obs.PrometheusLabels` is the single mapping, and a test asserts the two sets
describe the same thing.

---

## Testing

Docker is not required for any of it.

```bash
go test ./...                              # 50 tests
cd services/fulfillment && npm test        # 30 tests
```

```bash
./scripts/verify-stack.sh                  # 40+ assertions, needs the stack up
python3 scripts/validate-yaml.py           # strict: rejects duplicate keys
shellcheck -S warning scripts/*.sh
```

### The approach

Tracing is usually tested by starting the whole stack and looking at Grafana,
which is slow, flaky and impossible in CI. Instead, both runtimes have an
in-memory span exporter —
[`internal/obstest`](internal/obstest/obstest.go) wrapping
`tracetest.NewInMemoryExporter`, and
[`src/testing.ts`](services/fulfillment/src/testing.ts) wrapping
`InMemorySpanExporter` — and the tests assert on **the spans a request actually
produced**.

So the assertions are things like:

```go
if got, want := process.Parent.SpanID(), publish.SpanContext.SpanID(); got != want {
	t.Errorf("process parent = %s, want the publish span %s", got, want)
}
if !process.Parent.IsRemote() {
	t.Error("process parent is not marked remote; it was taken from local context, not from the message")
}
```

and, in [`internal/orders/orders_test.go`](internal/orders/orders_test.go), the
whole tree for one checkout: an exact span count, every parent link named, and
the walk from `inventory.reserve` up through the inventory server span to the
orders client span to the orders server span.

A test that merely counts spans, or asserts that something is *absent*, goes
green when the suite is not running. So:

- `obstest.Recorder.Span(t, name)` insists on **exactly one** span with that
  name, and prints every recorded name when it fails.
- The cross-service test asserts the **exact total**, 8 spans for its stack, so
  an extra or missing hop fails rather than passing quietly.
- Error tests require the stack trace to **name the failing package**
  (`internal/inventory`, `worker.ts`), not merely to be non-empty.
- CI re-runs the suites verbosely and fails if fewer than 40 Go tests or 25
  Vitest tests passed, or if any of seven named tests did not appear.

### These tests were mutation-checked

Each claim was verified by breaking the mechanism and confirming the suite goes
red:

| Mutation | Result |
|---|---|
| consumer ignores the message headers (Go) | 2 tests fail, naming the lost parent and the wrong trace id |
| consumer uses `context.active()` (Node) | 7 tests fail |
| outgoing call does not inject (Node) | 1 test fails on the `traceparent` it expected |

### What the stack check adds

`verify-stack.sh` is the part the in-memory tests cannot do: it proves the
spans *leave the processes*, survive the tail sampler, and land in all three
backends as one correlated whole. Every assertion is on content:

- Tempo returns a trace with ≥10 spans, containing all four `service.name`s and
  nine named spans — polling for "at least N spans", because Tempo answers 200
  with a partial trace while the ingester is still receiving.
- The Node consumer span's `parentSpanId` **equals** the Go publish span's id.
- Loki returns log lines from both `orders` and `fulfillment` filtered by
  `| trace_id="<the id we were handed>"`.
- A Prometheus exemplar exists **and the trace it names resolves in Tempo**.
- `traces_span_metrics_calls_total` has a series for `fulfillment`, which has
  no `/metrics` endpoint of its own.
- After a chaos run, TraceQL `{ status = error }` matches at least one trace —
  if the error path left the span status `Unset`, the tail sampler would have
  dropped them and this fails.

---

## Configuration

Standard OTel variables where they exist, read in one place per runtime
([`internal/obs/config.go`](internal/obs/config.go),
[`src/telemetry.ts`](services/fulfillment/src/telemetry.ts)).

| Variable | Default | |
|---|---|---|
| `OTEL_SERVICE_NAME` | per binary | becomes `service.name` |
| `SERVICE_VERSION` | `0.0.0-dev` | becomes `service.version` |
| `DEPLOYMENT_ENVIRONMENT` | `local` | becomes `deployment.environment.name` |
| `SERVICE_INSTANCE_ID` | hostname | becomes `service.instance.id` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | base URL; `/v1/traces` and `/v1/logs` appended |
| `OTEL_TRACES_SAMPLER_ARG` | `1.0` | head sampling ratio — leave it at 1.0 |
| `LOG_LEVEL` | `info` | `debug`\|`info`\|`warn`\|`error` |
| `LOG_FORMAT` | `json` | `json` or `text`; trace ids are present either way |
| `LISTEN_ADDR` | per binary | |
| `ORDERS_URL`, `INVENTORY_URL` | compose DNS names | |
| `REDIS_ADDR` / `REDIS_URL` | `redis:6379` | Go / Node |
| `QUEUE_PREFIX`, `QUEUE_TOPIC` | `otelq`, `orders.fulfil` | |

Chaos dials, read by the three Go services
([`internal/chaos`](internal/chaos/chaos.go)). They accept either `25` or
`0.25` for twenty-five per cent, because the compose file and a human typing
into a script disagree about which is natural:

| Variable | Default | |
|---|---|---|
| `CHAOS_LATENCY_PCT` | `0` | share of requests given extra delay |
| `CHAOS_LATENCY_MIN_MS` / `_MAX_MS` | `50` / `800` | the delay range |
| `CHAOS_FAULT_PCT` | `0` | share of requests that fail with 503 |

Everything the injector does is recorded on the affected span as
`chaos.injected_latency_ms` / `chaos.injected_fault`, so injected pain is
distinguishable from real pain:

```
{ status = error && span.chaos.injected_fault = nil }     # only real failures
```

---

## Limitations

Stated rather than hidden, because a portfolio piece that pretends to be
production-ready is less useful than one that knows where its edges are.

**Verified with the stack running:** everything in
[the stack check](#what-the-stack-check-adds), in CI, on every push. CI brings
the real compose stack up and asserts traces, logs, exemplars and tail-sampled
error traces all arrive.

**Not verified:**

- **Scale.** The collector is a single replica, which tail sampling requires
  unless you put a `loadbalancing` exporter in front. That is the first thing
  that breaks on a real deployment, and nothing here exercises it.
- **Durability.** Tempo, Loki and Prometheus write to local volumes with short
  retention. There is no object store, no compaction tuning, no HA.
- **The queue is a Redis list.** `RPUSH`/`BLPOP` — no consumer groups, no
  redelivery, no dead-letter queue. A crashed worker loses the job it held.
  This is deliberate: the subject is propagation, and a list exercises exactly
  the same propagation path a Kafka or SQS client would, with none of the
  operational surface. [`queue.Backend`](internal/queue/queue.go) is the seam
  to replace.
- **The datastores are in-memory maps.** `orders.insert` and
  `inventory.reserve` are real spans with real `db.*` attributes around a
  `map[string]`. Swapping in Postgres would add a migration story without
  adding anything to the tracing story.
- **No metrics SDK on the Node worker.** It emits traces and logs over OTLP;
  its RED metrics come from the collector's `spanmetrics` connector instead.
  That is a legitimate pattern — and the only metrics path for a service you do
  not control — but it is a different code path from the Go services'
  `/metrics`.
- **`Dockerfile` uses an alpine runtime, not distroless.** Purely so the
  compose healthchecks can use busybox `wget`. Distroless would need a Go
  health probe binary.
- **Grafana runs with anonymous admin.** Fine for a demo on `localhost`,
  obviously not otherwise.

**Known trap, written down because it cost time:** `resource.Merge` returns
*conflicting Schema URL* when the imported `semconv` version does not match the
one `resource.Default()` was built with, and a service whose resource failed to
build starts with no resource at all — so every dashboard grouping silently
collapses. Keep the `semconv/vX.Y.Z` import in step with the OTel SDK version
in `go.mod`.

---

## Repository layout

```
cmd/{gateway,orders,inventory}     thin mains; all three build from one Dockerfile
internal/
  obs/         resource, trace + log pipelines, the trace-correlating slog handler
  obstest/     in-memory span recorder used by every Go test
  queue/       the async boundary: carrier, producer span, consumer span
  spanerr/     exception events, stack traces, client-vs-server faults
  metrics/     RED metrics with trace_id exemplars
  httpx/       server/client wiring, route templating, X-Trace-Id
  chaos/       latency and fault injection, recorded on the span
  service/     the start-up and shutdown sequence the three binaries share
  {gateway,orders,inventory}/  the service logic, unit-testable with httptest
services/fulfillment/            the Node/TypeScript worker
deploy/
  collector/   tail sampling and spanmetrics, with the reasoning
  tempo/ loki/ prometheus/       backend configuration
  grafana/     datasources with all three link directions, plus 2 dashboards
scripts/
  traffic.sh        drive the stack, print trace ids
  chaos.sh          turn the dials and replay traffic
  verify-stack.sh   40+ assertions against the running stack
  validate-yaml.py  strict YAML check that rejects duplicate keys
```

## Licence

[MIT](LICENSE).
