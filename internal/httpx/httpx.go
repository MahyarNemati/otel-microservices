// Package httpx wires the HTTP edges of every service: a server whose spans are
// correctly named and correlated, and a client that propagates context onward.
//
// It is thin on purpose. otelhttp already does the hard part — reading the
// incoming traceparent, creating the server span, injecting on the way out. The
// pieces added here are the ones otelhttp cannot know about: the route template
// to use as the span name, the response header that lets a caller (or the
// chaos script, or the end-to-end test) find the trace it just created, and the
// metric observation with its exemplar.
package httpx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/metrics"
	"github.com/MahyarNemati/otel-microservices/internal/spanerr"
)

// TraceIDHeader is set on every response. A caller that wants to look at the
// trace for a request it just made would otherwise have to search by time and
// guess, and the end-to-end test would have no deterministic trace to assert on.
const TraceIDHeader = "X-Trace-Id"

// SamplingPriorityHeader asks the collector's tail sampler to keep this
// request's trace whatever else it decides.
//
// It exists because tail sampling makes a fast, successful trace a 20% lottery
// (see deploy/collector/config.yaml): "run this one request and watch it" is
// otherwise impossible, and scripts/verify-stack.sh could not assert on a
// specific trace at all. Send `X-Sampling-Priority: high`.
const SamplingPriorityHeader = "X-Sampling-Priority"

// AttrSamplingPriorityHigh is the span attribute the collector's
// `explicitly-requested` tail-sampling policy matches on.
const AttrSamplingPriorityHigh = "sampling.priority.high"

// MaxBodyBytes bounds a request body. Without it a malformed client can make a
// service allocate without limit, and the resulting OOM looks like a tracing
// problem because the span never ends.
const MaxBodyBytes = 1 << 20

// Route pairs a path pattern with its handler and the low-cardinality name used
// for the span and the metric label.
//
// Using the raw path would put the order id into the span name and the metric
// label, which blows up Prometheus cardinality and makes Tempo's span-name
// filter useless. This is the single most common mistake after losing the
// parent span.
type Route struct {
	// Pattern is a net/http ServeMux pattern, e.g. "POST /orders/{id}".
	Pattern string
	// Name is the low-cardinality template, e.g. "POST /orders/{id}".
	Name string
	// Handler serves the route.
	Handler http.HandlerFunc
}

// ServerOptions configures [NewServerHandler].
type ServerOptions struct {
	// ServiceName becomes the otelhttp instrumentation scope.
	ServiceName string
	// Metrics, when non-nil, receives a request count and a latency
	// observation with a trace exemplar for every route.
	Metrics *metrics.Registry
	// Routes are the instrumented application routes.
	Routes []Route
	// Uninstrumented routes (/healthz, /metrics) are mounted outside the
	// tracing middleware, so a scrape every 5 seconds does not bury the
	// application traces.
	Plain map[string]http.Handler

	// TracerProvider and Propagators override the globals. Production passes
	// neither and gets what obs.Setup installed; tests pass an in-memory
	// provider, which is what makes the span assertions possible without
	// mutating global state in a parallel test.
	TracerProvider trace.TracerProvider
	Propagators    propagation.TextMapPropagator
}

// NewServerHandler builds the service's root handler.
func NewServerHandler(opts ServerOptions) http.Handler {
	app := http.NewServeMux()
	for _, route := range opts.Routes {
		r := route
		handler := http.Handler(r.Handler)
		if opts.Metrics != nil {
			handler = observe(opts.Metrics, r.Name, handler)
		}
		handler = traceIDHeader(handler)
		app.Handle(r.Pattern, routeTag(r.Name, handler))
	}

	otelOpts := []otelhttp.Option{otelhttp.WithServerName(opts.ServiceName)}
	if opts.TracerProvider != nil {
		otelOpts = append(otelOpts, otelhttp.WithTracerProvider(opts.TracerProvider))
	}
	if opts.Propagators != nil {
		otelOpts = append(otelOpts, otelhttp.WithPropagators(opts.Propagators))
	}

	root := http.NewServeMux()
	root.Handle("/", otelhttp.NewHandler(app, "http.server", otelOpts...))
	for pattern, handler := range opts.Plain {
		root.Handle(pattern, handler)
	}
	return root
}

// routeTag renames the server span after the route template and sets
// http.route.
//
// otelhttp cannot do this itself: it creates the span before the mux has
// matched a route, so at that point the only name available is the raw target.
// Naming the span after the target puts order ids into span names, which makes
// Tempo's span-name filter and any aggregation over it useless. Renaming here,
// once the route is known, is the fix — and it has to happen before the handler
// runs, because a span's name is read when it ends but its attributes are read
// by the sampler as they are set.
func routeTag(name string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := trace.SpanFromContext(r.Context())
		span.SetName(name)
		span.SetAttributes(semconv.HTTPRoute(name))
		next.ServeHTTP(w, r)
	})
}

// traceIDHeader publishes the server span's trace id on the response, and
// honours an explicit sampling request. It runs inside otelhttp's handler so
// that there is a span to read, and writes the header before the application
// handler can call WriteHeader.
//
// The sampling attribute goes on the server span of whichever service received
// the header -- normally the gateway. A tail-sampling policy matches a trace if
// *any* of its spans match, so one span is enough to keep the whole trace.
func traceIDHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := trace.SpanFromContext(r.Context())
		if sc := span.SpanContext(); sc.IsValid() {
			w.Header().Set(TraceIDHeader, sc.TraceID().String())
		}
		if strings.EqualFold(r.Header.Get(SamplingPriorityHeader), "high") {
			span.SetAttributes(attribute.Bool(AttrSamplingPriorityHigh, true))
		}
		next.ServeHTTP(w, r)
	})
}

// statusRecorder captures the status code, which http.ResponseWriter does not
// expose. Defaulting to 200 matters: a handler that writes a body without
// calling WriteHeader has still served a 200, and recording 0 would create a
// phantom status label.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Status reports the status actually written.
func (s *statusRecorder) Status() int {
	if s.status == 0 {
		return http.StatusOK
	}
	return s.status
}

func observe(reg *metrics.Registry, route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		reg.ObserveRequest(
			trace.SpanContextFromContext(r.Context()),
			route, r.Method, rec.Status(), time.Since(start),
		)
	})
}

// ClientOptions configures [NewClient].
type ClientOptions struct {
	// Timeout bounds a whole request.
	Timeout time.Duration
	// TracerProvider and Propagators override the globals, as on the server.
	TracerProvider trace.TracerProvider
	Propagators    propagation.TextMapPropagator
}

// NewClient returns an HTTP client that injects trace context into every
// outgoing request and creates a client span for it.
//
// otelhttp.NewTransport is the whole mechanism: it reads the active span from
// the request's context and injects traceparent. A client built with
// &http.Client{} instead will happily make the call and start a new trace at
// the far end — the single most common cause of a trace that stops at the first
// hop.
func NewClient(opts ClientOptions) *http.Client {
	otelOpts := []otelhttp.Option{
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	}
	if opts.TracerProvider != nil {
		otelOpts = append(otelOpts, otelhttp.WithTracerProvider(opts.TracerProvider))
	}
	if opts.Propagators != nil {
		otelOpts = append(otelOpts, otelhttp.WithPropagators(opts.Propagators))
	}
	return &http.Client{
		Timeout:   opts.Timeout,
		Transport: otelhttp.NewTransport(http.DefaultTransport, otelOpts...),
	}
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

// ErrorBody is the error shape every service returns.
type ErrorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	TraceID string `json:"trace_id,omitempty"`
}

// WriteError records err on the active span and writes the matching response.
// The trace id goes in the body as well as the header so that an error surfaced
// to a user carries its own correlation handle.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	span := trace.SpanFromContext(r.Context())
	status := spanerr.RecordHTTP(span, err)

	body := ErrorBody{Error: err.Error()}
	var he *spanerr.HTTPError
	if errors.As(err, &he) {
		body.Code = he.Code
	}
	if sc := span.SpanContext(); sc.IsValid() {
		body.TraceID = sc.TraceID().String()
	}
	WriteJSON(w, status, body)
}

// DecodeJSON reads a bounded JSON request body into v, returning a client-fault
// error on malformed input.
func DecodeJSON(r *http.Request, v any) error {
	defer func() { _, _ = io.Copy(io.Discard, io.LimitReader(r.Body, MaxBodyBytes)) }()
	dec := json.NewDecoder(io.LimitReader(r.Body, MaxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return spanerr.NewHTTPError(http.StatusBadRequest, "invalid_json", err)
	}
	return nil
}

// PostJSON sends body to url and decodes the response into out.
//
// A non-2xx response is returned as an [spanerr.HTTPError] carrying the
// upstream status, so a 404 from a dependency becomes a 404-shaped failure
// rather than an opaque 500, and the caller's span is marked client-fault.
func PostJSON(r *http.Request, client *http.Client, url string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("httpx: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("httpx: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("httpx: call %s: %w", url, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		var upstream ErrorBody
		_ = json.NewDecoder(io.LimitReader(resp.Body, MaxBodyBytes)).Decode(&upstream)
		msg := upstream.Error
		if msg == "" {
			msg = resp.Status
		}
		code := upstream.Code
		if code == "" {
			code = "upstream_error"
		}
		return spanerr.NewHTTPError(resp.StatusCode, code, fmt.Errorf("%s: %s", url, msg))
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, MaxBodyBytes)).Decode(out); err != nil {
		return fmt.Errorf("httpx: decode response from %s: %w", url, err)
	}
	return nil
}

// SpanAttr adds an attribute to the active span.
func SpanAttr(r *http.Request, kv ...attribute.KeyValue) {
	trace.SpanFromContext(r.Context()).SetAttributes(kv...)
}
