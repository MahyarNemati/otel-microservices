// Package obstest builds an OpenTelemetry pipeline that records spans in
// memory, so the tracing behaviour of this repository can be asserted without
// a collector, a backend or Docker.
//
// Everything the README claims about propagation is checked through this
// package: that a child span's parent is the incoming span, that a trace id
// survives the queue hop, that an error sets the span status and records an
// exception with a stack trace, and that the resource attributes the dashboards
// group by are actually present on exported spans.
package obstest

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Recorder is an in-memory trace pipeline.
type Recorder struct {
	Tracer     trace.Tracer
	Propagator propagation.TextMapPropagator

	exporter *tracetest.InMemoryExporter
	provider *sdktrace.TracerProvider
}

// Options configures [New].
type Options struct {
	// Resource, when non-nil, is attached to every span the recorder produces.
	Resource *resource.Resource
	// Sampler overrides the default AlwaysSample.
	Sampler sdktrace.Sampler
}

// New builds a recorder and registers cleanup on t. It deliberately does not
// touch the global providers: tests run in parallel and a global would make
// them interfere.
func New(t *testing.T, opts Options) *Recorder {
	t.Helper()

	exp := tracetest.NewInMemoryExporter()
	sampler := opts.Sampler
	if sampler == nil {
		sampler = sdktrace.AlwaysSample()
	}

	providerOpts := []sdktrace.TracerProviderOption{
		// SyncExporter: an assertion must never race a batch interval.
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sampler),
	}
	if opts.Resource != nil {
		providerOpts = append(providerOpts, sdktrace.WithResource(opts.Resource))
	}

	tp := sdktrace.NewTracerProvider(providerOpts...)
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("obstest: shutdown tracer provider: %v", err)
		}
	})

	return &Recorder{
		Tracer: tp.Tracer("obstest"),
		Propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
		exporter: exp,
		provider: tp,
	}
}

// Provider exposes the SDK provider, for code that takes one directly.
func (r *Recorder) Provider() *sdktrace.TracerProvider { return r.provider }

// Spans returns every span exported so far, in completion order.
func (r *Recorder) Spans() tracetest.SpanStubs { return r.exporter.GetSpans() }

// Reset discards recorded spans.
func (r *Recorder) Reset() { r.exporter.Reset() }

// Span returns the single span with the given name, failing the test when there
// is none or more than one. Insisting on exactly one is what turns "a span
// exists" into a real assertion.
func (r *Recorder) Span(t *testing.T, name string) tracetest.SpanStub {
	t.Helper()
	var found []tracetest.SpanStub
	for _, s := range r.Spans() {
		if s.Name == name {
			found = append(found, s)
		}
	}
	switch len(found) {
	case 1:
		return found[0]
	case 0:
		t.Fatalf("no span named %q; recorded spans: %v", name, r.Names())
	default:
		t.Fatalf("expected exactly one span named %q, got %d", name, len(found))
	}
	return tracetest.SpanStub{}
}

// Names lists the recorded span names, for failure messages.
func (r *Recorder) Names() []string {
	spans := r.Spans()
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, s.Name)
	}
	return names
}

// Attr returns the value of key on span, and whether it was set.
func Attr(span tracetest.SpanStub, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// RequireAttr returns the string value of key on span, failing the test when it
// is absent.
func RequireAttr(t *testing.T, span tracetest.SpanStub, key string) string {
	t.Helper()
	v, ok := Attr(span, key)
	if !ok {
		t.Fatalf("span %q has no attribute %q; attributes: %v", span.Name, key, span.Attributes)
	}
	return v.Emit()
}

// ResourceAttr returns the value of a resource attribute on span.
func ResourceAttr(t *testing.T, span tracetest.SpanStub, key string) string {
	t.Helper()
	if span.Resource == nil {
		t.Fatalf("span %q carries no resource", span.Name)
	}
	for _, kv := range span.Resource.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	t.Fatalf("span %q resource has no attribute %q; resource: %v", span.Name, key, span.Resource.Attributes())
	return ""
}

// Event returns the named event on span.
func Event(t *testing.T, span tracetest.SpanStub, name string) sdktrace.Event {
	t.Helper()
	for _, e := range span.Events {
		if e.Name == name {
			return e
		}
	}
	names := make([]string, 0, len(span.Events))
	for _, e := range span.Events {
		names = append(names, e.Name)
	}
	t.Fatalf("span %q has no event %q; events: %v", span.Name, name, names)
	return sdktrace.Event{}
}

// EventAttr returns the string value of key on event.
func EventAttr(t *testing.T, event sdktrace.Event, key string) string {
	t.Helper()
	for _, kv := range event.Attributes {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	t.Fatalf("event %q has no attribute %q; attributes: %v", event.Name, key, event.Attributes)
	return ""
}
