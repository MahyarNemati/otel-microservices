package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/MahyarNemati/otel-microservices/internal/obs"
	"github.com/MahyarNemati/otel-microservices/internal/obstest"
)

// TestTraceHandlerCorrelatesLogLine is the assertion behind the claim that a
// Grafana trace links to its logs: every line logged inside a span carries the
// trace id and span id of that span, in the keys the datasource config parses.
func TestTraceHandlerCorrelatesLogLine(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})

	var buf bytes.Buffer
	logger := slog.New(obs.NewTraceHandler(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	ctx, span := rec.Tracer.Start(context.Background(), "checkout")
	logger.InfoContext(ctx, "reserving stock", slog.String("sku", "SKU-1"))
	span.End()

	line := map[string]any{}
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not valid JSON (%q): %v", buf.String(), err)
	}

	sc := span.SpanContext()
	if got := line[obs.LogKeyTraceID]; got != sc.TraceID().String() {
		t.Errorf("%s = %v, want %s", obs.LogKeyTraceID, got, sc.TraceID())
	}
	if got := line[obs.LogKeySpanID]; got != sc.SpanID().String() {
		t.Errorf("%s = %v, want %s", obs.LogKeySpanID, got, sc.SpanID())
	}
	if got := line[obs.LogKeyFlags]; got != "01" {
		t.Errorf("%s = %v, want 01 (sampled)", obs.LogKeyFlags, got)
	}
	// The payload must survive the decoration.
	if got := line["sku"]; got != "SKU-1" {
		t.Errorf("sku = %v, want SKU-1", got)
	}
	if got := line["msg"]; got != "reserving stock" {
		t.Errorf("msg = %v, want \"reserving stock\"", got)
	}
}

// TestTraceHandlerOmitsIDsOutsideSpan guards against the lazy implementation
// that always writes the ids: an all-zero trace id on a startup log makes every
// uncorrelated line look like one trace, and Loki's label cardinality tooling
// then reports a single enormous trace.
func TestTraceHandlerOmitsIDsOutsideSpan(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(obs.NewTraceHandler(slog.NewJSONHandler(&buf, nil)))
	logger.InfoContext(context.Background(), "listening")

	line := map[string]any{}
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not valid JSON: %v", err)
	}
	for _, key := range []string{obs.LogKeyTraceID, obs.LogKeySpanID, obs.LogKeyFlags} {
		if v, ok := line[key]; ok {
			t.Errorf("%s = %v on a line logged outside any span, want absent", key, v)
		}
	}
}

// TestTraceHandlerPreservesGroupsAndAttrs checks the decorator composes, since
// a handler that drops WithGroup state silently reshapes every log line.
func TestTraceHandlerPreservesGroupsAndAttrs(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})

	var buf bytes.Buffer
	logger := slog.New(obs.NewTraceHandler(slog.NewJSONHandler(&buf, nil))).
		With(slog.String("service", "orders")).
		WithGroup("order")

	ctx, span := rec.Tracer.Start(context.Background(), "s")
	logger.InfoContext(ctx, "created", slog.String("id", "o-1"))
	span.End()

	line := map[string]any{}
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not valid JSON: %v", err)
	}
	if got := line["service"]; got != "orders" {
		t.Errorf("service = %v, want orders", got)
	}
	group, ok := line["order"].(map[string]any)
	if !ok {
		t.Fatalf("expected an \"order\" group object, got %T in %s", line["order"], buf.String())
	}
	if got := group["id"]; got != "o-1" {
		t.Errorf("order.id = %v, want o-1", got)
	}
	// Trace ids are added inside the active group, which is where a Loki
	// `| json` pipeline expecting order_trace_id would look. What matters is
	// that they are present and non-zero somewhere on the line.
	if !strings.Contains(buf.String(), span.SpanContext().TraceID().String()) {
		t.Errorf("log line %s does not contain trace id %s", buf.String(), span.SpanContext().TraceID())
	}
}

// TestFanoutReachesEveryHandler covers the dual-sink wiring: a service logs
// once and the line must reach both stdout and the OTLP bridge. A fanout that
// stops at the first handler would leave Loki empty while the container logs
// looked fine.
func TestFanoutReachesEveryHandler(t *testing.T) {
	t.Parallel()

	var a, b bytes.Buffer
	logger := slog.New(obs.NewFanoutHandler(
		slog.NewJSONHandler(&a, nil),
		slog.NewTextHandler(&b, nil),
	))
	logger.Info("hello", slog.Int("n", 1))

	if !strings.Contains(a.String(), `"msg":"hello"`) {
		t.Errorf("first handler got %q", a.String())
	}
	if !strings.Contains(b.String(), "msg=hello") {
		t.Errorf("second handler got %q", b.String())
	}
	if !strings.Contains(a.String(), `"n":1`) || !strings.Contains(b.String(), "n=1") {
		t.Error("attributes did not reach both handlers")
	}
}

// TestFanoutRespectsPerHandlerLevel checks that a quiet sink does not silence a
// loud one.
func TestFanoutRespectsPerHandlerLevel(t *testing.T) {
	t.Parallel()

	var quiet, loud bytes.Buffer
	logger := slog.New(obs.NewFanoutHandler(
		slog.NewJSONHandler(&quiet, &slog.HandlerOptions{Level: slog.LevelError}),
		slog.NewJSONHandler(&loud, &slog.HandlerOptions{Level: slog.LevelDebug}),
	))
	logger.Info("info line")

	if quiet.Len() != 0 {
		t.Errorf("error-level handler received an info line: %q", quiet.String())
	}
	if !strings.Contains(loud.String(), "info line") {
		t.Errorf("debug-level handler missed the info line: %q", loud.String())
	}
}

func TestParseLevel(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]slog.Level{
		"debug":    slog.LevelDebug,
		"INFO":     slog.LevelInfo,
		" warn ":   slog.LevelWarn,
		"warning":  slog.LevelWarn,
		"error":    slog.LevelError,
		"":         slog.LevelInfo,
		"nonsense": slog.LevelInfo,
	} {
		if got := obs.ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}
