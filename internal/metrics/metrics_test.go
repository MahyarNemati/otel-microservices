package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/MahyarNemati/otel-microservices/internal/metrics"
	"github.com/MahyarNemati/otel-microservices/internal/obs"
	"github.com/MahyarNemati/otel-microservices/internal/obstest"
)

func newRegistry() *metrics.Registry {
	return metrics.New(obs.Config{ServiceName: "orders", ServiceVersion: "1.0.0", Environment: "test"})
}

// TestExemplarCarriesTraceID is the assertion behind the metric-to-trace path:
// after observing a request inside a sampled span, the histogram bucket holds
// an exemplar naming that trace.
func TestExemplarCarriesTraceID(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	reg := newRegistry()

	_, span := rec.Tracer.Start(context.Background(), "POST /checkout")
	sc := span.SpanContext()
	reg.ObserveRequest(sc, "/checkout", http.MethodPost, 200, 1337*time.Millisecond)
	span.End()

	hist := findHistogram(t, reg, "http_request_duration_seconds")

	var found *dto.Exemplar
	for _, bucket := range hist.GetBucket() {
		if ex := bucket.GetExemplar(); ex != nil {
			found = ex
			break
		}
	}
	if found == nil {
		t.Fatal("no bucket carries an exemplar; ObserveWithExemplar was not used")
	}

	var gotTraceID string
	for _, pair := range found.GetLabel() {
		if pair.GetName() == metrics.ExemplarTraceIDLabel {
			gotTraceID = pair.GetValue()
		}
	}
	if gotTraceID != sc.TraceID().String() {
		t.Errorf("exemplar %s = %q, want %q", metrics.ExemplarTraceIDLabel, gotTraceID, sc.TraceID())
	}
	if got := found.GetValue(); got != 1.337 {
		t.Errorf("exemplar value = %v, want 1.337", got)
	}
}

// TestExemplarOmittedForUnsampledTrace guards against linking to a trace the
// backend dropped. An exemplar pointing at a 404 looks like data loss.
func TestExemplarOmittedForUnsampledTrace(t *testing.T) {
	t.Parallel()
	reg := newRegistry()

	// A valid span context that was not sampled.
	sc := obstest.UnsampledSpanContext()
	reg.ObserveRequest(sc, "/checkout", http.MethodPost, 200, 100*time.Millisecond)

	hist := findHistogram(t, reg, "http_request_duration_seconds")
	for _, bucket := range hist.GetBucket() {
		if ex := bucket.GetExemplar(); ex != nil {
			t.Fatalf("bucket carries exemplar %v for an unsampled trace", ex.GetLabel())
		}
	}
	// The observation itself must still be counted.
	if got := hist.GetSampleCount(); got != 1 {
		t.Errorf("sample count = %d, want 1: the observation was dropped along with the exemplar", got)
	}
}

// TestHandlerNegotiatesOpenMetrics covers the third thing that has to line up:
// the text format cannot represent an exemplar, so without OpenMetrics the
// endpoint exposes a correct-looking histogram with the trace ids stripped.
func TestHandlerNegotiatesOpenMetrics(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	reg := newRegistry()

	_, span := rec.Tracer.Start(context.Background(), "POST /checkout")
	traceID := span.SpanContext().TraceID().String()
	reg.ObserveRequest(span.SpanContext(), "/checkout", http.MethodPost, 500, 2*time.Second)
	span.End()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text; version=1.0.0; charset=utf-8")
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "openmetrics-text") {
		t.Fatalf("Content-Type = %q, want OpenMetrics", ct)
	}
	// The exposition line looks like:
	//   http_request_duration_seconds_bucket{...,le="5.0"} 1 # {trace_id="..."} 2.0 169...
	if !strings.Contains(body, "# {"+metrics.ExemplarTraceIDLabel+`="`+traceID+`"}`) {
		t.Errorf("exposition does not carry the exemplar for trace %s.\n%s", traceID, body)
	}
}

// TestConstLabelsMatchResource keeps metric labels joinable with trace resource
// attributes.
func TestConstLabelsMatchResource(t *testing.T) {
	t.Parallel()
	reg := newRegistry()
	reg.ObserveRequest(obstest.UnsampledSpanContext(), "/x", http.MethodGet, 200, time.Millisecond)

	families, err := reg.Registerer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "http_requests_total" {
			continue
		}
		labels := map[string]string{}
		for _, pair := range f.GetMetric()[0].GetLabel() {
			labels[pair.GetName()] = pair.GetValue()
		}
		for k, want := range map[string]string{
			"service_name":                "orders",
			"service_version":             "1.0.0",
			"deployment_environment_name": "test",
			"route":                       "/x",
			"method":                      http.MethodGet,
			"status":                      "200",
		} {
			if labels[k] != want {
				t.Errorf("label %s = %q, want %q (all labels: %v)", k, labels[k], want, labels)
			}
		}
		return
	}
	t.Fatal("http_requests_total was not exported")
}

func TestQueueMetrics(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	reg := newRegistry()

	_, span := rec.Tracer.Start(context.Background(), "process")
	reg.ObserveQueueWait(span.SpanContext(), "orders.fulfil", 320*time.Millisecond)
	span.End()
	reg.SetQueueDepth("orders.fulfil", 17)

	wait := findHistogram(t, reg, "queue_wait_seconds")
	if got := wait.GetSampleSum(); got < 0.319 || got > 0.321 {
		t.Errorf("queue_wait_seconds sum = %v, want ~0.32", got)
	}
	var hasExemplar bool
	for _, b := range wait.GetBucket() {
		if b.GetExemplar() != nil {
			hasExemplar = true
		}
	}
	if !hasExemplar {
		t.Error("queue_wait_seconds carries no exemplar")
	}

	families, err := reg.Registerer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "queue_depth" {
			if got := f.GetMetric()[0].GetGauge().GetValue(); got != 17 {
				t.Errorf("queue_depth = %v, want 17", got)
			}
			return
		}
	}
	t.Fatal("queue_depth was not exported")
}

func findHistogram(t *testing.T, reg *metrics.Registry, name string) *dto.Histogram {
	t.Helper()
	families, err := reg.Registerer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		if len(f.GetMetric()) == 0 {
			t.Fatalf("%s has no metrics", name)
		}
		h := f.GetMetric()[0].GetHistogram()
		if h == nil {
			t.Fatalf("%s is not a histogram", name)
		}
		return h
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	t.Fatalf("%s was not exported; families: %v", name, names)
	return nil
}
