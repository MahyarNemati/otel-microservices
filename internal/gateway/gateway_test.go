package gateway_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"

	"github.com/MahyarNemati/otel-microservices/internal/chaos"
	"github.com/MahyarNemati/otel-microservices/internal/gateway"
	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/metrics"
	"github.com/MahyarNemati/otel-microservices/internal/obs"
	"github.com/MahyarNemati/otel-microservices/internal/obstest"
	"github.com/MahyarNemati/otel-microservices/internal/orders"
)

// TestGatewayStartsAndPropagatesTrace checks the edge behaviour: a request with
// no incoming traceparent starts a root trace, and that trace reaches the next
// service as a remote parent.
func TestGatewayStartsAndPropagatesTrace(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})

	// A stand-in orders service that records what it received.
	var gotTraceparent string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceparent = r.Header.Get("traceparent")
		httpx.WriteJSON(w, http.StatusAccepted, orders.CheckoutResponse{
			OrderID: "ord-1", SKU: "SKU-TEA", Quantity: 2, Remaining: 118, MessageID: "msg-1",
		})
	}))
	t.Cleanup(upstream.Close)

	svc := gateway.NewService(
		gateway.Config{OrdersBaseURL: upstream.URL},
		httpx.NewClient(httpx.ClientOptions{
			Timeout:        5 * time.Second,
			TracerProvider: rec.Provider(),
			Propagators:    rec.Propagator,
		}),
		chaos.New(chaos.Config{}),
	)
	srv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName:    "gateway",
		Routes:         svc.Routes(),
		Metrics:        metrics.New(obs.Config{ServiceName: "gateway", ServiceVersion: "test", Environment: "test"}),
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Post(srv.URL+"/checkout", "application/json",
		strings.NewReader(`{"sku":"SKU-TEA","quantity":2,"customer":"ada"}`))
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	var body gateway.CheckoutResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	server := rec.Span(t, "POST /checkout")
	if server.Parent.IsValid() {
		t.Errorf("gateway server span has parent %s; a request with no traceparent must start a root trace", server.Parent.SpanID())
	}
	if body.TraceID != server.SpanContext.TraceID().String() {
		t.Errorf("response trace_id = %q, want %q", body.TraceID, server.SpanContext.TraceID())
	}
	if body.OrderID != "ord-1" {
		t.Errorf("order id = %q, want ord-1", body.OrderID)
	}

	// The outgoing call must have carried the gateway's trace.
	if gotTraceparent == "" {
		t.Fatal("upstream received no traceparent header; the client is not instrumented")
	}
	client := rec.Span(t, "POST /orders")
	wantPrefix := "00-" + server.SpanContext.TraceID().String() + "-" + client.SpanContext.SpanID().String() + "-"
	if !strings.HasPrefix(gotTraceparent, wantPrefix) {
		t.Errorf("traceparent = %q, want prefix %q (the client span should be the remote parent)", gotTraceparent, wantPrefix)
	}
	if got, want := client.Parent.SpanID(), server.SpanContext.SpanID(); got != want {
		t.Errorf("client span parent = %s, want the server span %s", got, want)
	}
}

// TestGatewayContinuesIncomingTrace covers a request that already carries a
// traceparent, as it would behind a real load balancer or from a browser with
// RUM instrumentation.
func TestGatewayContinuesIncomingTrace(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusAccepted, orders.CheckoutResponse{OrderID: "ord-2"})
	}))
	t.Cleanup(upstream.Close)

	svc := gateway.NewService(
		gateway.Config{OrdersBaseURL: upstream.URL},
		httpx.NewClient(httpx.ClientOptions{Timeout: 5 * time.Second, TracerProvider: rec.Provider(), Propagators: rec.Propagator}),
		chaos.New(chaos.Config{}),
	)
	srv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName:    "gateway",
		Routes:         svc.Routes(),
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(srv.Close)

	const incomingTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	const incomingSpan = "00f067aa0ba902b7"

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/checkout", strings.NewReader(`{"sku":"SKU-TEA"}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", "00-"+incomingTrace+"-"+incomingSpan+"-01")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	server := rec.Span(t, "POST /checkout")
	if got := server.SpanContext.TraceID().String(); got != incomingTrace {
		t.Errorf("trace id = %s, want the incoming %s", got, incomingTrace)
	}
	if got := server.Parent.SpanID().String(); got != incomingSpan {
		t.Errorf("parent span id = %s, want the incoming %s", got, incomingSpan)
	}
	if got := resp.Header.Get(httpx.TraceIDHeader); got != incomingTrace {
		t.Errorf("%s = %q, want the incoming trace %s", httpx.TraceIDHeader, got, incomingTrace)
	}
}

// TestGatewaySurfacesUpstreamFailure checks the gateway does not mask a
// downstream fault behind a generic 502.
func TestGatewaySurfacesUpstreamFailure(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusConflict, httpx.ErrorBody{Error: "insufficient stock", Code: "out_of_stock"})
	}))
	t.Cleanup(upstream.Close)

	svc := gateway.NewService(
		gateway.Config{OrdersBaseURL: upstream.URL},
		httpx.NewClient(httpx.ClientOptions{Timeout: 5 * time.Second, TracerProvider: rec.Provider(), Propagators: rec.Propagator}),
		chaos.New(chaos.Config{}),
	)
	srv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName:    "gateway",
		Routes:         svc.Routes(),
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Post(srv.URL+"/checkout", "application/json", strings.NewReader(`{"sku":"X"}`))
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want the upstream 409", resp.StatusCode)
	}
	var body httpx.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Code != "out_of_stock" {
		t.Errorf("code = %q, want out_of_stock", body.Code)
	}

	server := rec.Span(t, "POST /checkout")
	if server.Status.Code != codes.Unset {
		t.Errorf("span status = %v, want Unset for an upstream 4xx", server.Status.Code)
	}
}

// TestGatewayMetricsRecordExemplar ties the metric and the trace together at
// the edge, which is where a latency panel is actually read from.
func TestGatewayMetricsRecordExemplar(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	reg := metrics.New(obs.Config{ServiceName: "gateway", ServiceVersion: "test", Environment: "test"})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusAccepted, orders.CheckoutResponse{OrderID: "ord-3"})
	}))
	t.Cleanup(upstream.Close)

	svc := gateway.NewService(
		gateway.Config{OrdersBaseURL: upstream.URL},
		httpx.NewClient(httpx.ClientOptions{Timeout: 5 * time.Second, TracerProvider: rec.Provider(), Propagators: rec.Propagator}),
		chaos.New(chaos.Config{}),
	)
	srv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName:    "gateway",
		Routes:         svc.Routes(),
		Metrics:        reg,
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Post(srv.URL+"/checkout", "application/json", strings.NewReader(`{"sku":"SKU-TEA"}`))
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	defer resp.Body.Close()

	traceID := rec.Span(t, "POST /checkout").SpanContext.TraceID().String()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, req)

	body := w.Body.String()
	if !strings.Contains(body, `http_requests_total{`) {
		t.Fatalf("no request counter in /metrics output:\n%s", body)
	}
	if !strings.Contains(body, `route="POST /checkout"`) {
		t.Errorf("metric is not labelled with the route template:\n%s", body)
	}
	if !strings.Contains(body, "# {trace_id=\""+traceID+"\"}") {
		t.Errorf("latency histogram carries no exemplar for trace %s:\n%s", traceID, body)
	}
}
