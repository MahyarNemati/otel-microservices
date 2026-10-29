package orders_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/chaos"
	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/inventory"
	"github.com/MahyarNemati/otel-microservices/internal/metrics"
	"github.com/MahyarNemati/otel-microservices/internal/obs"
	"github.com/MahyarNemati/otel-microservices/internal/obstest"
	"github.com/MahyarNemati/otel-microservices/internal/orders"
	"github.com/MahyarNemati/otel-microservices/internal/queue"
)

// stack wires orders in front of a real inventory service, both instrumented by
// the same in-memory recorder, so one request produces the whole span tree.
type stack struct {
	rec       *obstest.Recorder
	orders    *httptest.Server
	queue     *queue.Memory
	publisher *queue.Publisher
	consumer  *queue.Consumer
	logs      *bytes.Buffer
}

func newStack(t *testing.T, injector *chaos.Injector) *stack {
	t.Helper()
	rec := obstest.New(t, obstest.Options{})
	logs := &bytes.Buffer{}

	invSvc := inventory.NewService(inventory.NewStore(rec.Tracer), chaos.New(chaos.Config{}), rec.Tracer)
	invSrv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName:    "inventory",
		Routes:         invSvc.Routes(),
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(invSrv.Close)

	q := queue.NewMemory()
	publisher := queue.NewPublisher(q, rec.Tracer, rec.Propagator)
	client := httpx.NewClient(httpx.ClientOptions{
		Timeout:        5 * time.Second,
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	})

	ordSvc := orders.NewService(
		orders.Config{
			InventoryBaseURL: invSrv.URL,
			NewOrderID:       func() string { return "ord-test" },
		},
		orders.NewStore(rec.Tracer),
		publisher,
		client,
		injector,
		rec.Tracer,
		metrics.New(obs.Config{ServiceName: "orders", ServiceVersion: "test", Environment: "test"}),
		slog.New(obs.NewTraceHandler(slog.NewJSONHandler(logs, nil))),
	)
	ordSrv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName:    "orders",
		Routes:         ordSvc.Routes(),
		Logger:         slog.New(obs.NewTraceHandler(slog.NewJSONHandler(logs, nil))),
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(ordSrv.Close)

	return &stack{
		rec:       rec,
		logs:      logs,
		orders:    ordSrv,
		queue:     q,
		publisher: publisher,
		consumer:  queue.NewConsumer(q, rec.Tracer, rec.Propagator),
	}
}

func (s *stack) checkout(t *testing.T, body string) *http.Response {
	t.Helper()
	resp, err := s.orders.Client().Post(s.orders.URL+"/orders", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// TestCheckoutProducesOneConnectedTrace is the whole-topology assertion. One
// request crosses an HTTP boundary and a queue boundary; every span it produced
// must be in one trace, with the parent links in the right places.
func TestCheckoutProducesOneConnectedTrace(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	resp := st.checkout(t, `{"sku":"SKU-TEA","quantity":3,"customer":"ada"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	var body orders.CheckoutResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.OrderID != "ord-test" {
		t.Errorf("order id = %q, want ord-test", body.OrderID)
	}
	if body.Remaining != 117 {
		t.Errorf("remaining = %d, want 117", body.Remaining)
	}

	// Drain the queue the way the worker does.
	msg, err := st.queue.Dequeue(context.Background(), orders.TopicFulfil, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	var job orders.FulfilJob
	if err := json.Unmarshal(msg.Body, &job); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if job.OrderID != "ord-test" || job.SKU != "SKU-TEA" || job.Quantity != 3 {
		t.Errorf("job = %+v, want order ord-test / SKU-TEA / 3", job)
	}
	if err := st.consumer.Process(context.Background(), msg, func(ctx context.Context, _ queue.Message) error {
		_, s := st.rec.Tracer.Start(ctx, "fulfilment work")
		s.End()
		return nil
	}); err != nil {
		t.Fatalf("Process: %v", err)
	}

	server := st.rec.Span(t, "POST /orders")
	traceID := server.SpanContext.TraceID()

	// The response body must advertise the trace it belongs to.
	if body.TraceID != traceID.String() {
		t.Errorf("response trace_id = %q, want %q", body.TraceID, traceID)
	}
	if got := resp.Header.Get(httpx.TraceIDHeader); got != traceID.String() {
		t.Errorf("%s = %q, want %q", httpx.TraceIDHeader, got, traceID)
	}

	// Every span the request produced, across both services and both sides of
	// the queue. "POST /reserve" appears twice -- once as the orders client
	// span and once as the inventory server span -- which is itself the proof
	// that the HTTP hop produced a parent/child pair rather than one span.
	wantCounts := map[string]int{
		"POST /orders":          1, // orders server span
		"POST /reserve":         2, // orders client span + inventory server span
		"inventory.reserve":     1, // inventory datastore span
		"orders.insert":         1, // orders datastore span
		"orders.fulfil publish": 1, // producer
		"orders.fulfil process": 1, // consumer, in the worker
		"fulfilment work":       1, // the worker's own child
	}
	counts := map[string]int{}
	for _, s := range st.rec.Spans() {
		counts[s.Name]++
		if s.SpanContext.TraceID() != traceID {
			t.Errorf("span %q is in trace %s, want %s", s.Name, s.SpanContext.TraceID(), traceID)
		}
	}
	total := 0
	for name, want := range wantCounts {
		total += want
		if counts[name] != want {
			t.Errorf("span %q appeared %d times, want %d (all spans: %v)", name, counts[name], want, st.rec.Names())
		}
	}
	if len(st.rec.Spans()) != total {
		t.Errorf("the request produced %d spans, want %d: %v", len(st.rec.Spans()), total, st.rec.Names())
	}

	// Parent links. The inventory server span and the orders client span share
	// the name "POST /reserve"; rec.Span insists on exactly one, so assert the
	// shape via the spans that are unambiguous.
	reserveDB := st.rec.Span(t, "inventory.reserve")
	publish := st.rec.Span(t, "orders.fulfil publish")
	process := st.rec.Span(t, "orders.fulfil process")
	work := st.rec.Span(t, "fulfilment work")
	insert := st.rec.Span(t, "orders.insert")

	if got, want := publish.Parent.SpanID(), server.SpanContext.SpanID(); got != want {
		t.Errorf("publish parent = %s, want the orders server span %s", got, want)
	}
	if got, want := insert.Parent.SpanID(), server.SpanContext.SpanID(); got != want {
		t.Errorf("orders.insert parent = %s, want the orders server span %s", got, want)
	}
	if got, want := process.Parent.SpanID(), publish.SpanContext.SpanID(); got != want {
		t.Errorf("queue process parent = %s, want the publish span %s", got, want)
	}
	if got, want := work.Parent.SpanID(), process.SpanContext.SpanID(); got != want {
		t.Errorf("fulfilment work parent = %s, want the process span %s", got, want)
	}

	// inventory.reserve's parent must be the inventory *server* span, which in
	// turn must be remote-parented on the orders client span. Walk it by id.
	byID := map[trace.SpanID]string{}
	parents := map[trace.SpanID]trace.SpanID{}
	for _, s := range st.rec.Spans() {
		byID[s.SpanContext.SpanID()] = s.Name
		parents[s.SpanContext.SpanID()] = s.Parent.SpanID()
	}
	invServerID := reserveDB.Parent.SpanID()
	if byID[invServerID] != "POST /reserve" {
		t.Fatalf("inventory.reserve's parent is %q, want the inventory server span", byID[invServerID])
	}
	ordersClientID := parents[invServerID]
	if byID[ordersClientID] != "POST /reserve" {
		t.Fatalf("inventory server span's parent is %q, want the orders client span", byID[ordersClientID])
	}
	if parents[ordersClientID] != server.SpanContext.SpanID() {
		t.Error("the orders client span is not a child of the orders server span")
	}
}

// TestCheckoutPropagatesUpstreamStatus checks a dependency's 409 surfaces as a
// 409 and as a client fault, rather than as an opaque 500 that would ruin the
// error-rate signal for the whole stack.
func TestCheckoutPropagatesUpstreamStatus(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	resp := st.checkout(t, `{"sku":"`+inventory.PrefixOutOfStock+`MATCHA","quantity":1,"customer":"ada"}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}

	var body httpx.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Code != "out_of_stock" {
		t.Errorf("error code = %q, want out_of_stock", body.Code)
	}
	if body.TraceID == "" {
		t.Error("error body carries no trace_id")
	}

	server := st.rec.Span(t, "POST /orders")
	if server.Status.Code != codes.Unset {
		t.Errorf("orders server span status = %v, want Unset: an upstream 4xx is a client fault", server.Status.Code)
	}
	if got := obstest.RequireAttr(t, server, "error.kind"); got != "client" {
		t.Errorf("error.kind = %q, want client", got)
	}

	// Nothing must have been enqueued for a checkout that failed.
	if n := st.queue.Len(orders.TopicFulfil); n != 0 {
		t.Errorf("queue depth = %d after a failed checkout, want 0", n)
	}
}

// TestCheckoutUpstream5xxIsServerFault is the other side of the boundary.
func TestCheckoutUpstream5xxIsServerFault(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	resp := st.checkout(t, `{"sku":"`+inventory.PrefixPanic+`BROKEN","quantity":1,"customer":"ada"}`)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}

	server := st.rec.Span(t, "POST /orders")
	if server.Status.Code != codes.Error {
		t.Errorf("orders server span status = %v, want Error", server.Status.Code)
	}
	obstest.Event(t, server, "exception")
}

// TestCheckoutRejectsInvalidInput covers the validation branch and asserts it
// does not look like a server failure.
func TestCheckoutRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	for _, body := range []string{
		`{"sku":"","quantity":1}`,
		`{"sku":"SKU-TEA","quantity":0}`,
		`{"sku":"SKU-TEA","quantity":1,"unknown":true}`,
		`not json`,
	} {
		st.rec.Reset()
		resp := st.checkout(t, body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, resp.StatusCode)
			continue
		}
		server := st.rec.Span(t, "POST /orders")
		if server.Status.Code != codes.Unset {
			t.Errorf("body %s: span status = %v, want Unset", body, server.Status.Code)
		}
	}
}

// TestChaosFaultIsRecordedAsInjected checks that injected pain is
// distinguishable from real pain on the span, so a dashboard can exclude it.
func TestChaosFaultIsRecordedAsInjected(t *testing.T) {
	t.Parallel()
	// roll() == 0 always fires, since the comparison is roll() < probability.
	st := newStack(t, chaos.NewWithRoll(chaos.Config{FaultProbability: 1}, func() float64 { return 0 }))

	resp := st.checkout(t, `{"sku":"SKU-TEA","quantity":1}`)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}

	server := st.rec.Span(t, "POST /orders")
	if got := obstest.RequireAttr(t, server, chaos.AttrInjectedFault); got != "true" {
		t.Errorf("%s = %q, want true", chaos.AttrInjectedFault, got)
	}
	if server.Status.Code != codes.Error {
		t.Errorf("span status = %v, want Error", server.Status.Code)
	}
}

// TestDuplicateOrderIsConflict covers the store's own error path.
func TestDuplicateOrderIsConflict(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	if resp := st.checkout(t, `{"sku":"SKU-TEA","quantity":1}`); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first checkout: status = %d, want 202", resp.StatusCode)
	}
	// NewOrderID is fixed in the test stack, so the second checkout collides.
	resp := st.checkout(t, `{"sku":"SKU-TEA","quantity":1}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second checkout: status = %d, want 409", resp.StatusCode)
	}
}

// TestGetOrderReturnsStoredOrder covers the read path end to end.
func TestGetOrderReturnsStoredOrder(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	if resp := st.checkout(t, `{"sku":"SKU-COCOA","quantity":2,"customer":"grace"}`); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("checkout: status = %d", resp.StatusCode)
	}

	resp, err := st.orders.Client().Get(st.orders.URL + "/orders/ord-test")
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got orders.Order
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SKU != "SKU-COCOA" || got.Quantity != 2 || got.Customer != "grace" {
		t.Errorf("order = %+v", got)
	}

	// The route template, not the order id, must be the span name.
	span := st.rec.Span(t, "GET /orders/{id}")
	if got := obstest.RequireAttr(t, span, "http.route"); got != "GET /orders/{id}" {
		t.Errorf("http.route = %q; a raw path here would blow up metric cardinality", got)
	}
}

func TestGetUnknownOrderIsNotFound(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	resp, err := st.orders.Client().Get(st.orders.URL + "/orders/nope")
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestCheckoutLogsAreCorrelatedWithTheTrace is the assertion that was missing
// when the end-to-end check first ran: the correlation machinery was correct
// but the Go services logged nothing inside a request span, so Loki held no
// line to find and "click a span, see its logs" showed an empty panel.
//
// It asserts on the lines the service actually wrote, their trace ids, and the
// business fields a reader needs beside the waterfall.
func TestCheckoutLogsAreCorrelatedWithTheTrace(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	resp := st.checkout(t, `{"sku":"SKU-TEA","quantity":2,"customer":"ada"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	traceID := st.rec.Span(t, "POST /orders").SpanContext.TraceID().String()
	spanID := st.rec.Span(t, "POST /orders").SpanContext.SpanID().String()

	lines := []map[string]any{}
	for _, raw := range strings.Split(strings.TrimSpace(st.logs.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line is not JSON (%q): %v", raw, err)
		}
		lines = append(lines, line)
	}
	if len(lines) < 2 {
		t.Fatalf("the request produced %d log lines, want at least 2 (business + access):\n%s", len(lines), st.logs.String())
	}

	byMsg := map[string]map[string]any{}
	for _, line := range lines {
		msg, _ := line["msg"].(string)
		byMsg[msg] = line
		// Every line written during the request must be correlated, not just
		// the one we happen to assert on below.
		if got := line[obs.LogKeyTraceID]; got != traceID {
			t.Errorf("line %q has trace_id %v, want %s", msg, got, traceID)
		}
		if got := line[obs.LogKeySpanID]; got != spanID {
			t.Errorf("line %q has span_id %v, want the server span %s", msg, got, spanID)
		}
	}

	business, ok := byMsg["order accepted for fulfilment"]
	if !ok {
		t.Fatalf("no business log line; got %v", keysOf(byMsg))
	}
	if business["order.id"] != "ord-test" {
		t.Errorf("business line order.id = %v, want ord-test", business["order.id"])
	}
	if business["messaging.message.id"] == nil || business["messaging.message.id"] == "" {
		t.Error("business line carries no messaging.message.id, so the log cannot be joined to the queue hop")
	}

	access, ok := byMsg["request served"]
	if !ok {
		t.Fatalf("no access log line; got %v", keysOf(byMsg))
	}
	// The route template, not the target: ids in a log field are the same
	// cardinality mistake as ids in a span name.
	if access["http.route"] != "POST /orders" {
		t.Errorf("access line http.route = %v, want the route template", access["http.route"])
	}
	if got := access["http.response.status_code"]; got != float64(202) {
		t.Errorf("access line status = %v, want 202", got)
	}
}

// TestFailedCheckoutLogsAtErrorLevel keeps the access log useful for the
// "recent errors" panel, which filters on level.
func TestFailedCheckoutLogsAtErrorLevel(t *testing.T) {
	t.Parallel()
	st := newStack(t, chaos.New(chaos.Config{}))

	if resp := st.checkout(t, `{"sku":"`+inventory.PrefixPanic+`BROKEN","quantity":1}`); resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}

	var access map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(st.logs.String()), "\n") {
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err == nil && line["msg"] == "request served" {
			access = line
		}
	}
	if access == nil {
		t.Fatalf("no access log line:\n%s", st.logs.String())
	}
	if access["level"] != "ERROR" {
		t.Errorf("level = %v, want ERROR for a 500", access["level"])
	}
	if access[obs.LogKeyTraceID] != st.rec.Span(t, "POST /orders").SpanContext.TraceID().String() {
		t.Error("the error line is not correlated with the failing trace")
	}

	// A 4xx must not be logged as an error, for the same reason its span status
	// stays Unset.
	st.logs.Reset()
	st.rec.Reset()
	if resp := st.checkout(t, `{"sku":"`+inventory.PrefixOutOfStock+`MATCHA","quantity":1}`); resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	for _, raw := range strings.Split(strings.TrimSpace(st.logs.String()), "\n") {
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err == nil && line["msg"] == "request served" {
			if line["level"] != "WARN" {
				t.Errorf("level = %v for a 409, want WARN", line["level"])
			}
		}
	}
}

func keysOf(m map[string]map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
