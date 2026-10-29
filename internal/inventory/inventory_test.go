package inventory_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/chaos"
	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/inventory"
	"github.com/MahyarNemati/otel-microservices/internal/obstest"
)

// TestReservePanicIsRecordedAsException is the deliberate error path. The claim
// being checked is specific: not "the request returned 500" but "the span that
// failed carries an exception event with a stack trace and an Error status", so
// that Tempo's error filter and the collector's tail sampler both see it.
func TestReservePanicIsRecordedAsException(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	store := inventory.NewStore(rec.Tracer)

	ctx, root := rec.Tracer.Start(context.Background(), "caller")
	_, err := store.Reserve(ctx, inventory.PrefixPanic+"BROKEN", 1)
	root.End()

	if err == nil {
		t.Fatal("Reserve returned nil for the panicking SKU")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Errorf("error = %q, want it to mention the panic", err)
	}

	span := rec.Span(t, "inventory.reserve")
	if span.Status.Code != codes.Error {
		t.Errorf("span status = %v, want Error", span.Status.Code)
	}
	ev := obstest.Event(t, span, "exception")
	if got := obstest.EventAttr(t, ev, "exception.type"); got == "" {
		t.Error("exception.type is empty")
	}
	stack := obstest.EventAttr(t, ev, "exception.stacktrace")
	if stack == "" {
		t.Fatal("exception.stacktrace is empty; the trace shows a failure with no location")
	}
	// The stack must name this package, not just the runtime's recover frames.
	if !strings.Contains(stack, "internal/inventory") {
		t.Errorf("stack trace does not point at the failing package:\n%s", stack)
	}
	if got := obstest.RequireAttr(t, span, "error.kind"); got != "server" {
		t.Errorf("error.kind = %q, want server", got)
	}
}

// TestReserveOutOfStockIsClientFault pins the SLO boundary: a caller asking for
// stock that is not there must not raise the service's error rate.
func TestReserveOutOfStockIsClientFault(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	store := inventory.NewStore(rec.Tracer)

	ctx, root := rec.Tracer.Start(context.Background(), "caller")
	remaining, err := store.Reserve(ctx, inventory.PrefixOutOfStock+"MATCHA", 1)
	root.End()

	if err == nil {
		t.Fatal("Reserve succeeded for an out-of-stock SKU")
	}
	if remaining != 0 {
		t.Errorf("remaining = %d, want 0", remaining)
	}

	span := rec.Span(t, "inventory.reserve")
	if span.Status.Code != codes.Unset {
		t.Errorf("span status = %v, want Unset for a client fault", span.Status.Code)
	}
	obstest.Event(t, span, "exception") // still debuggable
	if got := obstest.RequireAttr(t, span, "error.kind"); got != "client" {
		t.Errorf("error.kind = %q, want client", got)
	}
	if got := obstest.RequireAttr(t, span, "inventory.available"); got != "0" {
		t.Errorf("inventory.available = %q, want 0", got)
	}
}

func TestReserveUnknownSKU(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	store := inventory.NewStore(rec.Tracer)

	ctx, root := rec.Tracer.Start(context.Background(), "caller")
	_, err := store.Reserve(ctx, "SKU-NOPE", 1)
	root.End()

	if err == nil {
		t.Fatal("Reserve succeeded for an unknown SKU")
	}
	span := rec.Span(t, "inventory.reserve")
	if span.Status.Code != codes.Unset {
		t.Errorf("span status = %v, want Unset", span.Status.Code)
	}
}

// TestReserveDecrementsStock is the happy path, asserted on both the returned
// value and the span attribute a dashboard would read.
func TestReserveDecrementsStock(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	store := inventory.NewStore(rec.Tracer)

	ctx, root := rec.Tracer.Start(context.Background(), "caller")
	remaining, err := store.Reserve(ctx, "SKU-COFFEE", 10)
	root.End()
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if remaining != 30 {
		t.Errorf("remaining = %d, want 30", remaining)
	}
	if level, _ := store.Level("SKU-COFFEE"); level != 30 {
		t.Errorf("stored level = %d, want 30", level)
	}

	span := rec.Span(t, "inventory.reserve")
	if span.SpanKind != trace.SpanKindClient {
		t.Errorf("span kind = %v, want Client for a datastore access", span.SpanKind)
	}
	if got := obstest.RequireAttr(t, span, "inventory.remaining"); got != "30" {
		t.Errorf("inventory.remaining = %q, want 30", got)
	}
	if got := obstest.RequireAttr(t, span, "db.operation.name"); got != "reserve" {
		t.Errorf("db.operation.name = %q, want reserve", got)
	}
	if got, want := span.Parent.SpanID(), root.SpanContext().SpanID(); got != want {
		t.Errorf("reserve parent = %s, want the caller span %s", got, want)
	}
}

// TestHandlerContinuesIncomingTrace is the HTTP-hop assertion: a request
// carrying a traceparent must produce a server span parented on it, not a new
// root. This is the check that would catch a missing propagator.
func TestHandlerContinuesIncomingTrace(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	svc := inventory.NewService(inventory.NewStore(rec.Tracer), chaos.New(chaos.Config{}), rec.Tracer)

	srv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName:    "inventory",
		Routes:         svc.Routes(),
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(srv.Close)

	// Build the caller side exactly as another service would: a client span,
	// then an injected traceparent.
	callerCtx, caller := rec.Tracer.Start(context.Background(), "orders calls inventory")
	req, err := http.NewRequestWithContext(callerCtx, http.MethodPost, srv.URL+"/reserve",
		strings.NewReader(`{"sku":"SKU-TEA","quantity":2,"order_id":"ord-1"}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	rec.Propagator.Inject(callerCtx, propagationHeader(req.Header))

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	caller.End()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// The response header must name the trace the caller started, which is what
	// makes a request traceable from the outside.
	if got, want := resp.Header.Get(httpx.TraceIDHeader), caller.SpanContext().TraceID().String(); got != want {
		t.Errorf("%s = %q, want %q", httpx.TraceIDHeader, got, want)
	}

	server := rec.Span(t, "POST /reserve")
	if got, want := server.SpanContext.TraceID(), caller.SpanContext().TraceID(); got != want {
		t.Errorf("server span trace id = %s, want %s: the incoming traceparent was ignored", got, want)
	}
	if got, want := server.Parent.SpanID(), caller.SpanContext().SpanID(); got != want {
		t.Errorf("server span parent = %s, want the caller %s", got, want)
	}
	if !server.Parent.IsRemote() {
		t.Error("server span parent is not remote; it came from local context rather than the header")
	}
	if server.SpanKind != trace.SpanKindServer {
		t.Errorf("span kind = %v, want Server", server.SpanKind)
	}
	if got := obstest.RequireAttr(t, server, "http.route"); got != "POST /reserve" {
		t.Errorf("http.route = %q, want the route template", got)
	}

	// The datastore span must hang off the server span, giving a three-level
	// tree: caller -> server -> reserve.
	reserve := rec.Span(t, "inventory.reserve")
	if got, want := reserve.Parent.SpanID(), server.SpanContext.SpanID(); got != want {
		t.Errorf("reserve parent = %s, want the server span %s", got, want)
	}
}

// TestHandlerErrorResponseCarriesTraceID checks the correlation handle survives
// onto an error response, where a human needs it most.
func TestHandlerErrorResponseCarriesTraceID(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	svc := inventory.NewService(inventory.NewStore(rec.Tracer), chaos.New(chaos.Config{}), rec.Tracer)

	srv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName:    "inventory",
		Routes:         svc.Routes(),
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Post(srv.URL+"/reserve", "application/json",
		strings.NewReader(`{"sku":"`+inventory.PrefixPanic+`BROKEN","quantity":1}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	headerTrace := resp.Header.Get(httpx.TraceIDHeader)
	if headerTrace == "" {
		t.Fatalf("%s is missing from the error response", httpx.TraceIDHeader)
	}

	server := rec.Span(t, "POST /reserve")
	if got := server.SpanContext.TraceID().String(); got != headerTrace {
		t.Errorf("header trace id %q does not match the server span's %q", headerTrace, got)
	}
	if server.Status.Code != codes.Error {
		t.Errorf("server span status = %v, want Error", server.Status.Code)
	}
}

// TestWorkerFailureSKUIsReservable guards the far-side-of-the-queue error path.
// Inventory must stock FAIL-, or checkout 404s here and the job never reaches
// the Node worker -- so the interesting failure, an error span parented on a
// publish span in another process, would never be produced at all.
func TestWorkerFailureSKUIsReservable(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	store := inventory.NewStore(rec.Tracer)

	if _, ok := store.Level(inventory.PrefixWorkerFailure + "CRATE"); !ok {
		t.Fatalf("%sCRATE is not in the catalogue", inventory.PrefixWorkerFailure)
	}

	ctx, root := rec.Tracer.Start(context.Background(), "caller")
	remaining, err := store.Reserve(ctx, inventory.PrefixWorkerFailure+"CRATE", 1)
	root.End()
	if err != nil {
		t.Fatalf("Reserve: %v; the worker-failure path is unreachable", err)
	}
	if remaining < 1 {
		t.Errorf("remaining = %d; stock it generously so a long CI run does not exhaust it", remaining)
	}

	span := rec.Span(t, "inventory.reserve")
	if span.Status.Code != codes.Unset {
		t.Errorf("span status = %v, want Unset: inventory succeeds, the worker is what fails", span.Status.Code)
	}
}
