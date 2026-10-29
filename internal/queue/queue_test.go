package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/obstest"
	"github.com/MahyarNemati/otel-microservices/internal/queue"
	"github.com/MahyarNemati/otel-microservices/internal/spanerr"
)

type job struct {
	OrderID string `json:"order_id"`
}

// TestPublishInjectsTraceparent is the narrow claim: the message that lands on
// the transport carries a W3C traceparent naming the producer span.
func TestPublishInjectsTraceparent(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	backend := queue.NewMemory()
	pub := queue.NewPublisher(backend, rec.Tracer, rec.Propagator)

	ctx, root := rec.Tracer.Start(context.Background(), "incoming request")
	msg, err := pub.Publish(ctx, "orders.fulfil", job{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	root.End()

	tp, ok := msg.Headers["traceparent"]
	if !ok {
		t.Fatalf("published message has no traceparent header; headers: %v", msg.Headers)
	}

	publishSpan := rec.Span(t, "orders.fulfil publish")
	// Parse the header the way a foreign runtime would, and check it names the
	// publish span rather than the surrounding request. If Inject ran before
	// tracer.Start, this is where it shows up.
	extracted := rec.Propagator.Extract(context.Background(), propagation.MapCarrier{"traceparent": tp})
	sc := trace.SpanContextFromContext(extracted)

	if got, want := sc.TraceID(), publishSpan.SpanContext.TraceID(); got != want {
		t.Errorf("traceparent trace id = %s, want the publish span's %s", got, want)
	}
	if got, want := sc.SpanID(), publishSpan.SpanContext.SpanID(); got != want {
		t.Errorf("traceparent span id = %s, want the publish span's %s (injected from the wrong context?)", got, want)
	}
	if !sc.IsSampled() {
		t.Error("traceparent is not marked sampled; a downstream ParentBased sampler would drop the consumer half")
	}
}

// TestTraceSurvivesAsyncHop is the headline assertion: after a full
// publish/consume round trip through a transport that preserves nothing but the
// bytes, the consumer's work is in the same trace and parented on the publish.
func TestTraceSurvivesAsyncHop(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	backend := queue.NewMemory()
	pub := queue.NewPublisher(backend, rec.Tracer, rec.Propagator)
	con := queue.NewConsumer(backend, rec.Tracer, rec.Propagator)

	ctx, root := rec.Tracer.Start(context.Background(), "POST /checkout")
	if _, err := pub.Publish(ctx, "orders.fulfil", job{OrderID: "o-7"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	root.End()

	// Round-trip through the transport. Serialising to JSON and back is what
	// Redis does, and it is the step that discards anything not in Headers.
	raw, err := backend.Dequeue(context.Background(), "orders.fulfil", time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	var msg queue.Message
	if err := json.Unmarshal(encoded, &msg); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}

	// The worker's polling loop runs inside its own long-lived span. A naive
	// consumer passes this context straight to tracer.Start and silently
	// re-parents every job onto it; ConsumeContext must not.
	loopCtx, loopSpan := rec.Tracer.Start(context.Background(), "worker loop")

	var handlerTraceID trace.TraceID
	err = con.Process(loopCtx, msg, func(ctx context.Context, m queue.Message) error {
		_, work := rec.Tracer.Start(ctx, "reserve stock")
		handlerTraceID = trace.SpanContextFromContext(ctx).TraceID()
		work.End()

		var decoded job
		if err := json.Unmarshal(m.Body, &decoded); err != nil {
			return err
		}
		if decoded.OrderID != "o-7" {
			t.Errorf("handler got order %q, want o-7", decoded.OrderID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	loopSpan.End()

	request := rec.Span(t, "POST /checkout")
	publish := rec.Span(t, "orders.fulfil publish")
	process := rec.Span(t, "orders.fulfil process")
	work := rec.Span(t, "reserve stock")
	loop := rec.Span(t, "worker loop")

	wantTrace := request.SpanContext.TraceID()
	for _, s := range []struct {
		name string
		id   trace.TraceID
	}{
		{"publish", publish.SpanContext.TraceID()},
		{"process", process.SpanContext.TraceID()},
		{"reserve stock", work.SpanContext.TraceID()},
		{"handler context", handlerTraceID},
	} {
		if s.id != wantTrace {
			t.Errorf("%s trace id = %s, want %s: the trace did not survive the queue", s.name, s.id, wantTrace)
		}
	}

	if got, want := publish.Parent.SpanID(), request.SpanContext.SpanID(); got != want {
		t.Errorf("publish parent = %s, want the request span %s", got, want)
	}
	if got, want := process.Parent.SpanID(), publish.SpanContext.SpanID(); got != want {
		t.Errorf("process parent = %s, want the publish span %s", got, want)
	}
	if got, want := work.Parent.SpanID(), process.SpanContext.SpanID(); got != want {
		t.Errorf("handler work parent = %s, want the process span %s", got, want)
	}

	// The worker's own loop span must be a separate trace. If it is not, the
	// consumer inherited the loop context and every job in the worker's
	// lifetime would hang off one span.
	if loop.SpanContext.TraceID() == wantTrace {
		t.Error("worker loop span joined the request trace; the consumer did not reset the local span context")
	}
	if !process.Parent.IsRemote() {
		t.Error("process parent is not marked remote; it was taken from local context, not from the message")
	}
}

// TestProcessPreservesCancellation checks the other half of ConsumeContext:
// extracting a remote parent must not throw away the worker's shutdown signal.
func TestProcessPreservesCancellation(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	backend := queue.NewMemory()
	pub := queue.NewPublisher(backend, rec.Tracer, rec.Propagator)
	con := queue.NewConsumer(backend, rec.Tracer, rec.Propagator)

	ctx, root := rec.Tracer.Start(context.Background(), "request")
	if _, err := pub.Publish(ctx, "t", job{OrderID: "x"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	root.End()
	msg, err := backend.Dequeue(context.Background(), "t", time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	var sawCancellation bool
	if err := con.Process(cancelCtx, msg, func(ctx context.Context, _ queue.Message) error {
		sawCancellation = ctx.Err() != nil
		return nil
	}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !sawCancellation {
		t.Error("handler context was not cancelled; ConsumeContext extracted into a detached root")
	}
}

// TestProcessRecordsHandlerFailure checks that a failing job produces an error
// trace a backend can find, not just a log line.
func TestProcessRecordsHandlerFailure(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	backend := queue.NewMemory()
	pub := queue.NewPublisher(backend, rec.Tracer, rec.Propagator)
	con := queue.NewConsumer(backend, rec.Tracer, rec.Propagator)

	ctx, root := rec.Tracer.Start(context.Background(), "request")
	if _, err := pub.Publish(ctx, "t", job{OrderID: "x"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	root.End()
	msg, err := backend.Dequeue(context.Background(), "t", time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	want := errors.New("warehouse unreachable")
	gotErr := con.Process(context.Background(), msg, func(context.Context, queue.Message) error {
		return want
	})
	if !errors.Is(gotErr, want) {
		t.Fatalf("Process returned %v, want %v", gotErr, want)
	}

	process := rec.Span(t, "t process")
	if process.Status.Code != codes.Error {
		t.Errorf("process span status = %v, want Error", process.Status.Code)
	}
	if process.Status.Description != want.Error() {
		t.Errorf("process span status description = %q, want %q", process.Status.Description, want.Error())
	}
	ev := obstest.Event(t, process, "exception")
	if got := obstest.EventAttr(t, ev, "exception.message"); got != want.Error() {
		t.Errorf("exception.message = %q, want %q", got, want.Error())
	}
	if obstest.EventAttr(t, ev, "exception.stacktrace") == "" {
		t.Error("exception event has an empty stack trace")
	}
	if got := obstest.RequireAttr(t, process, spanerr.AttrErrorKind); got != string(spanerr.KindServer) {
		t.Errorf("error.kind = %q, want %q", got, spanerr.KindServer)
	}
}

// TestProcessLeavesClientFaultUnset guards the SLO: a caller's bad input must
// be visible on the span without colouring the service's error rate.
func TestProcessLeavesClientFaultUnset(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	backend := queue.NewMemory()
	pub := queue.NewPublisher(backend, rec.Tracer, rec.Propagator)
	con := queue.NewConsumer(backend, rec.Tracer, rec.Propagator)

	ctx, root := rec.Tracer.Start(context.Background(), "request")
	if _, err := pub.Publish(ctx, "t", job{OrderID: "x"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	root.End()
	msg, err := backend.Dequeue(context.Background(), "t", time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	bad := spanerr.NewHTTPError(422, "unprocessable_job", errors.New("no such sku"))
	if err := con.Process(context.Background(), msg, func(context.Context, queue.Message) error {
		return bad
	}); !errors.Is(err, bad) {
		t.Fatalf("Process returned %v, want the client error", err)
	}

	process := rec.Span(t, "t process")
	if process.Status.Code != codes.Unset {
		t.Errorf("process span status = %v, want Unset for a client fault", process.Status.Code)
	}
	// Still recorded, so the trace is debuggable.
	obstest.Event(t, process, "exception")
	if got := obstest.RequireAttr(t, process, spanerr.AttrErrorKind); got != string(spanerr.KindClient) {
		t.Errorf("error.kind = %q, want %q", got, spanerr.KindClient)
	}
}

// TestConsumerRunDrainsQueue exercises the polling loop rather than Process
// alone, so the loop's own context handling is covered.
func TestConsumerRunDrainsQueue(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	backend := queue.NewMemory()
	pub := queue.NewPublisher(backend, rec.Tracer, rec.Propagator)
	con := queue.NewConsumer(backend, rec.Tracer, rec.Propagator)
	con.PollWait = 20 * time.Millisecond

	const n = 5
	roots := make([]trace.TraceID, 0, n)
	for i := 0; i < n; i++ {
		ctx, root := rec.Tracer.Start(context.Background(), "request")
		roots = append(roots, root.SpanContext().TraceID())
		if _, err := pub.Publish(ctx, "bulk", job{OrderID: "o"}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
		root.End()
	}

	runCtx, cancel := context.WithCancel(context.Background())
	seen := make(chan trace.TraceID, n)
	done := make(chan error, 1)
	go func() {
		done <- con.Run(runCtx, "bulk", func(ctx context.Context, _ queue.Message) error {
			seen <- trace.SpanContextFromContext(ctx).TraceID()
			return nil
		})
	}()

	got := make(map[trace.TraceID]bool, n)
	for i := 0; i < n; i++ {
		select {
		case id := <-seen:
			got[id] = true
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("timed out after %d of %d messages", i, n)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Each job must land under its own originating trace, not all under one.
	if len(got) != n {
		t.Errorf("handler saw %d distinct trace ids, want %d", len(got), n)
	}
	for i, id := range roots {
		if !got[id] {
			t.Errorf("trace %d (%s) never reached the handler", i, id)
		}
	}
}

// TestPublishWithoutPropagatorFails pins the loud-failure behaviour: shipping a
// message with no traceparent is the bug this package prevents, so it must not
// be silent.
func TestPublishWithoutPropagatorFails(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	backend := queue.NewMemory()
	// propagation.NewCompositeTextMapPropagator() with no members injects
	// nothing — the exact state of a service that forgot SetTextMapPropagator.
	pub := queue.NewPublisher(backend, rec.Tracer, propagation.NewCompositeTextMapPropagator())

	ctx, root := rec.Tracer.Start(context.Background(), "request")
	_, err := pub.Publish(ctx, "t", job{OrderID: "x"})
	root.End()

	if err == nil {
		t.Fatal("Publish succeeded with no propagator installed; it must refuse to ship an uncorrelated message")
	}
	if n := backend.Len("t"); n != 0 {
		t.Errorf("queue depth = %d, want 0: an uncorrelated message was enqueued anyway", n)
	}
	publish := rec.Span(t, "t publish")
	if publish.Status.Code != codes.Error {
		t.Errorf("publish span status = %v, want Error", publish.Status.Code)
	}
}

// TestUnsampledParentIsNotResampled covers the ParentBased sampler decision
// that obs.Setup installs: a parent that was dropped upstream must stay
// dropped, or a trace arrives with holes in the middle.
func TestUnsampledParentIsNotResampled(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{
		Sampler: sdktrace.ParentBased(sdktrace.NeverSample()),
	})
	backend := queue.NewMemory()
	con := queue.NewConsumer(backend, rec.Tracer, rec.Propagator)

	// A traceparent whose flags byte is 00: sampled=false.
	msg := queue.Message{
		ID:    "m1",
		Topic: "t",
		Headers: map[string]string{
			"traceparent": "00-11111111111111111111111111111111-2222222222222222-00",
		},
		Body: json.RawMessage(`{}`),
	}

	var recording bool
	if err := con.Process(context.Background(), msg, func(ctx context.Context, _ queue.Message) error {
		recording = trace.SpanFromContext(ctx).IsRecording()
		return nil
	}); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if recording {
		t.Error("consumer span is recording for an unsampled parent; ParentBased was bypassed")
	}
	if n := len(rec.Spans()); n != 0 {
		t.Errorf("exported %d spans for an unsampled trace, want 0: %v", n, rec.Names())
	}
}
