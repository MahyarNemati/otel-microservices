// Package queue carries W3C trace context across an asynchronous boundary.
//
// This is where end-to-end tracing usually breaks. An HTTP hop propagates
// context for free because every instrumentation library knows to read the
// traceparent header. A queue has no headers unless you put them there, so the
// naive worker calls tracer.Start(context.Background(), ...) and produces an
// orphan trace: the enqueue is in one trace and the work is in another, with
// nothing joining them. The symptom is a Tempo search that shows a tidy
// gateway->orders trace ending at the publish, and a separate single-span
// trace per job.
//
// The fix has three parts, all of them here:
//
//  1. The message carries a propagation envelope ([Message.Headers]) and the
//     publisher injects into it from *inside* the producer span, so the
//     traceparent names the enqueue rather than the surrounding request.
//  2. The consumer extracts into a fresh root context rather than reusing the
//     polling loop's context, so the job's parent is the producer span and not
//     the worker's long-lived startup span.
//  3. Both spans get the messaging.* attributes and the right span kinds
//     (PRODUCER / CONSUMER), which is what makes Tempo render the gap between
//     them as queue time instead of as an unexplained hole.
package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/spanerr"
)

// ErrNoMessage is returned by a [Backend] poll that timed out with nothing to
// hand back. It is an ordinary idle tick, not a failure, and must not be
// recorded on a span.
var ErrNoMessage = errors.New("queue: no message available")

// Message is one job on the queue.
//
// Headers is the propagation envelope. It is a separate map rather than fields
// on Body on purpose: the transport concern (which trace this belongs to) stays
// out of the business payload, exactly as it does for HTTP.
type Message struct {
	ID      string            `json:"id"`
	Topic   string            `json:"topic"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

// Carrier adapts a message's headers to the OTel propagation interface.
type Carrier struct {
	msg *Message
}

var _ propagation.TextMapCarrier = Carrier{}

// Get implements propagation.TextMapCarrier.
func (c Carrier) Get(key string) string {
	if c.msg == nil || c.msg.Headers == nil {
		return ""
	}
	return c.msg.Headers[key]
}

// Set implements propagation.TextMapCarrier.
func (c Carrier) Set(key, value string) {
	if c.msg == nil {
		return
	}
	if c.msg.Headers == nil {
		c.msg.Headers = make(map[string]string, 2)
	}
	c.msg.Headers[key] = value
}

// Keys implements propagation.TextMapCarrier.
func (c Carrier) Keys() []string {
	if c.msg == nil {
		return nil
	}
	keys := make([]string, 0, len(c.msg.Headers))
	for k := range c.msg.Headers {
		keys = append(keys, k)
	}
	return keys
}

// CarrierFor returns the propagation carrier for msg.
func CarrierFor(msg *Message) Carrier { return Carrier{msg: msg} }

// Backend is the transport under the tracing. Implemented by the in-memory
// queue used in tests and by the Redis list used under compose; neither knows
// anything about tracing, which is the point — the propagation logic is tested
// once and is identical for both.
type Backend interface {
	// Enqueue appends msg to topic.
	Enqueue(ctx context.Context, topic string, msg Message) error
	// Dequeue blocks for at most wait and returns [ErrNoMessage] on timeout.
	Dequeue(ctx context.Context, topic string, wait time.Duration) (Message, error)
	// Depth reports the number of queued messages, for the queue-depth metric.
	Depth(ctx context.Context, topic string) (int64, error)
	// Close releases transport resources.
	Close() error
}

// System is the messaging.system attribute value used by both spans.
const System = "otel-microservices.queue"

// Publisher wraps a [Backend] with a producer span and context injection.
type Publisher struct {
	backend    Backend
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	newID      func() string
}

// NewPublisher builds a publisher. Passing a nil tracer or propagator falls
// back to the globals installed by obs.Setup.
func NewPublisher(backend Backend, tracer trace.Tracer, propagator propagation.TextMapPropagator) *Publisher {
	if tracer == nil {
		tracer = otel.Tracer("internal/queue")
	}
	if propagator == nil {
		propagator = otel.GetTextMapPropagator()
	}
	return &Publisher{backend: backend, tracer: tracer, propagator: propagator, newID: newMessageID}
}

// Publish serialises body, stamps the active trace context onto the message and
// enqueues it. It returns the message as sent, so a caller can log its id.
func (p *Publisher) Publish(ctx context.Context, topic string, body any) (Message, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return Message{}, fmt.Errorf("queue: marshal body: %w", err)
	}

	msg := Message{ID: p.newID(), Topic: topic, Body: payload, Headers: map[string]string{}}

	// Span naming follows the messaging conventions: "<destination> publish".
	ctx, span := p.tracer.Start(ctx, topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String(System),
			semconv.MessagingDestinationName(topic),
			semconv.MessagingMessageID(msg.ID),
			semconv.MessagingMessageBodySize(len(payload)),
		),
	)
	defer span.End()

	// Inject from ctx *after* Start, so the traceparent on the wire points at
	// this producer span. Injecting the caller's context instead would make the
	// consumer a sibling of the publish rather than its child, and the queue
	// wait would vanish from the waterfall.
	p.propagator.Inject(ctx, CarrierFor(&msg))

	if _, ok := msg.Headers["traceparent"]; !ok {
		// A missing traceparent here means no propagator was installed. Fail
		// loudly: silently shipping an uncorrelated message is the bug this
		// whole package exists to prevent.
		err := errors.New("queue: propagator produced no traceparent; was obs.Setup called?")
		return msg, spanerr.RecordServer(span, err)
	}

	if err := p.backend.Enqueue(ctx, topic, msg); err != nil {
		return msg, spanerr.RecordServer(span, fmt.Errorf("queue: enqueue %s: %w", topic, err))
	}
	return msg, nil
}

// Handler processes one message. The ctx it receives is already a child of the
// producer span, so anything it traces lands in the original trace.
type Handler func(ctx context.Context, msg Message) error

// Consumer polls a [Backend] and runs a [Handler] per message inside a
// correctly parented consumer span.
type Consumer struct {
	backend    Backend
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator

	// PollWait bounds one Dequeue call.
	PollWait time.Duration
	// OnError is called for a handler or transport failure. Nil means ignore.
	OnError func(ctx context.Context, msg Message, err error)
}

// NewConsumer builds a consumer with sane defaults.
func NewConsumer(backend Backend, tracer trace.Tracer, propagator propagation.TextMapPropagator) *Consumer {
	if tracer == nil {
		tracer = otel.Tracer("internal/queue")
	}
	if propagator == nil {
		propagator = otel.GetTextMapPropagator()
	}
	return &Consumer{backend: backend, tracer: tracer, propagator: propagator, PollWait: time.Second}
}

// ConsumeContext returns the context a message should be handled under: the
// remote trace context extracted from its headers, carrying over the
// cancellation of parent.
//
// Both halves matter. Extracting into [context.Background] would detach the
// worker's shutdown signal and leave handlers running after SIGTERM; not
// extracting at all — the default if you simply pass the loop's context — makes
// every job a root span.
func (c *Consumer) ConsumeContext(parent context.Context, msg Message) context.Context {
	// Strip any span already on parent first. The polling loop usually runs
	// inside the worker's own startup span; without this the extracted remote
	// context would be overwritten only for the span key and a handler that
	// happened to ignore the returned context would silently re-parent onto the
	// startup span.
	root := trace.ContextWithSpanContext(parent, trace.SpanContext{})
	return c.propagator.Extract(root, CarrierFor(&msg))
}

// Process runs one message through h inside a consumer span.
func (c *Consumer) Process(ctx context.Context, msg Message, h Handler) error {
	ctx = c.ConsumeContext(ctx, msg)

	ctx, span := c.tracer.Start(ctx, msg.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String(System),
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingMessageID(msg.ID),
			semconv.MessagingOperationName("process"),
		),
	)
	defer span.End()

	if err := h(ctx, msg); err != nil {
		return spanerr.RecordAuto(span, err)
	}
	return nil
}

// Run polls topic until ctx is cancelled, handing each message to h.
func (c *Consumer) Run(ctx context.Context, topic string, h Handler) error {
	wait := c.PollWait
	if wait <= 0 {
		wait = time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		msg, err := c.backend.Dequeue(ctx, topic, wait)
		switch {
		case errors.Is(err, ErrNoMessage):
			continue
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil
		case err != nil:
			if c.OnError != nil {
				c.OnError(ctx, msg, fmt.Errorf("queue: dequeue %s: %w", topic, err))
			}
			// Back off so a broken transport does not spin the CPU.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		if err := c.Process(ctx, msg, h); err != nil && c.OnError != nil {
			c.OnError(ctx, msg, err)
		}
	}
}

func newMessageID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A message id is for correlation, not security; a time-based fallback
		// is better than refusing to publish.
		return fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	return "msg-" + hex.EncodeToString(b[:])
}
