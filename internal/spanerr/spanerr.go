// Package spanerr records errors onto spans the way a backend can actually use.
//
// The common mistake is span.SetAttributes(attribute.String("error", err.Error())).
// That produces a string on a span whose status is still Unset, so the trace is
// not an error trace: Tempo's error filter misses it, the tail sampler's
// status_code policy misses it, and the RED-metrics error rate stays at zero
// while users see 500s. A correctly recorded error sets the span status *and*
// adds an `exception` event carrying exception.type, exception.message and
// exception.stacktrace.
package spanerr

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Kind classifies a failure so a span can distinguish "the caller sent
// nonsense" from "we broke". Only the second should colour an error rate.
type Kind string

const (
	// KindClient is a caller fault: a 4xx. It is recorded as an exception event
	// for debuggability but leaves the span status Unset, because counting
	// validation failures as server errors makes an SLO meaningless.
	KindClient Kind = "client"
	// KindServer is our fault: a 5xx. Sets the span status to Error.
	KindServer Kind = "server"
)

// AttrErrorKind labels which side of the boundary the failure came from.
const AttrErrorKind = "error.kind"

// Record attaches err to span: an exception event with a stack trace always,
// and an Error status when kind is [KindServer].
//
// It returns err unchanged so call sites can write
// `return spanerr.Record(span, spanerr.KindServer, err)`.
func Record(span trace.Span, kind Kind, err error) error {
	if span == nil || err == nil {
		return err
	}

	span.SetAttributes(attribute.String(AttrErrorKind, string(kind)))

	// WithStackTrace is the whole point: the exception event without
	// exception.stacktrace tells you a thing failed but not where.
	span.RecordError(err,
		trace.WithStackTrace(true),
		trace.WithAttributes(attribute.String("exception.kind", string(kind))),
	)

	if kind == KindServer {
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// RecordServer is shorthand for a server-side failure.
func RecordServer(span trace.Span, err error) error { return Record(span, KindServer, err) }

// RecordClient is shorthand for a caller-side failure.
func RecordClient(span trace.Span, err error) error { return Record(span, KindClient, err) }

// HTTPError is an error that knows its own status code, so a handler can decide
// the span's kind from the error rather than from a second parallel switch.
type HTTPError struct {
	Status int
	Code   string
	Err    error
}

func (e *HTTPError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("%s (status %d)", e.Code, e.Status)
	}
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}

func (e *HTTPError) Unwrap() error { return e.Err }

// Kind reports whether this is a caller or server fault.
func (e *HTTPError) Kind() Kind {
	if e.Status >= 400 && e.Status < 500 {
		return KindClient
	}
	return KindServer
}

// NewHTTPError builds an HTTPError.
func NewHTTPError(status int, code string, err error) *HTTPError {
	return &HTTPError{Status: status, Code: code, Err: err}
}

// RecordHTTP records err on span, deriving the kind from an [HTTPError] in the
// chain and defaulting to a server fault when there is none. It returns the
// status code to write.
func RecordHTTP(span trace.Span, err error) int {
	if err == nil {
		return 200
	}
	var he *HTTPError
	if errors.As(err, &he) {
		Record(span, he.Kind(), err)
		return he.Status
	}
	RecordServer(span, err)
	return 500
}

// RecordAuto records err with the kind implied by an [HTTPError] in the chain,
// defaulting to a server fault. It returns err so it can be used in a return
// statement.
func RecordAuto(span trace.Span, err error) error {
	if err == nil {
		return nil
	}
	var he *HTTPError
	if errors.As(err, &he) {
		return Record(span, he.Kind(), err)
	}
	return RecordServer(span, err)
}
