package obs

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Keys written onto every log record emitted inside a span. These exact names
// are what deploy/grafana/provisioning/datasources/datasources.yaml parses out
// of a Loki line to build the "Tempo" link, and what Tempo's traces-to-logs
// query filters on coming back the other way.
const (
	LogKeyTraceID = "trace_id"
	LogKeySpanID  = "span_id"
	LogKeyFlags   = "trace_flags"
)

// TraceHandler is a slog.Handler decorator that copies the active span's
// identifiers onto each record.
//
// This is the piece most stacks get half right. Adding the OTel SDK gives you
// traces, and adding a log shipper gives you logs, but without trace_id on the
// log line the two are separate products and an incident means reading both and
// guessing. The handler is deliberately a decorator rather than a bespoke
// handler so it composes with slog.JSONHandler, slog.TextHandler and the
// otelslog bridge alike.
type TraceHandler struct {
	inner slog.Handler
}

// NewTraceHandler wraps inner so that records logged with a context carrying a
// recording span gain trace_id, span_id and trace_flags attributes.
func NewTraceHandler(inner slog.Handler) *TraceHandler {
	return &TraceHandler{inner: inner}
}

// Enabled implements slog.Handler.
func (h *TraceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle implements slog.Handler. A record logged outside a span is passed
// through untouched: emitting trace_id="00000000000000000000000000000000"
// would make every uncorrelated line look like it belonged to one trace.
func (h *TraceHandler) Handle(ctx context.Context, rec slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		rec = rec.Clone()
		rec.AddAttrs(
			slog.String(LogKeyTraceID, sc.TraceID().String()),
			slog.String(LogKeySpanID, sc.SpanID().String()),
			slog.String(LogKeyFlags, sc.TraceFlags().String()),
		)
	}
	return h.inner.Handle(ctx, rec)
}

// WithAttrs implements slog.Handler.
func (h *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{inner: h.inner.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
func (h *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{inner: h.inner.WithGroup(name)}
}

// Inner returns the wrapped handler. Used by tests.
func (h *TraceHandler) Inner() slog.Handler { return h.inner }

// fanout sends each record to every handler, so one slog call reaches both the
// container's stdout (for `docker compose logs`) and the OTLP log pipeline
// (for Loki) without the call site knowing there are two destinations.
type fanout struct {
	handlers []slog.Handler
}

func (f *fanout) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f *fanout) Handle(ctx context.Context, rec slog.Record) error {
	var firstErr error
	for _, h := range f.handlers {
		if !h.Enabled(ctx, rec.Level) {
			continue
		}
		// Each handler gets its own clone: handlers are allowed to retain the
		// record, and slog.Record's attribute slice is shared on copy.
		if err := h.Handle(ctx, rec.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (f *fanout) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		next[i] = h.WithAttrs(attrs)
	}
	return &fanout{handlers: next}
}

func (f *fanout) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		next[i] = h.WithGroup(name)
	}
	return &fanout{handlers: next}
}

// NewFanoutHandler returns a handler that forwards to all of handlers.
func NewFanoutHandler(handlers ...slog.Handler) slog.Handler {
	return &fanout{handlers: handlers}
}

// ParseLevel maps a configuration string onto a slog level, defaulting to info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewStdoutHandler builds the human-and-machine-readable stdout handler,
// already wrapped for trace correlation.
func NewStdoutHandler(w io.Writer, cfg Config) slog.Handler {
	opts := &slog.HandlerOptions{Level: ParseLevel(cfg.LogLevel)}
	var base slog.Handler
	if cfg.LogJSON {
		base = slog.NewJSONHandler(w, opts)
	} else {
		base = slog.NewTextHandler(w, opts)
	}
	return NewTraceHandler(base)
}
