package obs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Providers is the result of [Setup]: the live SDK objects plus a single
// Shutdown that flushes them in the right order.
type Providers struct {
	Tracer trace.Tracer
	Logger *slog.Logger

	tracerProvider *sdktrace.TracerProvider
	loggerProvider *sdklog.LoggerProvider
}

// Shutdown flushes and stops both pipelines. Call it from main's defer: without
// it a short-lived process exits with its last spans still in the batch queue,
// which is the usual reason a trace is missing its final service.
func (p *Providers) Shutdown(ctx context.Context) error {
	var errs []error
	if p.tracerProvider != nil {
		if err := p.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("tracer provider: %w", err))
		}
	}
	if p.loggerProvider != nil {
		if err := p.loggerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("logger provider: %w", err))
		}
	}
	return errors.Join(errs...)
}

// ForceFlush exports anything queued without stopping the pipelines. The
// end-to-end test uses it so an assertion does not race the batch interval.
func (p *Providers) ForceFlush(ctx context.Context) error {
	var errs []error
	if p.tracerProvider != nil {
		if err := p.tracerProvider.ForceFlush(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if p.loggerProvider != nil {
		if err := p.loggerProvider.ForceFlush(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Setup builds the trace and log pipelines for one service and installs them as
// the global providers and propagator.
//
// The propagator is the W3C pair (traceparent/tracestate plus baggage) and
// nothing else. It is set here, once, because a service that forgets it accepts
// no incoming context and starts a fresh trace per hop — the classic symptom of
// "every service has traces but nothing joins up".
func Setup(ctx context.Context, cfg Config) (*Providers, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	res, err := NewResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("obs: build resource: %w", err)
	}

	traceExp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(cfg.OTLPEndpoint+"/v1/traces"),
		otlptracehttp.WithTimeout(cfg.ExportTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("obs: otlp trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
		// ParentBased keeps a sampling decision made upstream. Without it a
		// downstream service re-rolls the dice and produces half a trace.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)

	logExp, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(cfg.OTLPEndpoint+"/v1/logs"),
		otlploghttp.WithTimeout(cfg.ExportTimeout),
	)
	if err != nil {
		// Do not leak the tracer provider if the second exporter fails.
		_ = tp.Shutdown(ctx)
		return nil, fmt.Errorf("obs: otlp log exporter: %w", err)
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Two sinks for one slog call: stdout for `docker compose logs`, and the
	// OTLP bridge for Loki. The bridge reads trace context from the ctx itself,
	// so OTLP log records are correlated natively; TraceHandler does the same
	// job for the textual stream, where there is no structured place to put it.
	handler := NewFanoutHandler(
		NewStdoutHandler(os.Stdout, cfg),
		otelslog.NewHandler(cfg.ServiceName, otelslog.WithLoggerProvider(lp)),
	)
	logger := slog.New(handler).With(
		slog.String(AttrServiceName, cfg.ServiceName),
		slog.String(AttrServiceVersion, cfg.ServiceVersion),
	)

	return &Providers{
		Tracer:         tp.Tracer(cfg.ServiceName),
		Logger:         logger,
		tracerProvider: tp,
		loggerProvider: lp,
	}, nil
}
