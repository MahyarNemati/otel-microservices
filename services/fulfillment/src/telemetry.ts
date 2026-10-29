/**
 * Telemetry wiring for the fulfilment worker.
 *
 * This file is the Node mirror of Go's `internal/obs`: the resource attributes
 * are built in one place and reused by both the trace and the log pipelines, so
 * a Grafana panel that groups by `service.version` sees the Go services and
 * this one as the same kind of thing.
 *
 * The three settings that matter for cross-runtime propagation:
 *
 *  - `W3CTraceContextPropagator` must be the global propagator. Without it,
 *    `propagation.extract` returns the context unchanged and every job becomes
 *    a root span, which is the orphan-trace bug seen from the Node side.
 *  - `AsyncLocalStorageContextManager` must be installed, or `context.active()`
 *    returns ROOT_CONTEXT after the first `await` and the child spans of an
 *    async handler detach from their parent.
 *  - The sampler must be parent-based, so a decision made by the Go gateway is
 *    honoured here rather than re-rolled.
 */
import api, {
  type Context,
  type Span,
  SpanStatusCode,
  type Tracer,
} from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CBaggagePropagator, W3CTraceContextPropagator, CompositePropagator } from '@opentelemetry/core';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-http';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { type Resource, resourceFromAttributes } from '@opentelemetry/resources';
import { BatchLogRecordProcessor, LoggerProvider } from '@opentelemetry/sdk-logs';
import {
  AlwaysOnSampler,
  BatchSpanProcessor,
  ParentBasedSampler,
  type SpanProcessor,
  TracerProvider,
} from '@opentelemetry/sdk-trace';
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions';

/**
 * Resource attribute keys. `deployment.environment.name` and
 * `service.instance.id` have no stable export in the semantic-conventions
 * package yet, so they are named here — and they must match
 * `internal/obs/resource.go` exactly, or the dashboards split one deployment
 * into two series.
 */
export const ATTR_DEPLOYMENT_ENVIRONMENT = 'deployment.environment.name';
export const ATTR_SERVICE_INSTANCE_ID = 'service.instance.id';

/** Configuration for the worker's telemetry. */
export interface TelemetryConfig {
  serviceName: string;
  serviceVersion: string;
  environment: string;
  serviceInstanceId: string;
  /** Base URL of the collector's OTLP/HTTP receiver, without a signal path. */
  otlpEndpoint: string;
}

/** Reads the same environment variables the Go services read. */
export function configFromEnv(serviceName = 'fulfillment'): TelemetryConfig {
  const env = process.env;
  return {
    serviceName: env['OTEL_SERVICE_NAME']?.trim() || serviceName,
    serviceVersion: env['SERVICE_VERSION']?.trim() || '0.0.0-dev',
    environment: env['DEPLOYMENT_ENVIRONMENT']?.trim() || 'local',
    serviceInstanceId: env['SERVICE_INSTANCE_ID']?.trim() || env['HOSTNAME']?.trim() || 'unknown',
    otlpEndpoint: env['OTEL_EXPORTER_OTLP_ENDPOINT']?.trim() || 'http://localhost:4318',
  };
}

/** Builds the resource shared by every signal this process emits. */
export function buildResource(cfg: TelemetryConfig): Resource {
  return resourceFromAttributes({
    [ATTR_SERVICE_NAME]: cfg.serviceName,
    [ATTR_SERVICE_VERSION]: cfg.serviceVersion,
    [ATTR_DEPLOYMENT_ENVIRONMENT]: cfg.environment,
    [ATTR_SERVICE_INSTANCE_ID]: cfg.serviceInstanceId,
  });
}

/** Installs the context manager and propagator that make propagation work. */
export function installGlobals(): void {
  api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  api.propagation.setGlobalPropagator(
    new CompositePropagator({
      propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()],
    }),
  );
}

/** A live telemetry pipeline. */
export interface Telemetry {
  tracer: Tracer;
  loggerProvider: LoggerProvider;
  tracerProvider: TracerProvider;
  shutdown: () => Promise<void>;
  forceFlush: () => Promise<void>;
}

/**
 * Builds the worker's telemetry.
 *
 * `spanProcessors` is injectable so the tests can swap the OTLP batch exporter
 * for an in-memory one and assert on the spans a job actually produced.
 */
export function setup(cfg: TelemetryConfig, spanProcessors?: SpanProcessor[]): Telemetry {
  const resource = buildResource(cfg);

  const processors =
    spanProcessors ??
    [
      new BatchSpanProcessor({
        exporter: new OTLPTraceExporter({ url: `${cfg.otlpEndpoint}/v1/traces` }),
      }),
    ];

  const tracerProvider = new TracerProvider({
    resource,
    spanProcessors: processors,
    // Honour an upstream decision; fall back to sampling when this process is
    // the root, which only happens for the worker's own start-up span.
    sampler: new ParentBasedSampler({ root: new AlwaysOnSampler() }),
  });

  const loggerProvider = new LoggerProvider({
    resource,
    processors: [
      new BatchLogRecordProcessor({
        exporter: new OTLPLogExporter({ url: `${cfg.otlpEndpoint}/v1/logs` }),
      }),
    ],
  });

  installGlobals();
  api.trace.setGlobalTracerProvider(tracerProvider);

  return {
    tracer: tracerProvider.getTracer(cfg.serviceName, cfg.serviceVersion),
    tracerProvider,
    loggerProvider,
    forceFlush: async () => {
      await tracerProvider.forceFlush();
      await loggerProvider.forceFlush();
    },
    shutdown: async () => {
      await tracerProvider.shutdown();
      await loggerProvider.shutdown();
    },
  };
}

/**
 * Records an error on a span the way a backend can use it: an exception event
 * with a stack trace plus an Error status, rather than a string attribute that
 * leaves the span's status Unset and invisible to Tempo's error filter.
 *
 * The Go side of this lives in `internal/spanerr`; the two must agree, because
 * a trace that spans both runtimes is read as one thing.
 */
export function recordError(span: Span, err: unknown, kind: 'client' | 'server' = 'server'): Error {
  const error = err instanceof Error ? err : new Error(String(err));
  span.setAttribute('error.kind', kind);
  span.recordException(error);
  if (kind === 'server') {
    span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
  }
  return error;
}

/** Returns the active trace and span ids, or undefined outside a span. */
export function traceContext(ctx: Context = api.context.active()):
  | { trace_id: string; span_id: string; trace_flags: string }
  | undefined {
  const sc = api.trace.getSpanContext(ctx);
  if (!sc || !api.trace.isSpanContextValid(sc)) {
    return undefined;
  }
  return {
    trace_id: sc.traceId,
    span_id: sc.spanId,
    trace_flags: sc.traceFlags.toString(16).padStart(2, '0'),
  };
}
