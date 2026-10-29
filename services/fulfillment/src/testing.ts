/**
 * Test harness: an in-memory trace pipeline, so every claim this worker makes
 * about propagation can be asserted without a collector, a backend or Docker.
 */
import api, { type Tracer } from '@opentelemetry/api';
import {
  InMemorySpanExporter,
  type ReadableSpan,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace';

import { buildResource, installGlobals, type TelemetryConfig } from './telemetry.js';
import { TracerProvider } from '@opentelemetry/sdk-trace';

/** A recorder over an in-memory exporter. */
export interface Recorder {
  tracer: Tracer;
  spans: () => ReadableSpan[];
  names: () => string[];
  reset: () => void;
  /** Returns the single span with this name, throwing when it is not unique. */
  span: (name: string) => ReadableSpan;
  shutdown: () => Promise<void>;
}

/** The default config used by tests. */
export const TEST_CONFIG: TelemetryConfig = {
  serviceName: 'fulfillment',
  serviceVersion: '1.2.3',
  environment: 'test',
  serviceInstanceId: 'fulfillment-test',
  otlpEndpoint: 'http://localhost:4318',
};

/** Builds a recorder and installs it as the global tracer provider. */
export function newRecorder(config: TelemetryConfig = TEST_CONFIG): Recorder {
  const exporter = new InMemorySpanExporter();
  const provider = new TracerProvider({
    resource: buildResource(config),
    // Synchronous: an assertion must never race a batch interval.
    spanProcessors: [new SimpleSpanProcessor({ exporter })],
  });

  installGlobals();
  api.trace.setGlobalTracerProvider(provider);

  const spans = (): ReadableSpan[] => exporter.getFinishedSpans();

  return {
    tracer: provider.getTracer(config.serviceName, config.serviceVersion),
    spans,
    names: () => spans().map((s) => s.name),
    reset: () => exporter.reset(),
    span: (name) => {
      const found = spans().filter((s) => s.name === name);
      if (found.length === 0) {
        throw new Error(`no span named "${name}"; recorded: ${JSON.stringify(spans().map((s) => s.name))}`);
      }
      if (found.length > 1) {
        throw new Error(`expected exactly one span named "${name}", got ${found.length}`);
      }
      return found[0]!;
    },
    shutdown: () => provider.shutdown(),
  };
}

/** Returns a span's attribute as a string, or undefined. */
export function attr(span: ReadableSpan, key: string): string | undefined {
  const value = span.attributes[key];
  return value === undefined ? undefined : String(value);
}

/** Returns a span's parent span id, or undefined for a root span. */
export function parentSpanId(span: ReadableSpan): string | undefined {
  return span.parentSpanContext?.spanId;
}

/** Builds a `traceparent` header value. */
export function traceparent(traceId: string, spanId: string, sampled = true): string {
  return `00-${traceId}-${spanId}-${sampled ? '01' : '00'}`;
}
