import api from '@opentelemetry/api';
import { InMemoryLogRecordExporter, LoggerProvider, SimpleLogRecordProcessor } from '@opentelemetry/sdk-logs';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';

import { LEVELS, createLogger, parseLevel, type Sink } from './logger.js';
import { buildResource } from './telemetry.js';
import { TEST_CONFIG, newRecorder } from './testing.js';

const rec = newRecorder();

beforeEach(() => rec.reset());
afterAll(() => rec.shutdown());

function capture(): { lines: string[]; sink: Sink } {
  const lines: string[] = [];
  return { lines, sink: { write: (line) => lines.push(line) } };
}

describe('createLogger', () => {
  it('stamps the active trace onto the line, in the keys Grafana parses', () => {
    const { lines, sink } = capture();
    const logger = createLogger({ sink, now: () => new Date('2026-10-28T12:00:00Z') });

    const span = rec.tracer.startSpan('checkout');
    api.context.with(api.trace.setSpan(api.context.active(), span), () => {
      logger.info('reserving stock', { sku: 'SKU-1' });
    });
    span.end();

    expect(lines).toHaveLength(1);
    const line = JSON.parse(lines[0]!);
    expect(line.trace_id).toBe(span.spanContext().traceId);
    expect(line.span_id).toBe(span.spanContext().spanId);
    expect(line.trace_flags).toBe('01');
    expect(line.sku).toBe('SKU-1');
    expect(line.msg).toBe('reserving stock');
    expect(line.level).toBe('info');
    expect(line.time).toBe('2026-10-28T12:00:00.000Z');
  });

  it('omits the ids outside a span rather than writing zeros', () => {
    const { lines, sink } = capture();
    createLogger({ sink }).info('worker starting');

    const line = JSON.parse(lines[0]!);
    expect(line.trace_id).toBeUndefined();
    expect(line.span_id).toBeUndefined();
    expect(line.trace_flags).toBeUndefined();
  });

  it('honours the level threshold', () => {
    const { lines, sink } = capture();
    const logger = createLogger({ sink, level: 'warn' });
    logger.debug('a');
    logger.info('b');
    logger.warn('c');
    logger.error('d');
    expect(lines.map((l) => JSON.parse(l).msg)).toEqual(['c', 'd']);
  });

  it('merges base fields into every line', () => {
    const { lines, sink } = capture();
    createLogger({ sink, base: { 'service.name': 'fulfillment' } }).info('x');
    expect(JSON.parse(lines[0]!)['service.name']).toBe('fulfillment');
  });

  it('also emits an OTLP log record carrying the trace context', () => {
    const exporter = new InMemoryLogRecordExporter();
    const loggerProvider = new LoggerProvider({
      resource: buildResource(TEST_CONFIG),
      processors: [new SimpleLogRecordProcessor({ exporter })],
    });
    const { sink } = capture();
    const logger = createLogger({ sink, loggerProvider, scope: 'test' });

    const span = rec.tracer.startSpan('checkout');
    api.context.with(api.trace.setSpan(api.context.active(), span), () => {
      logger.error('inventory unreachable', { attempt: 3 });
    });
    span.end();

    const records = exporter.getFinishedLogRecords();
    expect(records).toHaveLength(1);
    const record = records[0]!;
    // This is the path Loki actually receives, so the correlation has to be on
    // the record itself and not only on the stdout line.
    expect(record.spanContext?.traceId).toBe(span.spanContext().traceId);
    expect(record.spanContext?.spanId).toBe(span.spanContext().spanId);
    expect(record.body).toBe('inventory unreachable');
    expect(record.severityText).toBe('ERROR');
    expect(record.attributes['attempt']).toBe(3);
    expect(record.resource.attributes['service.name']).toBe('fulfillment');
  });
});

describe('parseLevel', () => {
  it('accepts every supported level', () => {
    for (const level of LEVELS) {
      expect(parseLevel(level)).toBe(level);
    }
    expect(parseLevel('WARN')).toBe('warn');
    expect(parseLevel('warning')).toBe('warn');
  });

  it('falls back to info', () => {
    expect(parseLevel(undefined)).toBe('info');
    expect(parseLevel('')).toBe('info');
    expect(parseLevel('verbose')).toBe('info');
  });
});
