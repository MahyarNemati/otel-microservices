import api, { SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';

import {
  MalformedMessageError,
  consumeContext,
  hasTraceContext,
  parseJob,
  parseMessage,
  queueWaitMs,
  withConsumerSpan,
} from './queue.js';
import { recordError } from './telemetry.js';
import { attr, newRecorder, parentSpanId, traceparent } from './testing.js';

const rec = newRecorder();

// A trace id and span id as the Go publisher would have produced them.
const GO_TRACE_ID = '4bf92f3577b34da6a3ce929d0e0e4736';
const GO_PUBLISH_SPAN_ID = '00f067aa0ba902b7';

function goMessage(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: 'msg-abc',
    topic: 'orders.fulfil',
    headers: { traceparent: traceparent(GO_TRACE_ID, GO_PUBLISH_SPAN_ID) },
    body: {
      order_id: 'ord-1',
      sku: 'SKU-TEA',
      quantity: 2,
      customer: 'ada',
      enqueued_at: '2026-10-28T12:00:00.000Z',
    },
    ...overrides,
  };
}

beforeEach(() => rec.reset());
afterAll(() => rec.shutdown());

describe('parseMessage', () => {
  it('round-trips the envelope the Go publisher wrote', () => {
    const raw = JSON.stringify(goMessage());
    const msg = parseMessage(raw);
    expect(msg.id).toBe('msg-abc');
    expect(msg.topic).toBe('orders.fulfil');
    expect(msg.headers['traceparent']).toBe(traceparent(GO_TRACE_ID, GO_PUBLISH_SPAN_ID));
    expect(parseJob(msg)).toMatchObject({ order_id: 'ord-1', sku: 'SKU-TEA', quantity: 2 });
  });

  it('rejects input that is not a message', () => {
    expect(() => parseMessage('not json')).toThrow(MalformedMessageError);
    expect(() => parseMessage('42')).toThrow(MalformedMessageError);
    expect(() => parseMessage('{}')).toThrow(MalformedMessageError);
  });

  it('tolerates a missing headers map rather than throwing', () => {
    const msg = parseMessage(JSON.stringify({ id: 'm', topic: 't', body: {} }));
    expect(msg.headers).toEqual({});
    expect(hasTraceContext(msg)).toBe(false);
  });

  it('rejects a body with no order', () => {
    const msg = parseMessage(JSON.stringify({ id: 'm', topic: 't', body: { sku: 'X' } }));
    expect(() => parseJob(msg)).toThrow(/order_id/);
  });
});

describe('consumeContext', () => {
  it('extracts the Go publisher span as a remote parent', () => {
    const ctx = consumeContext(parseMessage(JSON.stringify(goMessage())));
    const sc = api.trace.getSpanContext(ctx);
    expect(sc).toBeDefined();
    expect(sc!.traceId).toBe(GO_TRACE_ID);
    expect(sc!.spanId).toBe(GO_PUBLISH_SPAN_ID);
    expect(sc!.isRemote).toBe(true);
    expect(sc!.traceFlags).toBe(1);
  });

  it('yields no span context when the message carries none', () => {
    const ctx = consumeContext(parseMessage(JSON.stringify({ id: 'm', topic: 't', body: {} })));
    expect(api.trace.getSpanContext(ctx)).toBeUndefined();
  });

  it('ignores whatever span is locally active', async () => {
    // The worker's poll loop runs inside its own span. A consumer that reuses
    // the active context re-parents every job onto it, which is the Node-side
    // shape of the orphan-trace bug.
    const loop = rec.tracer.startSpan('worker loop');
    const loopCtx = api.trace.setSpan(api.context.active(), loop);

    await api.context.with(loopCtx, async () => {
      const ctx = consumeContext(parseMessage(JSON.stringify(goMessage())));
      expect(api.trace.getSpanContext(ctx)!.spanId).toBe(GO_PUBLISH_SPAN_ID);
    });
    loop.end();
  });
});

describe('withConsumerSpan', () => {
  it('parents the consumer span on the Go publish span', async () => {
    const message = parseMessage(JSON.stringify(goMessage()));

    await withConsumerSpan({ tracer: rec.tracer, message }, async (ctx) => {
      const child = rec.tracer.startSpan('child work', {}, ctx);
      child.end();
      // The context handed to the callback must also be the active one, or an
      // auto-instrumented library inside it would detach.
      expect(api.trace.getSpanContext(api.context.active())?.traceId).toBe(GO_TRACE_ID);
    });

    const process = rec.span('orders.fulfil process');
    const child = rec.span('child work');

    expect(process.spanContext().traceId).toBe(GO_TRACE_ID);
    expect(parentSpanId(process)).toBe(GO_PUBLISH_SPAN_ID);
    expect(process.parentSpanContext?.isRemote).toBe(true);
    expect(process.kind).toBe(SpanKind.CONSUMER);
    expect(attr(process, 'messaging.message.id')).toBe('msg-abc');
    expect(attr(process, 'messaging.destination.name')).toBe('orders.fulfil');

    expect(child.spanContext().traceId).toBe(GO_TRACE_ID);
    expect(parentSpanId(child)).toBe(process.spanContext().spanId);
  });

  it('starts a root span for a message with no trace context', async () => {
    const message = parseMessage(JSON.stringify({ id: 'm', topic: 'orders.fulfil', body: {} }));
    await withConsumerSpan({ tracer: rec.tracer, message }, async () => {});

    const process = rec.span('orders.fulfil process');
    expect(parentSpanId(process)).toBeUndefined();
  });

  it('ends the span even when the handler throws', async () => {
    const message = parseMessage(JSON.stringify(goMessage()));
    await expect(
      withConsumerSpan({ tracer: rec.tracer, message }, async (ctx) => {
        recordError(api.trace.getSpan(ctx)!, new Error('packing failed'));
        throw new Error('packing failed');
      }),
    ).rejects.toThrow('packing failed');

    // An unended span is never exported: if this lookup finds the span, the
    // finally block did its job.
    const process = rec.span('orders.fulfil process');
    expect(process.ended).toBe(true);
    expect(process.status.code).toBe(SpanStatusCode.ERROR);
    expect(process.events.map((e) => e.name)).toContain('exception');
  });

  it('carries the resource attributes the dashboards group by', async () => {
    const message = parseMessage(JSON.stringify(goMessage()));
    await withConsumerSpan({ tracer: rec.tracer, message }, async () => {});

    const resource = rec.span('orders.fulfil process').resource.attributes;
    expect(resource['service.name']).toBe('fulfillment');
    expect(resource['service.version']).toBe('1.2.3');
    expect(resource['deployment.environment.name']).toBe('test');
    expect(resource['service.instance.id']).toBe('fulfillment-test');
  });
});

describe('queueWaitMs', () => {
  const job = {
    order_id: 'o',
    sku: 's',
    quantity: 1,
    customer: '',
    enqueued_at: '2026-10-28T12:00:00.000Z',
  };

  it('measures from the producer timestamp', () => {
    expect(queueWaitMs(job, new Date('2026-10-28T12:00:01.500Z'))).toBe(1500);
  });

  it('clamps clock skew to zero instead of reporting a negative wait', () => {
    expect(queueWaitMs(job, new Date('2026-10-28T11:59:59.000Z'))).toBe(0);
  });

  it('returns undefined when the timestamp is missing or unparseable', () => {
    expect(queueWaitMs({ ...job, enqueued_at: '' })).toBeUndefined();
    expect(queueWaitMs({ ...job, enqueued_at: 'yesterday' })).toBeUndefined();
  });
});
