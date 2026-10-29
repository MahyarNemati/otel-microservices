import api, { SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';

import { createLogger, type Sink } from './logger.js';
import { parseMessage } from './queue.js';
import { attr, newRecorder, parentSpanId, traceparent } from './testing.js';
import { PREFIX_WORKER_FAILURE, handleMessage } from './worker.js';

const rec = newRecorder();

const GO_TRACE_ID = 'a1b2c3d4e5f60718293a4b5c6d7e8f90';
const GO_PUBLISH_SPAN_ID = '1122334455667788';

beforeEach(() => rec.reset());
afterAll(() => rec.shutdown());

interface Captured {
  lines: string[];
  sink: Sink;
}

function capture(): Captured {
  const lines: string[] = [];
  return { lines, sink: { write: (line) => lines.push(line) } };
}

/** A fetch double that records the headers it was given. */
function fakeFetch(status = 200, body: unknown = { order_id: 'ord-1', committed: true }) {
  const calls: Array<{ url: string; headers: Record<string, string> }> = [];
  const impl = (async (url: string | URL | Request, init?: RequestInit) => {
    calls.push({
      url: String(url),
      headers: (init?.headers ?? {}) as Record<string, string>,
    });
    return new Response(JSON.stringify(body), {
      status,
      headers: { 'content-type': 'application/json' },
    });
  }) as unknown as typeof fetch;
  return { impl, calls };
}

function message(overrides: Record<string, unknown> = {}, bodyOverrides: Record<string, unknown> = {}) {
  return parseMessage(
    JSON.stringify({
      id: 'msg-1',
      topic: 'orders.fulfil',
      headers: { traceparent: traceparent(GO_TRACE_ID, GO_PUBLISH_SPAN_ID) },
      body: {
        order_id: 'ord-1',
        sku: 'SKU-TEA',
        quantity: 2,
        customer: 'ada',
        enqueued_at: '2026-10-28T12:00:00.000Z',
        ...bodyOverrides,
      },
      ...overrides,
    }),
  );
}

describe('handleMessage', () => {
  it('continues the Go trace and hands it back to Go', async () => {
    const { impl, calls } = fakeFetch();
    const captured = capture();
    const logger = createLogger({ sink: captured.sink, base: { 'service.name': 'fulfillment' } });

    const outcome = await handleMessage(
      { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
      message(),
    );

    expect(outcome).toMatchObject({ orderId: 'ord-1', committed: true });

    // The three spans this worker produced, all in the trace the Go gateway
    // started.
    const process = rec.span('orders.fulfil process');
    const pack = rec.span('fulfillment.pick_and_pack');
    const commit = rec.span('POST /commit');

    for (const span of [process, pack, commit]) {
      expect(span.spanContext().traceId).toBe(GO_TRACE_ID);
    }
    expect(parentSpanId(process)).toBe(GO_PUBLISH_SPAN_ID);
    expect(parentSpanId(pack)).toBe(process.spanContext().spanId);
    expect(parentSpanId(commit)).toBe(process.spanContext().spanId);
    expect(commit.kind).toBe(SpanKind.CLIENT);

    // And the header that carries it back into the Go inventory service must
    // name the client span, in the trace the whole request belongs to.
    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toBe('http://inventory:8082/commit');
    expect(calls[0]!.headers['traceparent']).toBe(
      traceparent(GO_TRACE_ID, commit.spanContext().spanId),
    );

    expect(attr(process, 'order.id')).toBe('ord-1');
    expect(attr(process, 'messaging.queue.wait_ms')).toBeDefined();
    expect(attr(process, 'messaging.orphaned')).toBeUndefined();
  });

  it('correlates every log line it wrote with the Go trace', async () => {
    const { impl } = fakeFetch();
    const captured = capture();
    const logger = createLogger({ sink: captured.sink, base: { 'service.name': 'fulfillment' } });

    await handleMessage(
      { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
      message(),
    );

    const process = rec.span('orders.fulfil process');
    expect(captured.lines.length).toBeGreaterThanOrEqual(2);

    for (const raw of captured.lines) {
      const line = JSON.parse(raw);
      expect(line.trace_id).toBe(GO_TRACE_ID);
      expect(line.span_id).toBe(process.spanContext().spanId);
      expect(line.trace_flags).toBe('01');
      expect(line['service.name']).toBe('fulfillment');
    }
    // And the payload survived the correlation.
    const first = JSON.parse(captured.lines[0]!);
    expect(first.order_id).toBe('ord-1');
    expect(first.msg).toBe('fulfilling order');
  });

  it('records a worker-side failure as an exception with a stack trace', async () => {
    const { impl, calls } = fakeFetch();
    const captured = capture();
    const logger = createLogger({ sink: captured.sink });

    await expect(
      handleMessage(
        { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
        message({}, { sku: `${PREFIX_WORKER_FAILURE}CRATE` }),
      ),
    ).rejects.toThrow(/packing failed/);

    const process = rec.span('orders.fulfil process');
    expect(process.status.code).toBe(SpanStatusCode.ERROR);
    expect(attr(process, 'error.kind')).toBe('server');

    const exception = process.events.find((e) => e.name === 'exception');
    expect(exception).toBeDefined();
    expect(exception!.attributes?.['exception.message']).toMatch(/packing failed/);
    expect(String(exception!.attributes?.['exception.stacktrace'])).toContain('worker.ts');

    // The failing job must not have reached the commit call.
    expect(calls).toHaveLength(0);
    // The span is still a child of the Go publish span: an error trace that
    // loses its ancestry is the one you most need to be able to walk.
    expect(parentSpanId(process)).toBe(GO_PUBLISH_SPAN_ID);
  });

  it('treats a malformed job as the producer fault, not its own', async () => {
    const { impl } = fakeFetch();
    const logger = createLogger({ sink: capture().sink });

    await expect(
      handleMessage(
        { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
        parseMessage(
          JSON.stringify({
            id: 'msg-2',
            topic: 'orders.fulfil',
            headers: { traceparent: traceparent(GO_TRACE_ID, GO_PUBLISH_SPAN_ID) },
            body: { sku: 'SKU-TEA' },
          }),
        ),
      ),
    ).rejects.toThrow(/order_id/);

    const process = rec.span('orders.fulfil process');
    expect(process.status.code).toBe(SpanStatusCode.UNSET);
    expect(attr(process, 'error.kind')).toBe('client');
    expect(process.events.map((e) => e.name)).toContain('exception');
  });

  it('surfaces an upstream 5xx as a server fault on the client span', async () => {
    const { impl } = fakeFetch(503, { error: 'inventory unavailable' });
    const logger = createLogger({ sink: capture().sink });

    await expect(
      handleMessage(
        { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
        message(),
      ),
    ).rejects.toThrow(/returned 503/);

    const commit = rec.span('POST /commit');
    expect(commit.status.code).toBe(SpanStatusCode.ERROR);
    expect(attr(commit, 'http.response.status_code')).toBe('503');
    expect(attr(commit, 'error.kind')).toBe('server');
  });

  it('leaves an upstream 4xx as a client fault', async () => {
    const { impl } = fakeFetch(409, { error: 'already committed' });
    const logger = createLogger({ sink: capture().sink });

    await expect(
      handleMessage(
        { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
        message(),
      ),
    ).rejects.toThrow(/returned 409/);

    const commit = rec.span('POST /commit');
    expect(commit.status.code).toBe(SpanStatusCode.UNSET);
    expect(attr(commit, 'error.kind')).toBe('client');
  });

  it('flags an orphaned job instead of pretending it is a new trace', async () => {
    const { impl } = fakeFetch();
    const captured = capture();
    const logger = createLogger({ sink: captured.sink });

    await handleMessage(
      { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
      parseMessage(
        JSON.stringify({
          id: 'msg-3',
          topic: 'orders.fulfil',
          body: { order_id: 'ord-9', sku: 'SKU-TEA', quantity: 1 },
        }),
      ),
    );

    const process = rec.span('orders.fulfil process');
    expect(parentSpanId(process)).toBeUndefined();
    expect(attr(process, 'messaging.orphaned')).toBe('true');
    expect(captured.lines.some((l) => JSON.parse(l).msg === 'job arrived with no trace context')).toBe(true);
  });

  it('does not re-sample a parent that was dropped upstream', async () => {
    // traceparent with flags 00: the Go collector already decided not to keep
    // this trace. ParentBasedSampler must honour that, or Tempo receives a
    // trace that starts in the middle.
    const { impl } = fakeFetch();
    const logger = createLogger({ sink: capture().sink });

    await handleMessage(
      { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
      parseMessage(
        JSON.stringify({
          id: 'msg-4',
          topic: 'orders.fulfil',
          headers: { traceparent: traceparent(GO_TRACE_ID, GO_PUBLISH_SPAN_ID, false) },
          body: { order_id: 'ord-4', sku: 'SKU-TEA', quantity: 1 },
        }),
      ),
    );

    expect(rec.spans()).toHaveLength(0);
  });
});

describe('the active context inside the handler', () => {
  it('survives awaits, so async children stay attached', async () => {
    const { impl } = fakeFetch();
    const logger = createLogger({ sink: capture().sink });

    await handleMessage(
      { tracer: rec.tracer, logger, inventoryUrl: 'http://inventory:8082', fetchImpl: impl },
      message(),
    );

    // pick_and_pack awaits a timer before its span ends; its child relationship
    // is the proof that AsyncLocalStorage context propagation is installed. The
    // commit span is created after that await returns.
    const process = rec.span('orders.fulfil process');
    expect(parentSpanId(rec.span('POST /commit'))).toBe(process.spanContext().spanId);
    expect(api.trace.getSpanContext(api.context.active())).toBeUndefined();
  });
});
