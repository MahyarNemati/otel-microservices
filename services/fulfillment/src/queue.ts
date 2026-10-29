/**
 * The consuming half of the async boundary, in a second runtime.
 *
 * The Go publisher in `internal/queue` put a `traceparent` into the message's
 * `headers` map. Nothing about that map is Go-specific — it is W3C trace
 * context, which is exactly the point of the standard — so the work this file
 * does is the mirror image:
 *
 *  1. `propagation.extract(ROOT_CONTEXT, message.headers)` turns the header
 *     back into a context whose span context is marked remote.
 *  2. The consumer span is started *with that context*, so its parent is the
 *     publish span in the Go process.
 *  3. Outgoing HTTP calls inject from the consumer span's context, so the
 *     trace continues back into Go.
 *
 * Extracting into `ROOT_CONTEXT` rather than `context.active()` is deliberate.
 * The poll loop runs inside the worker's own start-up span; extracting into the
 * active context there would work for the span key but leaves a trap for any
 * later refactor, and starting from the root makes the intent explicit: this
 * job's ancestry comes from the message, not from the process.
 */
import api, { ROOT_CONTEXT, type Context, SpanKind, type Tracer } from '@opentelemetry/api';

/** The on-the-wire message, matching `queue.Message` in Go. */
export interface QueueMessage {
  id: string;
  topic: string;
  headers: Record<string, string>;
  body: unknown;
}

/** The fulfilment job payload, matching `orders.FulfilJob` in Go. */
export interface FulfilJob {
  order_id: string;
  sku: string;
  quantity: number;
  customer: string;
  enqueued_at: string;
}

/** Thrown when a message cannot be parsed. */
export class MalformedMessageError extends Error {
  override name = 'MalformedMessageError';
}

/**
 * Parses a raw queue payload.
 *
 * It tolerates a missing `headers` map rather than throwing, because a message
 * with no trace context is still a job that has to be done — but
 * [[consumeContext]] then produces a root span, and the worker logs it as an
 * uncorrelated job so the gap is visible instead of silent.
 */
export function parseMessage(raw: string): QueueMessage {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new MalformedMessageError(`message is not valid JSON: ${(err as Error).message}`);
  }
  if (typeof parsed !== 'object' || parsed === null) {
    throw new MalformedMessageError(`message is not an object, got ${typeof parsed}`);
  }
  const obj = parsed as Record<string, unknown>;
  if (typeof obj['id'] !== 'string' || obj['id'] === '') {
    throw new MalformedMessageError('message has no id');
  }
  return {
    id: obj['id'],
    topic: typeof obj['topic'] === 'string' ? obj['topic'] : '',
    headers: isStringMap(obj['headers']) ? obj['headers'] : {},
    body: obj['body'] ?? null,
  };
}

/** Parses the job out of a message body. */
export function parseJob(message: QueueMessage): FulfilJob {
  const body = message.body;
  if (typeof body !== 'object' || body === null) {
    throw new MalformedMessageError(`message ${message.id} has no job body`);
  }
  const obj = body as Record<string, unknown>;
  const orderId = obj['order_id'];
  const sku = obj['sku'];
  if (typeof orderId !== 'string' || orderId === '') {
    throw new MalformedMessageError(`message ${message.id} has no order_id`);
  }
  if (typeof sku !== 'string' || sku === '') {
    throw new MalformedMessageError(`message ${message.id} has no sku`);
  }
  return {
    order_id: orderId,
    sku,
    quantity: typeof obj['quantity'] === 'number' ? obj['quantity'] : 1,
    customer: typeof obj['customer'] === 'string' ? obj['customer'] : '',
    enqueued_at: typeof obj['enqueued_at'] === 'string' ? obj['enqueued_at'] : '',
  };
}

/** Returns true when the message carries W3C trace context. */
export function hasTraceContext(message: QueueMessage): boolean {
  return typeof message.headers['traceparent'] === 'string' && message.headers['traceparent'] !== '';
}

/**
 * Builds the context a message should be handled under: the remote trace
 * context from its headers, extracted into the root.
 */
export function consumeContext(message: QueueMessage): Context {
  return api.propagation.extract(ROOT_CONTEXT, message.headers);
}

/** `messaging.system` value, matching `queue.System` in Go. */
export const MESSAGING_SYSTEM = 'otel-microservices.queue';

/** Options for [[withConsumerSpan]]. */
export interface ConsumerSpanOptions {
  tracer: Tracer;
  message: QueueMessage;
  /** Overrides the span name; defaults to "<topic> process". */
  name?: string;
}

/**
 * Runs `fn` inside a correctly parented consumer span.
 *
 * The span is ended in a `finally`, so a thrown handler still produces a closed
 * span. An unended span is never exported, which makes a crashing handler look
 * like a propagation failure in Tempo.
 */
export async function withConsumerSpan<T>(
  opts: ConsumerSpanOptions,
  fn: (ctx: Context) => Promise<T>,
): Promise<T> {
  const parent = consumeContext(opts.message);
  const topic = opts.message.topic || 'unknown';
  const span = opts.tracer.startSpan(
    opts.name ?? `${topic} process`,
    {
      kind: SpanKind.CONSUMER,
      attributes: {
        'messaging.system': MESSAGING_SYSTEM,
        'messaging.destination.name': topic,
        'messaging.message.id': opts.message.id,
        'messaging.operation.name': 'process',
      },
    },
    parent,
  );

  const ctx = api.trace.setSpan(parent, span);
  try {
    return await api.context.with(ctx, () => fn(ctx));
  } finally {
    span.end();
  }
}

/** Milliseconds a job spent queued, or undefined when the timestamp is absent. */
export function queueWaitMs(job: FulfilJob, now: Date = new Date()): number | undefined {
  if (!job.enqueued_at) {
    return undefined;
  }
  const enqueued = Date.parse(job.enqueued_at);
  if (Number.isNaN(enqueued)) {
    return undefined;
  }
  // Clamp at zero: the producer and the consumer are different processes and
  // may disagree about the clock by a few milliseconds, and a negative queue
  // wait in a histogram is worse than a zero.
  return Math.max(0, now.getTime() - enqueued);
}

function isStringMap(v: unknown): v is Record<string, string> {
  if (typeof v !== 'object' || v === null || Array.isArray(v)) {
    return false;
  }
  return Object.values(v as Record<string, unknown>).every((x) => typeof x === 'string');
}
