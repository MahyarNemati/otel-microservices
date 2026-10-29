/**
 * The fulfilment worker's business logic, separated from its transport so the
 * tests can drive it with an in-memory queue and an in-memory span exporter.
 */
import api, { type Context, type Tracer } from '@opentelemetry/api';

import { postJSON } from './http.js';
import { type Logger } from './logger.js';
import {
  type FulfilJob,
  type QueueMessage,
  hasTraceContext,
  parseJob,
  queueWaitMs,
  withConsumerSpan,
} from './queue.js';
import { recordError } from './telemetry.js';

/** A SKU prefix that makes the worker fail, mirroring the inventory service. */
export const PREFIX_WORKER_FAILURE = 'FAIL-';

/** Configuration for [[handleMessage]]. */
export interface WorkerDeps {
  tracer: Tracer;
  logger: Logger;
  /** Base URL of the inventory service, for the commit call back into Go. */
  inventoryUrl: string;
  /** Injected for tests. */
  fetchImpl?: typeof fetch;
  /** Injected for tests. */
  now?: () => Date;
}

/** The outcome of processing one message. */
export interface Outcome {
  orderId: string;
  committed: boolean;
  queueWaitMs: number | undefined;
}

/**
 * Processes one queue message inside a consumer span parented on the Go
 * publisher's span, then calls back into the Go inventory service so the trace
 * crosses the runtime boundary twice.
 */
export async function handleMessage(deps: WorkerDeps, message: QueueMessage): Promise<Outcome> {
  const now = deps.now ?? (() => new Date());

  return withConsumerSpan({ tracer: deps.tracer, message }, async (ctx) => {
    const span = api.trace.getSpan(ctx)!;

    if (!hasTraceContext(message)) {
      // Visible rather than silent: a job with no traceparent produces a root
      // span, and this attribute is how a reader tells that apart from a
      // genuinely new trace.
      span.setAttribute('messaging.orphaned', true);
      deps.logger.warn('job arrived with no trace context', { message_id: message.id });
    }

    let job: FulfilJob;
    try {
      job = parseJob(message);
    } catch (err) {
      // Malformed input is the producer's fault, not ours.
      throw recordError(span, err, 'client');
    }

    const waited = queueWaitMs(job, now());
    span.setAttributes({
      'order.id': job.order_id,
      'order.sku': job.sku,
      'order.quantity': job.quantity,
      ...(waited === undefined ? {} : { 'messaging.queue.wait_ms': waited }),
    });

    deps.logger.info('fulfilling order', {
      order_id: job.order_id,
      sku: job.sku,
      quantity: job.quantity,
      ...(waited === undefined ? {} : { queue_wait_ms: waited }),
    });

    if (job.sku.startsWith(PREFIX_WORKER_FAILURE)) {
      // The deliberate worker-side error path. Thrown from inside the consumer
      // span so the exception, its stack and the Error status all land on the
      // span that is the child of the Go publisher.
      throw recordError(span, new Error(`packing failed for ${job.sku}`), 'server');
    }

    await pickAndPack(deps.tracer, ctx, job);

    const commit = await postJSON<{ order_id: string; committed: boolean }>({
      tracer: deps.tracer,
      url: `${deps.inventoryUrl}/commit`,
      body: { order_id: job.order_id, sku: job.sku },
      ctx,
      ...(deps.fetchImpl ? { fetchImpl: deps.fetchImpl } : {}),
    });

    deps.logger.info('order fulfilled', { order_id: job.order_id, committed: commit.committed });
    return { orderId: job.order_id, committed: commit.committed, queueWaitMs: waited };
  });
}

/**
 * A simulated warehouse step, so the waterfall has a segment between the queue
 * hop and the commit call rather than an unexplained gap.
 */
async function pickAndPack(tracer: Tracer, ctx: Context, job: FulfilJob): Promise<void> {
  const span = tracer.startSpan('fulfillment.pick_and_pack', { attributes: { 'order.sku': job.sku } }, ctx);
  try {
    await api.context.with(api.trace.setSpan(ctx, span), async () => {
      // Scale with quantity so a large order is visibly slower in the trace.
      await new Promise((resolve) => setTimeout(resolve, Math.min(5 * job.quantity, 120)));
    });
  } finally {
    span.end();
  }
}
