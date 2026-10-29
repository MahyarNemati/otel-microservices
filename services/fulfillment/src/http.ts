/**
 * The outbound HTTP hop, instrumented by hand.
 *
 * Auto-instrumentation would do this, but writing it out is the point: the
 * client span plus `propagation.inject` is the entire mechanism by which this
 * Node process hands the trace back to a Go service. Seeing it in nine lines is
 * more instructive than a registered instrumentation that does it invisibly.
 */
import api, { type Context, SpanKind, type Tracer } from '@opentelemetry/api';

import { recordError } from './telemetry.js';

/** Options for [[postJSON]]. */
export interface PostOptions {
  tracer: Tracer;
  url: string;
  body: unknown;
  ctx: Context;
  timeoutMs?: number;
  /** Injected for tests; defaults to the global fetch. */
  fetchImpl?: typeof fetch;
}

/** A non-2xx response from a downstream service. */
export class UpstreamError extends Error {
  override name = 'UpstreamError';

  constructor(
    readonly status: number,
    readonly url: string,
    message: string,
  ) {
    super(`${url} returned ${status}: ${message}`);
  }
}

/**
 * POSTs `body` to `url` inside a client span, injecting trace context.
 */
export async function postJSON<T>(opts: PostOptions): Promise<T> {
  const parsed = new URL(opts.url);
  const span = opts.tracer.startSpan(
    `POST ${parsed.pathname}`,
    {
      kind: SpanKind.CLIENT,
      attributes: {
        'http.request.method': 'POST',
        'url.full': opts.url,
        'server.address': parsed.hostname,
        'server.port': Number(parsed.port) || (parsed.protocol === 'https:' ? 443 : 80),
      },
    },
    opts.ctx,
  );

  const ctx = api.trace.setSpan(opts.ctx, span);
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  // This one line is what keeps the trace alive across the runtime boundary.
  api.propagation.inject(ctx, headers);

  const fetchImpl = opts.fetchImpl ?? fetch;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), opts.timeoutMs ?? 10_000);

  try {
    const response = await fetchImpl(opts.url, {
      method: 'POST',
      headers,
      body: JSON.stringify(opts.body),
      signal: controller.signal,
    });

    span.setAttribute('http.response.status_code', response.status);
    if (!response.ok) {
      const text = await response.text().catch(() => '');
      // A 4xx is the caller's fault and must not raise this service's error
      // rate; a 5xx is a real failure of the call.
      const kind = response.status >= 400 && response.status < 500 ? 'client' : 'server';
      throw recordError(span, new UpstreamError(response.status, opts.url, text || response.statusText), kind);
    }
    return (await response.json()) as T;
  } catch (err) {
    if (!(err instanceof UpstreamError)) {
      recordError(span, err, 'server');
    }
    throw err;
  } finally {
    clearTimeout(timeout);
    span.end();
  }
}
