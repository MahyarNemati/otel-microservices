/**
 * The trace-correlated logger.
 *
 * Every line this writes carries `trace_id` and `span_id` when there is an
 * active span, in the same keys `internal/obs/slog.go` uses, so one Grafana
 * datasource configuration links traces to logs for both runtimes.
 *
 * Lines logged outside a span deliberately carry no ids. An all-zero trace id
 * is worse than none: it groups every unrelated start-up line into one
 * enormous fake trace.
 */
import api, { type Context, SeverityNumber } from '@opentelemetry/api';
import { type LoggerProvider } from '@opentelemetry/sdk-logs';

import { traceContext } from './telemetry.js';

/** Log levels, lowest first. */
export const LEVELS = ['debug', 'info', 'warn', 'error'] as const;

/** One of the supported log levels. */
export type Level = (typeof LEVELS)[number];

const SEVERITY: Record<Level, SeverityNumber> = {
  debug: SeverityNumber.DEBUG,
  info: SeverityNumber.INFO,
  warn: SeverityNumber.WARN,
  error: SeverityNumber.ERROR,
};

/** Structured log fields. */
export type Fields = Record<string, unknown>;

/** Where a log line is written. Injected so tests can capture output. */
export interface Sink {
  write: (line: string) => void;
}

/** Options for [[createLogger]]. */
export interface LoggerOptions {
  level?: Level;
  /** Fields merged into every line, e.g. service.name. */
  base?: Fields;
  /** Defaults to process.stdout. */
  sink?: Sink;
  /** When set, lines are also emitted as OTLP log records for Loki. */
  loggerProvider?: LoggerProvider;
  /** Scope name for the OTLP logger. */
  scope?: string;
  /** Clock, injected for deterministic tests. */
  now?: () => Date;
}

/** A trace-correlated structured logger. */
export interface Logger {
  debug: (msg: string, fields?: Fields) => void;
  info: (msg: string, fields?: Fields) => void;
  warn: (msg: string, fields?: Fields) => void;
  error: (msg: string, fields?: Fields) => void;
  log: (level: Level, msg: string, fields?: Fields, ctx?: Context) => void;
}

/** Builds a logger. */
export function createLogger(options: LoggerOptions = {}): Logger {
  const level = options.level ?? 'info';
  const threshold = LEVELS.indexOf(level);
  const base = options.base ?? {};
  const sink: Sink = options.sink ?? { write: (line) => process.stdout.write(`${line}\n`) };
  const now = options.now ?? (() => new Date());
  const otelLogger = options.loggerProvider?.getLogger(options.scope ?? 'fulfillment');

  function log(lvl: Level, msg: string, fields: Fields = {}, ctx?: Context): void {
    if (LEVELS.indexOf(lvl) < threshold) {
      return;
    }
    const active = ctx ?? api.context.active();
    const correlation = traceContext(active);

    sink.write(
      JSON.stringify({
        time: now().toISOString(),
        level: lvl,
        msg,
        ...base,
        ...fields,
        ...(correlation ?? {}),
      }),
    );

    // The OTLP record carries trace context natively: emit() reads it from the
    // context it is given, which is why `context` is passed explicitly rather
    // than relying on whatever is active inside the SDK.
    otelLogger?.emit({
      severityNumber: SEVERITY[lvl],
      severityText: lvl.toUpperCase(),
      body: msg,
      attributes: { ...base, ...fields } as Record<string, never>,
      context: active,
    });
  }

  return {
    log,
    debug: (msg, fields) => log('debug', msg, fields),
    info: (msg, fields) => log('info', msg, fields),
    warn: (msg, fields) => log('warn', msg, fields),
    error: (msg, fields) => log('error', msg, fields),
  };
}

/** Parses a level name, defaulting to info. */
export function parseLevel(raw: string | undefined): Level {
  const candidate = raw?.trim().toLowerCase();
  if (candidate === 'warning') {
    return 'warn';
  }
  return (LEVELS as readonly string[]).includes(candidate ?? '') ? (candidate as Level) : 'info';
}
