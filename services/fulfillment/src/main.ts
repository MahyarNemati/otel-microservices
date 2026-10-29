/**
 * The fulfilment worker's entrypoint: BLPOP from Redis, hand each message to
 * the worker, flush telemetry on the way out.
 */
import http from 'node:http';

import Redis from 'ioredis';

import { createLogger, parseLevel } from './logger.js';
import { MalformedMessageError, parseMessage } from './queue.js';
import { configFromEnv, setup } from './telemetry.js';
import { handleMessage } from './worker.js';

const TOPIC = process.env['QUEUE_TOPIC'] ?? 'orders.fulfil';
const PREFIX = process.env['QUEUE_PREFIX'] ?? 'otelq';
const KEY = `${PREFIX}:${TOPIC}`;
const BLPOP_TIMEOUT_SECONDS = 2;

async function main(): Promise<void> {
  const cfg = configFromEnv();
  const telemetry = setup(cfg);
  const logger = createLogger({
    level: parseLevel(process.env['LOG_LEVEL']),
    base: { 'service.name': cfg.serviceName, 'service.version': cfg.serviceVersion },
    loggerProvider: telemetry.loggerProvider,
    scope: cfg.serviceName,
  });

  const redis = new Redis(process.env['REDIS_URL'] ?? 'redis://redis:6379', {
    // The worker starts alongside Redis under compose; retrying beats a crash
    // loop that makes the logs unreadable.
    maxRetriesPerRequest: null,
    retryStrategy: (times) => Math.min(times * 200, 2000),
  });

  let running = true;
  const health = startHealthServer(() => running, Number(process.env['HEALTH_PORT'] ?? 8083));

  const stop = async (signal: string): Promise<void> => {
    logger.info('shutdown signal received', { signal });
    running = false;
    // Disconnect first so the in-flight BLPOP returns, then flush: flushing
    // before the loop has stopped races the exporter against new spans.
    redis.disconnect();
    health.close();
    await telemetry.shutdown();
    process.exit(0);
  };
  process.on('SIGINT', () => void stop('SIGINT'));
  process.on('SIGTERM', () => void stop('SIGTERM'));

  logger.info('worker starting', {
    topic: TOPIC,
    key: KEY,
    otlp_endpoint: cfg.otlpEndpoint,
    environment: cfg.environment,
  });

  const deps = {
    tracer: telemetry.tracer,
    logger,
    inventoryUrl: process.env['INVENTORY_URL'] ?? 'http://inventory:8082',
  };

  while (running) {
    let raw: [string, string] | null;
    try {
      raw = await redis.blpop(KEY, BLPOP_TIMEOUT_SECONDS);
    } catch (err) {
      if (!running) {
        break;
      }
      logger.error('reading from queue failed', { error: String(err) });
      await sleep(500);
      continue;
    }
    if (!raw) {
      continue;
    }

    try {
      const message = parseMessage(raw[1]);
      if (!message.topic) {
        message.topic = TOPIC;
      }
      await handleMessage(deps, message);
    } catch (err) {
      // Already recorded on the span by handleMessage; logged here so a
      // failure is visible without opening Grafana.
      const level = err instanceof MalformedMessageError ? 'warn' : 'error';
      logger.log(level, 'job failed', { error: String(err) });
    }
  }
}

/**
 * A minimal health endpoint. Compose's healthcheck needs something to poll,
 * and a worker with no HTTP surface otherwise has nothing to offer it.
 */
function startHealthServer(isRunning: () => boolean, port: number): http.Server {
  const server = http.createServer((req, res) => {
    if (req.url === '/healthz' || req.url === '/readyz') {
      res.writeHead(isRunning() ? 200 : 503, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ status: isRunning() ? 'ok' : 'stopping' }));
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(port);
  return server;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

await main();
