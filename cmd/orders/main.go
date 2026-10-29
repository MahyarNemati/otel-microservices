// Command orders is the middle service: it reserves stock synchronously and
// publishes a fulfilment job for the Node worker to pick up.
package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/orders"
	"github.com/MahyarNemati/otel-microservices/internal/queue"
	"github.com/MahyarNemati/otel-microservices/internal/service"
)

// queueDepthInterval is how often the order service republishes the depth
// gauge. The worker cannot publish it reliably: a worker keeping up reports
// zero, which is exactly when you most want to know the producer's view.
const queueDepthInterval = 5 * time.Second

func main() {
	ctx := context.Background()

	rt, err := service.Setup(ctx, "orders")
	if err != nil {
		log.Fatalf("orders: telemetry setup: %v", err)
	}

	backend := queue.NewRedis(queue.RedisOptions{
		Addr:   service.Env("REDIS_ADDR", "redis:6379"),
		Prefix: service.Env("QUEUE_PREFIX", "otelq"),
	})
	defer func() {
		if err := backend.Close(); err != nil {
			rt.Logger.Warn("closing redis", slog.Any("error", err))
		}
	}()

	svc := orders.NewService(
		orders.Config{InventoryBaseURL: service.Env("INVENTORY_URL", "http://inventory:8082")},
		orders.NewStore(rt.Providers.Tracer),
		queue.NewPublisher(backend, rt.Providers.Tracer, nil),
		httpx.NewClient(httpx.ClientOptions{Timeout: 10 * time.Second}),
		rt.Chaos,
		rt.Providers.Tracer,
		rt.Metrics,
		rt.Logger,
	)

	if err := rt.Serve(service.ServeOptions{
		Addr:   service.MustListen(service.Env("LISTEN_ADDR", ":8081")),
		Routes: svc.Routes(),
		Ready:  backend.Ping,
		Background: func(ctx context.Context) error {
			publishQueueDepth(ctx, rt, backend)
			return nil
		},
	}); err != nil {
		log.Fatalf("orders: %v", err)
	}
}

// publishQueueDepth keeps the queue_depth gauge current until shutdown.
func publishQueueDepth(ctx context.Context, rt *service.Runtime, backend *queue.Redis) {
	ticker := time.NewTicker(queueDepthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depth, err := backend.Depth(ctx, orders.TopicFulfil)
			if err != nil {
				// Deliberately not recorded on a span: this runs outside any
				// request, so there is no trace to attach it to, and inventing
				// a root span per tick would flood Tempo.
				rt.Logger.Warn("reading queue depth",
					slog.String("topic", orders.TopicFulfil), slog.Any("error", err))
				continue
			}
			rt.Metrics.SetQueueDepth(orders.TopicFulfil, depth)
		}
	}
}
