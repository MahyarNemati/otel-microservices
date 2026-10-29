// Command gateway is the public HTTP edge of the demo topology. It accepts a
// checkout and calls the orders service, starting the trace that every other
// service joins.
package main

import (
	"context"
	"log"
	"time"

	"github.com/MahyarNemati/otel-microservices/internal/gateway"
	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/service"
)

func main() {
	ctx := context.Background()

	rt, err := service.Setup(ctx, "gateway")
	if err != nil {
		log.Fatalf("gateway: telemetry setup: %v", err)
	}

	svc := gateway.NewService(
		gateway.Config{OrdersBaseURL: service.Env("ORDERS_URL", "http://orders:8081")},
		// No TracerProvider or Propagators: the client picks up the globals
		// obs.Setup installed. Tests pass them explicitly instead.
		httpx.NewClient(httpx.ClientOptions{Timeout: 10 * time.Second}),
		rt.Chaos,
	)

	if err := rt.Serve(service.ServeOptions{
		Addr:   service.MustListen(service.Env("LISTEN_ADDR", ":8080")),
		Routes: svc.Routes(),
	}); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}
