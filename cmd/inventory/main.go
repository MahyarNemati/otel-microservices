// Command inventory is the leaf service holding stock levels, and the service
// that fails on purpose so the traces are worth reading.
package main

import (
	"context"
	"log"

	"github.com/MahyarNemati/otel-microservices/internal/inventory"
	"github.com/MahyarNemati/otel-microservices/internal/service"
)

func main() {
	ctx := context.Background()

	rt, err := service.Setup(ctx, "inventory")
	if err != nil {
		log.Fatalf("inventory: telemetry setup: %v", err)
	}

	store := inventory.NewStore(rt.Providers.Tracer)
	svc := inventory.NewService(store, rt.Chaos, rt.Providers.Tracer)

	if err := rt.Serve(service.ServeOptions{
		Addr:   service.MustListen(service.Env("LISTEN_ADDR", ":8082")),
		Routes: svc.Routes(),
	}); err != nil {
		log.Fatalf("inventory: %v", err)
	}
}
