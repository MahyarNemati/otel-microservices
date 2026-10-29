// Package service holds the start-up sequence every Go binary in this
// repository shares: read configuration, build the telemetry pipelines, serve
// HTTP, shut down cleanly.
//
// The shutdown half matters more than it looks. A process that exits without
// flushing drops whatever is still in the batch span processor's queue, which
// shows up in Tempo as a trace that is missing its last service — the same
// symptom as a propagation bug, with a completely different cause.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MahyarNemati/otel-microservices/internal/chaos"
	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/metrics"
	"github.com/MahyarNemati/otel-microservices/internal/obs"
)

// ShutdownGrace bounds the flush-and-stop sequence.
const ShutdownGrace = 10 * time.Second

// Runtime is everything a service needs, built once.
type Runtime struct {
	Config    obs.Config
	Chaos     *chaos.Injector
	Providers *obs.Providers
	Metrics   *metrics.Registry
	Logger    *slog.Logger
}

// Setup builds the runtime for a service named name.
func Setup(ctx context.Context, name string) (*Runtime, error) {
	cfg := obs.ConfigFromEnv(name)
	providers, err := obs.Setup(ctx, cfg)
	if err != nil {
		return nil, err
	}
	chaosCfg := chaos.ConfigFromEnv()
	rt := &Runtime{
		Config:    cfg,
		Chaos:     chaos.New(chaosCfg),
		Providers: providers,
		Metrics:   metrics.New(cfg),
		Logger:    providers.Logger,
	}
	rt.Logger.InfoContext(ctx, "telemetry ready",
		slog.String("otlp_endpoint", cfg.OTLPEndpoint),
		slog.String("environment", cfg.Environment),
		slog.Float64("head_sample_ratio", cfg.SampleRatio),
		slog.String("chaos", chaosCfg.String()),
	)
	return rt, nil
}

// Shutdown flushes telemetry.
func (r *Runtime) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownGrace)
	defer cancel()
	return r.Providers.Shutdown(ctx)
}

// HealthHandler reports liveness. It is mounted outside the tracing middleware:
// a container probe every two seconds would otherwise be the overwhelming
// majority of spans in Tempo.
func HealthHandler(ready func(context.Context) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ready != nil {
			if err := ready(r.Context()); err != nil {
				httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "error": err.Error()})
				return
			}
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// ServeOptions configures [Serve].
type ServeOptions struct {
	// Addr is the listen address, e.g. ":8080".
	Addr string
	// Routes are the instrumented application routes.
	Routes []httpx.Route
	// Ready, when non-nil, gates /readyz.
	Ready func(context.Context) error
	// Background, when non-nil, runs alongside the server and is cancelled on
	// shutdown. The worker uses it for its consume loop.
	Background func(context.Context) error
}

// Serve runs the HTTP server until SIGINT or SIGTERM, then drains it and
// flushes telemetry.
func (r *Runtime) Serve(opts ServeOptions) error {
	plain := map[string]http.Handler{
		"/healthz": HealthHandler(nil),
		"/readyz":  HealthHandler(opts.Ready),
		"/metrics": r.Metrics.Handler(),
	}

	handler := httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName: r.Config.ServiceName,
		Metrics:     r.Metrics,
		Routes:      opts.Routes,
		Plain:       plain,
	})

	srv := &http.Server{
		Addr:              opts.Addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bgDone := make(chan error, 1)
	if opts.Background != nil {
		go func() { bgDone <- opts.Background(ctx) }()
	}

	serveErr := make(chan error, 1)
	go func() {
		r.Logger.InfoContext(ctx, "listening",
			slog.String("addr", opts.Addr),
			slog.String("service", r.Config.ServiceName),
			slog.String("version", r.Config.ServiceVersion),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	var runErr error
	select {
	case <-ctx.Done():
		r.Logger.Info("shutdown signal received")
	case err := <-serveErr:
		runErr = err
	case err := <-bgDone:
		if err != nil {
			runErr = fmt.Errorf("background worker: %w", err)
		}
	}
	stop()

	drainCtx, cancel := context.WithTimeout(context.Background(), ShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("http shutdown: %w", err))
	}
	if err := r.Shutdown(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("telemetry shutdown: %w", err))
	}
	return runErr
}

// Env returns the value of key, or fallback.
func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// MustListen checks the address is parseable before start-up, so a typo fails
// with a clear message rather than a bind error ten lines later.
func MustListen(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		panic(fmt.Sprintf("service: invalid listen address %q: %v", addr, err))
	}
	return addr
}
