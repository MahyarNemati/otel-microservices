// Package obs owns every piece of OpenTelemetry wiring in this repository:
// the resource attributes, the trace and log pipelines, and the slog handler
// that stamps trace correlation onto human-readable logs.
//
// It exists as a single package on purpose. Resource attributes set in four
// different places drift, and dashboards that group by service.version then
// show four series for one deployment. Everything here derives from one
// [Config].
package obs

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Defaults used when the corresponding environment variable is unset.
const (
	DefaultEnvironment = "local"
	DefaultVersion     = "0.0.0-dev"
	DefaultOTLPURL     = "http://localhost:4318"
)

// Config is the complete set of knobs for a service's telemetry. It is built
// once, in main, and passed to [Setup].
type Config struct {
	// ServiceName becomes the service.name resource attribute. Required.
	ServiceName string
	// ServiceVersion becomes service.version. Dashboards group by it to tell
	// a deploy-shaped latency change from a traffic-shaped one.
	ServiceVersion string
	// Environment becomes deployment.environment.name.
	Environment string
	// ServiceInstanceID becomes service.instance.id and defaults to the
	// hostname, which under compose is the container id.
	ServiceInstanceID string

	// OTLPEndpoint is the base URL of the collector's OTLP/HTTP receiver,
	// for example http://otel-collector:4318. Signal paths (/v1/traces,
	// /v1/logs) are appended by the exporters.
	OTLPEndpoint string
	// ExportTimeout bounds a single export attempt.
	ExportTimeout time.Duration

	// SampleRatio is the head sampling ratio for traces started by this
	// service. Keep it at 1.0 and let the collector's tail sampler decide:
	// see deploy/collector/config.yaml for why.
	SampleRatio float64

	// LogLevel is one of debug, info, warn, error.
	LogLevel string
	// LogJSON writes the stdout log stream as JSON. Off gives a readable
	// text stream for local development; the trace ids are present either way.
	LogJSON bool
}

// ConfigFromEnv reads the standard OTel environment variables, falling back to
// the documented defaults. name is used when OTEL_SERVICE_NAME is unset so that
// a binary always has a usable service.name.
func ConfigFromEnv(name string) Config {
	cfg := Config{
		ServiceName:       envOr("OTEL_SERVICE_NAME", name),
		ServiceVersion:    envOr("SERVICE_VERSION", DefaultVersion),
		Environment:       envOr("DEPLOYMENT_ENVIRONMENT", DefaultEnvironment),
		ServiceInstanceID: envOr("SERVICE_INSTANCE_ID", hostname()),
		OTLPEndpoint:      envOr("OTEL_EXPORTER_OTLP_ENDPOINT", DefaultOTLPURL),
		ExportTimeout:     10 * time.Second,
		SampleRatio:       envFloat("OTEL_TRACES_SAMPLER_ARG", 1.0),
		LogLevel:          envOr("LOG_LEVEL", "info"),
		LogJSON:           envOr("LOG_FORMAT", "json") == "json",
	}
	return cfg
}

// Validate reports whether the configuration can produce usable telemetry.
func (c Config) Validate() error {
	if strings.TrimSpace(c.ServiceName) == "" {
		return fmt.Errorf("obs: ServiceName is required")
	}
	if c.SampleRatio < 0 || c.SampleRatio > 1 {
		return fmt.Errorf("obs: SampleRatio must be in [0,1], got %v", c.SampleRatio)
	}
	if c.ExportTimeout <= 0 {
		return fmt.Errorf("obs: ExportTimeout must be positive, got %v", c.ExportTimeout)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	var f float64
	if _, err := fmt.Sscanf(v, "%g", &f); err != nil {
		return fallback
	}
	return f
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown"
	}
	return h
}
