package obs_test

import (
	"context"
	"testing"

	"github.com/MahyarNemati/otel-microservices/internal/obs"
	"github.com/MahyarNemati/otel-microservices/internal/obstest"
)

// TestResourceAttributesReachExportedSpans is the assertion behind "the
// dashboards can group by them". A resource built but never attached to the
// provider is a silent failure: spans still export, and every Grafana panel
// that groups by service.version shows one unnamed series.
func TestResourceAttributesReachExportedSpans(t *testing.T) {
	t.Parallel()

	cfg := obs.Config{
		ServiceName:       "orders",
		ServiceVersion:    "1.4.2",
		Environment:       "staging",
		ServiceInstanceID: "orders-7c9f",
	}
	res, err := obs.NewResource(cfg)
	if err != nil {
		t.Fatalf("NewResource: %v", err)
	}

	rec := obstest.New(t, obstest.Options{Resource: res})
	_, span := rec.Tracer.Start(context.Background(), "work")
	span.End()

	got := rec.Span(t, "work")
	for key, want := range map[string]string{
		obs.AttrServiceName:           "orders",
		obs.AttrServiceVersion:        "1.4.2",
		obs.AttrDeploymentEnvironment: "staging",
		obs.AttrServiceInstanceID:     "orders-7c9f",
	} {
		if v := obstest.ResourceAttr(t, got, key); v != want {
			t.Errorf("resource %s = %q, want %q", key, v, want)
		}
	}

	// resource.Default() must survive the merge, or the SDK self-telemetry
	// dashboards lose their grouping.
	if v := obstest.ResourceAttr(t, got, "telemetry.sdk.language"); v != "go" {
		t.Errorf("telemetry.sdk.language = %q, want go", v)
	}
}

func TestResourceOmitsEmptyInstanceID(t *testing.T) {
	t.Parallel()

	res, err := obs.NewResource(obs.Config{ServiceName: "gateway", ServiceVersion: "0.0.1", Environment: "local"})
	if err != nil {
		t.Fatalf("NewResource: %v", err)
	}
	for _, kv := range res.Attributes() {
		if string(kv.Key) == obs.AttrServiceInstanceID {
			t.Fatalf("service.instance.id was set to %q despite an empty config value", kv.Value.Emit())
		}
	}
}

// TestPrometheusLabelsMatchResource keeps the metric labels and the resource
// attributes describing the same thing. Prometheus label names cannot contain
// dots, so the two sets are necessarily spelled differently; this test is what
// stops them from also meaning different things.
func TestPrometheusLabelsMatchResource(t *testing.T) {
	t.Parallel()

	cfg := obs.Config{ServiceName: "inventory", ServiceVersion: "2.0.0", Environment: "prod"}
	labels := obs.PrometheusLabels(cfg)

	for label, want := range map[string]string{
		"service_name":                "inventory",
		"service_version":             "2.0.0",
		"deployment_environment_name": "prod",
	} {
		if labels[label] != want {
			t.Errorf("label %s = %q, want %q", label, labels[label], want)
		}
	}
	if len(labels) != 3 {
		t.Errorf("PrometheusLabels returned %d labels, want 3: %v", len(labels), labels)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	base := obs.ConfigFromEnv("svc")
	if err := base.Validate(); err != nil {
		t.Fatalf("ConfigFromEnv produced an invalid config: %v", err)
	}
	if base.ServiceName != "svc" {
		t.Errorf("ServiceName = %q, want svc", base.ServiceName)
	}
	if base.Environment != obs.DefaultEnvironment {
		t.Errorf("Environment = %q, want %q", base.Environment, obs.DefaultEnvironment)
	}
	if base.SampleRatio != 1.0 {
		t.Errorf("SampleRatio = %v, want 1.0 (head sampling is the collector's job)", base.SampleRatio)
	}

	for name, mutate := range map[string]func(*obs.Config){
		"no service name":     func(c *obs.Config) { c.ServiceName = "  " },
		"ratio above one":     func(c *obs.Config) { c.SampleRatio = 1.5 },
		"negative ratio":      func(c *obs.Config) { c.SampleRatio = -0.1 },
		"zero export timeout": func(c *obs.Config) { c.ExportTimeout = 0 },
	} {
		cfg := base
		mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: Validate returned nil, want an error", name)
		}
	}
}

func TestConfigFromEnvReadsOverrides(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "from-env")
	t.Setenv("SERVICE_VERSION", "9.9.9")
	t.Setenv("DEPLOYMENT_ENVIRONMENT", "ci")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	t.Setenv("LOG_FORMAT", "text")

	cfg := obs.ConfigFromEnv("fallback")
	if cfg.ServiceName != "from-env" {
		t.Errorf("ServiceName = %q", cfg.ServiceName)
	}
	if cfg.ServiceVersion != "9.9.9" {
		t.Errorf("ServiceVersion = %q", cfg.ServiceVersion)
	}
	if cfg.Environment != "ci" {
		t.Errorf("Environment = %q", cfg.Environment)
	}
	if cfg.OTLPEndpoint != "http://collector:4318" {
		t.Errorf("OTLPEndpoint = %q", cfg.OTLPEndpoint)
	}
	if cfg.SampleRatio != 0.25 {
		t.Errorf("SampleRatio = %v, want 0.25", cfg.SampleRatio)
	}
	if cfg.LogJSON {
		t.Error("LogJSON is true despite LOG_FORMAT=text")
	}
}
