package obs

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Resource attribute keys that the Grafana dashboards group by. They are named
// here rather than inline so a dashboard change and a code change refer to the
// same string.
const (
	AttrServiceName       = string(semconv.ServiceNameKey)
	AttrServiceVersion    = string(semconv.ServiceVersionKey)
	AttrServiceInstanceID = string(semconv.ServiceInstanceIDKey)

	// AttrDeploymentEnvironment is deployment.environment.name. The older
	// deployment.environment was deprecated in semconv 1.27; Loki and Tempo
	// both key off whatever is sent, so emitting the current name and
	// documenting it avoids a dashboard that silently matches nothing.
	AttrDeploymentEnvironment = "deployment.environment.name"
)

// NewResource builds the resource every signal from this process carries.
//
// This is the only place resource attributes are constructed. Traces, logs and
// the Prometheus target labels all derive from it, which is what makes
// "group by service.version" work across all three signals in Grafana.
func NewResource(cfg Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		attribute.String(AttrDeploymentEnvironment, cfg.Environment),
	}
	if cfg.ServiceInstanceID != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(cfg.ServiceInstanceID))
	}

	// resource.Default() contributes telemetry.sdk.* and the schema URL; merging
	// onto it rather than replacing it keeps backend-side SDK dashboards working.
	//
	// The semconv version imported here must match the one resource.Default()
	// was built against, or Merge fails with "conflicting Schema URL" and the
	// service starts with no resource at all. Keep the import in step with the
	// otel SDK version in go.mod.
	return resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, attrs...))
}

// PrometheusLabels returns the resource attributes that the Prometheus metrics
// of this process are labelled with, so a metric and a trace can be joined on
// the same dimensions. Prometheus label names cannot contain dots.
func PrometheusLabels(cfg Config) map[string]string {
	return map[string]string{
		"service_name":                cfg.ServiceName,
		"service_version":             cfg.ServiceVersion,
		"deployment_environment_name": cfg.Environment,
	}
}
