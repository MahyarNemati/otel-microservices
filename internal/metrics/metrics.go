// Package metrics exposes the RED metrics for a service, with exemplars.
//
// An exemplar is a trace id attached to a single observation in a histogram
// bucket. It is what turns "p99 latency is 3 seconds" into "here is a trace of
// one of those 3-second requests", which is the only question anyone actually
// has when looking at a latency panel. Without it the path from metric to trace
// is: eyeball the time range, guess a service, search Tempo by duration, hope.
//
// Three things have to line up for an exemplar to survive, and all three are
// easy to miss:
//
//   - The observation must be made through [prometheus.ExemplarObserver], not
//     plain Observe. ObserveWithExemplar is a different method; Observe simply
//     discards the trace id with no error.
//   - The /metrics endpoint must negotiate OpenMetrics. Exemplars have no
//     representation in the original Prometheus text format, so promhttp drops
//     them silently unless EnableOpenMetrics is set.
//   - Prometheus must be started with --enable-feature=exemplar-storage, or it
//     scrapes them and throws them away. See deploy/docker-compose.yml.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/obs"
)

// ExemplarTraceIDLabel is the label name Grafana looks for when turning an
// exemplar into a link. Prometheus requires it to be exactly this.
const ExemplarTraceIDLabel = "trace_id"

// Registry holds a service's collectors and the handler that exposes them.
type Registry struct {
	// Requests counts served requests by route, method and status class.
	Requests *prometheus.CounterVec
	// Duration is the request latency histogram that carries exemplars.
	Duration *prometheus.HistogramVec
	// QueueDepth is the gauge the worker and the order service publish.
	QueueDepth *prometheus.GaugeVec
	// QueueWait records how long a job sat on the queue before being picked up,
	// measured from the producer's enqueue timestamp.
	QueueWait *prometheus.HistogramVec

	reg *prometheus.Registry
}

// New builds a registry whose collectors are pre-labelled with the service's
// resource identity, so a Prometheus query can group by the same dimensions a
// Tempo query does.
func New(cfg obs.Config) *Registry {
	reg := prometheus.NewRegistry()
	constLabels := prometheus.Labels(obs.PrometheusLabels(cfg))

	r := &Registry{
		reg: reg,
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "http_requests_total",
			Help:        "Requests served, by route, method and status code.",
			ConstLabels: constLabels,
		}, []string{"route", "method", "status"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "http_request_duration_seconds",
			Help:        "Request latency in seconds. Carries a trace_id exemplar per observation.",
			ConstLabels: constLabels,
			// Explicit buckets rather than DefBuckets: the chaos script injects
			// latency up to a couple of seconds, and DefBuckets tops out at 10s
			// with nothing between 1s and 2.5s, which is exactly where the
			// interesting traces land.
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		}, []string{"route", "method", "status"}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "queue_depth",
			Help:        "Messages waiting on a queue topic.",
			ConstLabels: constLabels,
		}, []string{"topic"}),
		QueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "queue_wait_seconds",
			Help:        "Time a job spent queued before processing started. Carries a trace_id exemplar.",
			ConstLabels: constLabels,
			Buckets:     []float64{0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 15},
		}, []string{"topic"}),
	}

	reg.MustRegister(
		r.Requests, r.Duration, r.QueueDepth, r.QueueWait,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return r
}

// Registerer exposes the underlying registry for tests and for extra
// collectors.
func (r *Registry) Registerer() *prometheus.Registry { return r.reg }

// Handler returns the /metrics handler.
//
// EnableOpenMetrics is the line that makes exemplars visible to Prometheus. It
// is off by default, which is why so many services expose exemplars that
// nothing ever reads.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		Registry:          r.reg,
	})
}

// ObserveRequest records one served request, attaching the active trace as an
// exemplar on the latency histogram.
func (r *Registry) ObserveRequest(span trace.SpanContext, route, method string, status int, d time.Duration) {
	labels := prometheus.Labels{
		"route":  route,
		"method": method,
		"status": strconv.Itoa(status),
	}
	r.Requests.With(labels).Inc()
	observeWithExemplar(r.Duration.With(labels), d.Seconds(), span)
}

// ObserveQueueWait records how long a job waited, with an exemplar.
func (r *Registry) ObserveQueueWait(span trace.SpanContext, topic string, d time.Duration) {
	observeWithExemplar(r.QueueWait.With(prometheus.Labels{"topic": topic}), d.Seconds(), span)
}

// SetQueueDepth publishes the current depth of a topic.
func (r *Registry) SetQueueDepth(topic string, depth int64) {
	r.QueueDepth.With(prometheus.Labels{"topic": topic}).Set(float64(depth))
}

// observeWithExemplar attaches the trace id when there is a sampled trace to
// point at, and falls back to a plain Observe otherwise.
//
// The sampled check matters: an exemplar naming a trace the backend dropped is
// a link to a 404, which is worse than no link because it looks like data loss.
func observeWithExemplar(o prometheus.Observer, v float64, sc trace.SpanContext) {
	eo, ok := o.(prometheus.ExemplarObserver)
	if !ok || !sc.IsValid() || !sc.IsSampled() {
		o.Observe(v)
		return
	}
	eo.ObserveWithExemplar(v, prometheus.Labels{ExemplarTraceIDLabel: sc.TraceID().String()})
}
