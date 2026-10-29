// Package chaos injects latency and failures so that the traces in Tempo show
// something worth looking at.
//
// A stack where every request succeeds in 4ms demonstrates nothing: the
// waterfall is a flat bar, the latency histogram has one bucket, and the tail
// sampler never sees an error to keep. The injectors here are what make the
// dashboards legible, and each one is recorded on the span that it affected so
// a reader can tell injected pain from real pain.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Attributes recorded on a span the injector touched. They are prefixed
// `chaos.` so a Tempo query can exclude injected failures when measuring the
// real thing.
const (
	AttrInjectedLatency = "chaos.injected_latency_ms"
	AttrInjectedFault   = "chaos.injected_fault"
)

// ErrInjected is the failure the fault injector returns.
var ErrInjected = errors.New("chaos: injected fault")

// Config is read from the environment so the chaos script can turn the dials
// on a running stack without a rebuild.
type Config struct {
	// LatencyProbability is the chance in [0,1] that a request gets extra delay.
	LatencyProbability float64
	// LatencyMin and LatencyMax bound the injected delay.
	LatencyMin time.Duration
	LatencyMax time.Duration
	// FaultProbability is the chance in [0,1] that a request fails outright.
	FaultProbability float64
}

// Enabled reports whether any injector would do anything.
func (c Config) Enabled() bool {
	return c.LatencyProbability > 0 || c.FaultProbability > 0
}

// String renders the configuration for a startup log line, so that a confusing
// dashboard can be traced back to chaos being left on.
func (c Config) String() string {
	if !c.Enabled() {
		return "disabled"
	}
	return fmt.Sprintf("latency %.0f%% in [%s,%s], faults %.0f%%",
		c.LatencyProbability*100, c.LatencyMin, c.LatencyMax, c.FaultProbability*100)
}

// ConfigFromEnv reads CHAOS_LATENCY_PCT, CHAOS_LATENCY_MIN_MS,
// CHAOS_LATENCY_MAX_MS and CHAOS_FAULT_PCT. Everything defaults to off.
func ConfigFromEnv() Config {
	cfg := Config{
		LatencyProbability: pct("CHAOS_LATENCY_PCT"),
		LatencyMin:         millis("CHAOS_LATENCY_MIN_MS", 50*time.Millisecond),
		LatencyMax:         millis("CHAOS_LATENCY_MAX_MS", 800*time.Millisecond),
		FaultProbability:   pct("CHAOS_FAULT_PCT"),
	}
	if cfg.LatencyMax < cfg.LatencyMin {
		cfg.LatencyMax = cfg.LatencyMin
	}
	return cfg
}

// Injector applies a [Config]. The random source is a field so tests can make
// it deterministic.
type Injector struct {
	cfg  Config
	roll func() float64
}

// New builds an injector using the global random source.
func New(cfg Config) *Injector {
	return &Injector{cfg: cfg, roll: rand.Float64}
}

// NewWithRoll builds an injector with a supplied [0,1) source, for tests.
func NewWithRoll(cfg Config, roll func() float64) *Injector {
	return &Injector{cfg: cfg, roll: roll}
}

// Config returns the injector's configuration.
func (i *Injector) Config() Config { return i.cfg }

// Apply sleeps and/or fails according to the configuration, recording what it
// did on the active span. It returns [ErrInjected] when a fault fires.
//
// The sleep is interruptible: a chaos injector that ignores context
// cancellation turns a graceful shutdown into a timeout, and the resulting
// unended spans look exactly like a propagation bug.
func (i *Injector) Apply(ctx context.Context) error {
	if i == nil || !i.cfg.Enabled() {
		return nil
	}
	span := trace.SpanFromContext(ctx)

	if i.cfg.LatencyProbability > 0 && i.roll() < i.cfg.LatencyProbability {
		spread := i.cfg.LatencyMax - i.cfg.LatencyMin
		delay := i.cfg.LatencyMin
		if spread > 0 {
			delay += time.Duration(i.roll() * float64(spread))
		}
		span.SetAttributes(attribute.Int64(AttrInjectedLatency, delay.Milliseconds()))

		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	if i.cfg.FaultProbability > 0 && i.roll() < i.cfg.FaultProbability {
		span.SetAttributes(attribute.Bool(AttrInjectedFault, true))
		return ErrInjected
	}
	return nil
}

func pct(key string) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseFloat(strings.TrimSuffix(raw, "%"), 64)
	if err != nil || v <= 0 {
		return 0
	}
	// Accept both 0.2 and 20 for twenty per cent: the compose file and a human
	// typing into the chaos script disagree about which is natural.
	if v > 1 {
		v /= 100
	}
	if v > 1 {
		return 1
	}
	return v
}

func millis(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return fallback
	}
	return time.Duration(v) * time.Millisecond
}
