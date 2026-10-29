package chaos_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MahyarNemati/otel-microservices/internal/chaos"
	"github.com/MahyarNemati/otel-microservices/internal/obstest"
)

// rolls returns a deterministic sequence, so an assertion about "the fault
// fired" is about the configuration rather than about luck.
func rolls(vs ...float64) func() float64 {
	i := -1
	return func() float64 {
		i++
		if i < len(vs) {
			return vs[i]
		}
		return vs[len(vs)-1]
	}
}

func TestDisabledInjectorDoesNothing(t *testing.T) {
	t.Parallel()
	inj := chaos.New(chaos.Config{})
	start := time.Now()
	if err := inj.Apply(context.Background()); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Errorf("a disabled injector slept for %s", elapsed)
	}
}

// TestInjectedLatencyIsRecordedOnSpan is the honesty requirement: injected pain
// must be labelled, or a reader cannot tell a demo from a regression.
func TestInjectedLatencyIsRecordedOnSpan(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	inj := chaos.NewWithRoll(chaos.Config{
		LatencyProbability: 1,
		LatencyMin:         30 * time.Millisecond,
		LatencyMax:         30 * time.Millisecond,
	}, rolls(0))

	ctx, span := rec.Tracer.Start(context.Background(), "work")
	start := time.Now()
	if err := inj.Apply(ctx); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	elapsed := time.Since(start)
	span.End()

	if elapsed < 25*time.Millisecond {
		t.Errorf("slept %s, want at least ~30ms", elapsed)
	}
	got := rec.Span(t, "work")
	if v := obstest.RequireAttr(t, got, chaos.AttrInjectedLatency); v != "30" {
		t.Errorf("%s = %q, want 30", chaos.AttrInjectedLatency, v)
	}
}

func TestInjectedFaultIsRecordedOnSpan(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	inj := chaos.NewWithRoll(chaos.Config{FaultProbability: 0.5}, rolls(0.1))

	ctx, span := rec.Tracer.Start(context.Background(), "work")
	err := inj.Apply(ctx)
	span.End()

	if !errors.Is(err, chaos.ErrInjected) {
		t.Fatalf("Apply returned %v, want ErrInjected", err)
	}
	got := rec.Span(t, "work")
	if v := obstest.RequireAttr(t, got, chaos.AttrInjectedFault); v != "true" {
		t.Errorf("%s = %q, want true", chaos.AttrInjectedFault, v)
	}
}

func TestRollAboveProbabilityDoesNotFire(t *testing.T) {
	t.Parallel()
	inj := chaos.NewWithRoll(chaos.Config{FaultProbability: 0.2}, rolls(0.9))
	if err := inj.Apply(context.Background()); err != nil {
		t.Errorf("Apply fired at roll 0.9 with probability 0.2: %v", err)
	}
}

// TestApplyRespectsCancellation covers the shutdown path: a chaos sleep that
// ignores cancellation turns a graceful stop into unended spans, which looks
// exactly like a propagation bug.
func TestApplyRespectsCancellation(t *testing.T) {
	t.Parallel()
	inj := chaos.NewWithRoll(chaos.Config{
		LatencyProbability: 1,
		LatencyMin:         10 * time.Second,
		LatencyMax:         10 * time.Second,
	}, rolls(0))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := inj.Apply(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply returned %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Apply took %s to notice cancellation", elapsed)
	}
}

func TestConfigFromEnvAcceptsBothPercentForms(t *testing.T) {
	t.Setenv("CHAOS_LATENCY_PCT", "25")
	t.Setenv("CHAOS_FAULT_PCT", "0.1")
	t.Setenv("CHAOS_LATENCY_MIN_MS", "100")
	t.Setenv("CHAOS_LATENCY_MAX_MS", "200")

	cfg := chaos.ConfigFromEnv()
	if cfg.LatencyProbability != 0.25 {
		t.Errorf("LatencyProbability = %v, want 0.25 (25 should mean 25%%)", cfg.LatencyProbability)
	}
	if cfg.FaultProbability != 0.1 {
		t.Errorf("FaultProbability = %v, want 0.1", cfg.FaultProbability)
	}
	if cfg.LatencyMin != 100*time.Millisecond || cfg.LatencyMax != 200*time.Millisecond {
		t.Errorf("latency bounds = [%s,%s], want [100ms,200ms]", cfg.LatencyMin, cfg.LatencyMax)
	}
	if !cfg.Enabled() {
		t.Error("Enabled() is false for a configured injector")
	}
}

func TestConfigFromEnvDefaultsToOff(t *testing.T) {
	t.Setenv("CHAOS_LATENCY_PCT", "")
	t.Setenv("CHAOS_FAULT_PCT", "")
	cfg := chaos.ConfigFromEnv()
	if cfg.Enabled() {
		t.Errorf("chaos is enabled by default: %s", cfg)
	}
	if cfg.String() != "disabled" {
		t.Errorf("String() = %q, want \"disabled\"", cfg.String())
	}
}

func TestConfigFromEnvSwapsInvertedBounds(t *testing.T) {
	t.Setenv("CHAOS_LATENCY_PCT", "10")
	t.Setenv("CHAOS_LATENCY_MIN_MS", "500")
	t.Setenv("CHAOS_LATENCY_MAX_MS", "100")
	cfg := chaos.ConfigFromEnv()
	if cfg.LatencyMax < cfg.LatencyMin {
		t.Errorf("max %s is below min %s", cfg.LatencyMax, cfg.LatencyMin)
	}
}
