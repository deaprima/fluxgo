package fluxgo

import (
	"testing"
	"time"
)

// observeHealthy feeds b healthy signals together with enough outcomes and
// latency samples to satisfy the sample requirements of a valid capture.
func observeHealthy(b *BaselineEstimator, cfg Config, St, Pt float64) {
	b.Observe(St, Pt, 0, cfg.MinRequests, cfg.WindowSize, true)
}

func TestBaselineEstimator_IdleBeforeFirstObservation(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	clock.Advance(10 * time.Minute)
	if !b.IsWarmingUp() {
		t.Fatal("expected warm-up to remain active before the first observation")
	}

	observeHealthy(b, cfg, 20, 40)
	clock.Advance(cfg.WarmupDuration)
	observeHealthy(b, cfg, 20, 40)

	if sbase, pbase := b.Baseline(); sbase != 20 || pbase != 40 {
		t.Fatalf("expected baseline (20, 40), got (%v, %v)", sbase, pbase)
	}
}

func TestBaselineEstimator_NoCaptureBeforeWarmupEnds(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	observeHealthy(b, cfg, 20, 40)

	if sbase, pbase := b.Baseline(); sbase != 0 || pbase != 0 {
		t.Fatalf("expected no capture on the first observation, got (%v, %v)", sbase, pbase)
	}
	if !b.IsWarmingUp() {
		t.Fatal("expected warm-up to be active")
	}
}

func TestBaselineEstimator_FallbackThenRecapture(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	b.Observe(100, 300, 0.5, cfg.MinRequests, cfg.WindowSize, true)
	clock.Advance(cfg.WarmupDuration)
	b.Observe(100, 300, 0.5, cfg.MinRequests, cfg.WindowSize, true)
	if b.IsWarmingUp() || !b.NeedsCapture() {
		t.Fatal("expected fallback baseline with capture still pending")
	}

	observeHealthy(b, cfg, 20, 40)
	if sbase, pbase := b.Baseline(); sbase != 20 || pbase != 40 {
		t.Fatalf("expected re-captured baseline (20, 40), got (%v, %v)", sbase, pbase)
	}
	if b.NeedsCapture() {
		t.Fatal("expected capture to be complete")
	}
}

func TestBaselineEstimator_NoValidCaptureWhenNotClosed(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cfg.WarmupDuration = 0
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	b.Observe(20, 40, 0, cfg.MinRequests, cfg.WindowSize, false)

	if !b.NeedsCapture() {
		t.Fatal("expected no valid capture while the circuit is not closed")
	}
}

func TestBaselineEstimator_RejectsNonPositiveSignals(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cfg.WarmupDuration = 0
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	observeHealthy(b, cfg, 0, 0)

	if !b.NeedsCapture() || !b.IsWarmingUp() {
		t.Fatal("expected zero signals to be rejected without setting a baseline")
	}

	observeHealthy(b, cfg, 20, 40)
	if sbase, pbase := b.Baseline(); sbase != 20 || pbase != 40 {
		t.Fatalf("expected baseline (20, 40) after positive signals, got (%v, %v)", sbase, pbase)
	}
}

func TestBaselineEstimator_WarmingUpUntilBaselineSet(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	observeHealthy(b, cfg, 20, 40)
	clock.Advance(2 * cfg.WarmupDuration)

	if !b.IsWarmingUp() {
		t.Fatal("expected warm-up to remain active until a baseline is set")
	}
}

func TestBaselineEstimator_WaitsForFullRollingWindow(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cfg.WarmupDuration = 0
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	b.Observe(20, 40, 0, cfg.MinRequests, cfg.WindowSize-1, true)
	if !b.IsWarmingUp() {
		t.Fatal("expected no baseline, valid or fallback, from a partial rolling window")
	}

	b.Observe(20, 40, 0, cfg.MinRequests, cfg.WindowSize, true)
	if b.NeedsCapture() {
		t.Fatal("expected a valid capture once the rolling window is full")
	}
}

func TestBaselineEstimator_NoValidCaptureBelowMinRequests(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cfg.WarmupDuration = 0
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	// A freshly reset outcome window, as right after a state transition,
	// reports a zero failure rate over a single outcome.
	b.Observe(20, 40, 0, 1, cfg.WindowSize, true)

	if !b.NeedsCapture() {
		t.Fatal("expected no valid capture from fewer than MinRequests outcomes")
	}
}

func TestNewBaselineEstimator_StartsWarmingUp(t *testing.T) {
	b := NewBaselineEstimator(DefaultConfig())
	if !b.IsWarmingUp() || !b.NeedsCapture() {
		t.Fatal("expected a new estimator to be warming up")
	}
}

func TestBaselineEstimator_ValidBaselineIsLocked(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cfg.WarmupDuration = 0
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	observeHealthy(b, cfg, 20, 40)
	observeHealthy(b, cfg, 500, 900)

	if sbase, pbase := b.Baseline(); sbase != 20 || pbase != 40 {
		t.Fatalf("expected the valid baseline (20, 40) to be locked, got (%v, %v)", sbase, pbase)
	}
}
