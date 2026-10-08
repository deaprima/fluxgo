package fluxgo

import (
	"testing"
	"time"
)

func TestBaselineEstimator_IdleBeforeFirstObservation(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	b := newBaselineEstimatorWithClock(cfg, clock.Now)

	clock.Advance(10 * time.Minute)
	if !b.IsWarmingUp() {
		t.Fatal("expected warm-up to remain active before the first observation")
	}

	b.Observe(20, 40, 0, true)
	clock.Advance(cfg.WarmupDuration)
	b.Observe(20, 40, 0, true)

	if sbase, pbase := b.Baseline(); sbase != 20 || pbase != 40 {
		t.Fatalf("expected baseline (20, 40), got (%v, %v)", sbase, pbase)
	}
}

func TestBaselineEstimator_NoCaptureBeforeWarmupEnds(t *testing.T) {
	clock := newFakeClock()
	b := newBaselineEstimatorWithClock(DefaultConfig(), clock.Now)

	b.Observe(20, 40, 0, true)

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

	b.Observe(100, 300, 0.5, true)
	clock.Advance(cfg.WarmupDuration)
	b.Observe(100, 300, 0.5, true)
	if b.IsWarmingUp() || !b.NeedsCapture() {
		t.Fatal("expected fallback baseline with capture still pending")
	}

	b.Observe(20, 40, 0, true)
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

	b.Observe(20, 40, 0, false)

	if !b.NeedsCapture() {
		t.Fatal("expected no valid capture while the circuit is not closed")
	}
}