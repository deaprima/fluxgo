// baseline_estimator_test.go tests BaselineEstimator for correct capture
// behavior, safeguards, fallback, and re-capture after warmup expiry.
package fluxgo

import (
    "testing"
    "time"
)

// testClock returns a controllable clock. Calling advance(d) moves time forward.
func testClock(initial time.Time) (now func() time.Time, advance func(time.Duration)) {
    current := initial
    return func() time.Time { return current },
        func(d time.Duration) { current = current.Add(d) }
}

func TestBaselineEstimator_CaptureSucceedsWhenConditionsGood(t *testing.T) {
    cfg := DefaultConfig()
    now, _ := testClock(time.Now())
    b := newBaselineEstimatorWithClock(cfg, now)

    if !b.IsWarmingUp() {
        t.Fatal("expected IsWarmingUp() = true before any capture")
    }

    b.TryCapture(100.0, 120.0, 0.02) // failure rate 2% < 5% threshold

    if b.IsWarmingUp() {
        t.Error("expected IsWarmingUp() = false after valid capture")
    }
    sbase, pbase := b.Baseline()
    if sbase != 100.0 || pbase != 120.0 {
        t.Errorf("Baseline() = (%f, %f), want (100.0, 120.0)", sbase, pbase)
    }
}

func TestBaselineEstimator_CaptureRejectedWhenFailureRateHigh(t *testing.T) {
    cfg := DefaultConfig()
    now, _ := testClock(time.Now())
    b := newBaselineEstimatorWithClock(cfg, now)

    b.TryCapture(100.0, 120.0, 0.30) // failure rate 30% > 5% threshold

    if !b.IsWarmingUp() {
        t.Error("expected IsWarmingUp() = true when capture was rejected")
    }
    sbase, pbase := b.Baseline()
    if sbase != 0 || pbase != 0 {
        t.Errorf("Baseline() = (%f, %f), want (0, 0) after rejected capture", sbase, pbase)
    }
}

func TestBaselineEstimator_FallbackAfterWarmupExpires(t *testing.T) {
    cfg := DefaultConfig() // WarmupDuration = 30s
    now, advance := testClock(time.Now())
    b := newBaselineEstimatorWithClock(cfg, now)

    advance(31 * time.Second) // past warmup duration

    b.TryCapture(80.0, 90.0, 0.40) // bad conditions, but warmup expired

    if b.IsWarmingUp() {
        t.Error("expected IsWarmingUp() = false after warmup expired (fallback)")
    }
    sbase, pbase := b.Baseline()
    if sbase != 80.0 || pbase != 90.0 {
        t.Errorf("Baseline() = (%f, %f), want fallback (80.0, 90.0)", sbase, pbase)
    }
}

func TestBaselineEstimator_RecaptureUpgradesFallback(t *testing.T) {
    cfg := DefaultConfig()
    now, advance := testClock(time.Now())
    b := newBaselineEstimatorWithClock(cfg, now)

    // trigger fallback
    advance(31 * time.Second)
    b.TryCapture(80.0, 90.0, 0.40)

    // conditions improve → should upgrade to valid capture
    b.TryCapture(100.0, 120.0, 0.02)

    sbase, pbase := b.Baseline()
    if sbase != 100.0 || pbase != 120.0 {
        t.Errorf("Baseline() = (%f, %f), want upgraded capture (100.0, 120.0)", sbase, pbase)
    }
}
