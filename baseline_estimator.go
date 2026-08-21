// baseline_estimator.go implements BaselineEstimator, which captures a
// healthy-state baseline (Sbase, Pbase) during the warm-up period and
// provides safeguards against biased baseline capture.
package fluxgo

import (
	"sync"
	"time"
)

// BaselineEstimator tracks the warm-up period and captures a healthy-state
// baseline (Sbase, Pbase) for use by AdaptiveThresholdCalculator.
//
// The estimator moves through three phases:
//  1. Warmup active: IsWarmingUp() returns true; TryCapture waits for healthy conditions.
//  2. Valid capture: conditions were good; baseline is locked and IsWarmingUp() returns false.
//  3. Fallback: warmup expired without a valid capture; current signals are stored
//     as a best-effort baseline (producing theta_t ≈ theta_base, equivalent to a
//     static gobreaker threshold), and re-capture continues on subsequent calls.
type BaselineEstimator struct {
    cfg       Config
    startTime time.Time
    now       func() time.Time
    mu           sync.RWMutex
    sbase        float64
    pbase        float64
    validCapture bool
    fallback     bool
}

// NewBaselineEstimator returns a BaselineEstimator that begins its warm-up
// period immediately using the wall clock.
func NewBaselineEstimator(cfg Config) *BaselineEstimator {
    return newBaselineEstimatorWithClock(cfg, time.Now)
}

// newBaselineEstimatorWithClock returns a BaselineEstimator with an injectable
// clock, used exclusively in tests to control time without sleeping.
func newBaselineEstimatorWithClock(cfg Config, now func() time.Time) *BaselineEstimator {
    return &BaselineEstimator{
        cfg:       cfg,
        startTime: now(),
        now:       now,
    }
}

// IsWarmingUp reports whether the estimator is still in the warm-up phase.
// It returns false once a valid baseline is captured or a fallback is set.
func (b *BaselineEstimator) IsWarmingUp() bool {
    b.mu.RLock()
    defer b.mu.RUnlock()
    if b.validCapture || b.fallback {
        return false
    }
    return b.now().Sub(b.startTime) < b.cfg.WarmupDuration
}

// TryCapture attempts to record St and Pt as the healthy-state baseline.
// A capture is accepted only when currentFailureRate is below WarmupMaxFailRate
// and both signal values are positive.
//
// If the warmup window expires before any valid capture, the current signals are
// stored as a best-effort fallback (making theta_t ≈ theta_base). Re-capture
// continues on subsequent calls and upgrades the baseline when conditions improve.
func (b *BaselineEstimator) TryCapture(St, Pt, currentFailureRate float64) {
    if St <= 0 || Pt <= 0 {
        return // no usable signal data yet
    }
    b.mu.Lock()
    defer b.mu.Unlock()
    conditionsGood := currentFailureRate < b.cfg.WarmupMaxFailRate
    if conditionsGood {
        b.sbase = St
        b.pbase = Pt
        b.validCapture = true
        b.fallback = false // upgrade from fallback if previously set
        return
    }
    // warmup expired without a valid capture: store current signals as fallback
    if !b.validCapture && !b.fallback {
        if b.now().Sub(b.startTime) >= b.cfg.WarmupDuration {
            b.sbase = St
            b.pbase = Pt
            b.fallback = true
        }
    }
}
// Baseline returns the current Sbase and Pbase values.
// Returns (0, 0) if no capture or fallback has occurred yet.
func (b *BaselineEstimator) Baseline() (sbase, pbase float64) {
    b.mu.RLock()
    defer b.mu.RUnlock()
    return b.sbase, b.pbase
}