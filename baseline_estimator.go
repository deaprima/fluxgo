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
// The warm-up period starts at the first observation rather than at
// construction, so a circuit breaker that stays idle after startup still
// performs a full warm-up once traffic arrives. The estimator moves through
// three phases:
//  1. Warm-up: no baseline is set; IsWarmingUp returns true.
//  2. Fallback: warm-up ended while conditions were unhealthy; the current
//     signals are stored as a best-effort baseline and Observe keeps
//     trying to replace it with a valid capture.
//  3. Valid capture: the baseline is locked and Observe becomes a no-op.
type BaselineEstimator struct {
    cfg       Config
    now       func() time.Time

    mu           sync.RWMutex
    started      bool
    startTime    time.Time
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
    if !b.started {
        return true
    }
    return b.now().Sub(b.startTime) < b.cfg.WarmupDuration
}

// NeedsCapture reports whether the estimator still accepts observations.
// it returns true until a valid baseline has been capture
func (b *BaselineEstimator) NeedsCapture() bool {
    b.mu.RLock()
    defer b.mu.RUnlock()
    return !b.validCapture
}

// Observe feeds the current signals to the estimator. The first call starts the warm-up clock
func (b *BaselineEstimator) Observe(St, Pt, failureRate float64, closed bool) {
    b.mu.Lock()
    defer b.mu.Unlock()
    if b.validCapture{
        return
    }

    if !b.started {
        b.started = true
        b.startTime = b.now()
    }

    if b.now().Sub(b.startTime) < b.cfg.WarmupDuration {
        return
    }

    if closed && failureRate < b.cfg.WarmupMaxFailRate {
        b.sbase, b.pbase = St, Pt
        b.validCapture = true
        b.fallback  = false
        return
    }
    if !b.fallback {
        b.sbase, b.pbase = St, Pt
        b.fallback = true
    }
}

// Baseline returns the current Sbase and Pbase values.
// Returns (0, 0) if no capture or fallback has occurred yet.
func (b *BaselineEstimator) Baseline() (sbase, pbase float64) {
    b.mu.RLock()
    defer b.mu.RUnlock()
    return b.sbase, b.pbase
}