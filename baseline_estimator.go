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
//
// Signals that are not strictly positive are never stored, so the baseline
// is either unset or usable as a divisor.
type BaselineEstimator struct {
	cfg Config
	now func() time.Time

	mu           sync.RWMutex
	started      bool
	startTime    time.Time
	sbase        float64
	pbase        float64
	validCapture bool
	fallback     bool
}

// NewBaselineEstimator returns a BaselineEstimator using the wall clock.
// The warm-up period starts lazily, at the first call to Observe.
func NewBaselineEstimator(cfg Config) *BaselineEstimator {
	return newBaselineEstimatorWithClock(cfg, time.Now)
}

// newBaselineEstimatorWithClock returns a BaselineEstimator with an injectable
// clock, used by tests to control time without sleeping.
func newBaselineEstimatorWithClock(cfg Config, now func() time.Time) *BaselineEstimator {
	return &BaselineEstimator{cfg: cfg, now: now}
}

// IsWarmingUp reports whether no baseline, valid or fallback, has been set.
func (b *BaselineEstimator) IsWarmingUp() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return !b.validCapture && !b.fallback
}

// NeedsCapture reports whether the estimator still accepts observations.
// It returns true until a valid baseline has been captured.
func (b *BaselineEstimator) NeedsCapture() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return !b.validCapture
}

// Observe feeds the current signals to the estimator. The first call starts
// the warm-up clock. Once WarmupDuration has elapsed and both signals are
// positive, the signals are captured as a valid baseline if the circuit is
// closed and failureRate is below WarmupMaxFailRate; otherwise they are
// stored once as a fallback and later calls keep attempting a valid capture.
func (b *BaselineEstimator) Observe(St, Pt, failureRate float64, closed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.validCapture {
		return
	}
	if !b.started {
		b.started = true
		b.startTime = b.now()
	}
	if b.now().Sub(b.startTime) < b.cfg.WarmupDuration {
		return
	}
	if St <= 0 || Pt <= 0 {
		return
	}
	if closed && failureRate < b.cfg.WarmupMaxFailRate {
		b.sbase, b.pbase = St, Pt
		b.validCapture = true
		b.fallback = false
		return
	}
	if !b.fallback {
		b.sbase, b.pbase = St, Pt
		b.fallback = true
	}
}

// Baseline returns the current Sbase and Pbase values.
// It returns (0, 0) if no capture or fallback has occurred yet.
func (b *BaselineEstimator) Baseline() (sbase, pbase float64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.sbase, b.pbase
}
