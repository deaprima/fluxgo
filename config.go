// config.go defines the Config struct and default values for all tunable
// parameters of the circuit breaker.

package fluxgo

import (
	"errors"
	"time"
)

// weightSumTolerance is the maximum allowed deviation of W1 + W2 from 1.0,
// absorbing floating-point rounding in user-supplied weights.
const weightSumTolerance = 1e-9

// SlowRequestReference selects the latency that a successful request is
// compared with when it is classified as slow.
type SlowRequestReference int

const (
	// SlowRefRecent compares a request with the current rolling percentile
	// (Pt × SlowRequestMargin). It detects sudden latency spikes relative to
	// recent traffic, but follows a gradual degradation without flagging it.
	// This is the default.
	SlowRefRecent SlowRequestReference = iota

	// SlowRefBaseline compares a request with the healthy-state percentile
	// baseline (Pbase × SlowRequestMargin) once a valid baseline has been
	// captured, and behaves like SlowRefRecent until then. It also detects
	// gradual degradation, at the cost of classifying sustained traffic
	// spikes above the margin as slow.
	SlowRefBaseline
)

// Config holds all tunable parameters for a CircuitBreaker instance.
// Use DefaultConfig to obtain a valid starting point.
type Config struct {
	// Alpha is the EWMA smoothing factor (0 < Alpha <= 1).
	Alpha float64

	// WindowSize is the number of recent observations kept by the rolling
	// percentile engine and the outcome window.
	WindowSize int

	// PercentileTarget is the latency percentile to compute. Accepted values: 95 or 99.
	PercentileTarget float64

	// W1 is the weight of the EWMA signal in the degradation ratio. W1 + W2 must equal 1.
	W1 float64

	// W2 is the weight of the rolling percentile signal. W1 + W2 must equal 1.
	W2 float64

	// ThetaBase is the baseline failure-rate threshold, equivalent to a static
	// gobreaker threshold. It must be in the range (0, 1].
	ThetaBase float64

	// ThetaMin is the lower bound of the adaptive threshold after clamping.
	// It must satisfy 0 < ThetaMin <= ThetaMax.
	ThetaMin float64

	// ThetaMax is the upper bound of the adaptive threshold after clamping.
	// It must not exceed 1, otherwise the circuit could never trip.
	ThetaMax float64

	// WarmupDuration is the minimum period during which baseline statistics are
	// collected before adaptive thresholds take effect. Warm-up also continues
	// until the rolling window holds WindowSize latencies.
	WarmupDuration time.Duration

	// WarmupMaxFailRate is the maximum allowed failure rate for a baseline capture
	// to be considered valid (e.g., 0.05 = 5%). It must be in the range (0, 1].
	WarmupMaxFailRate float64

	// RecoveryTimeout is how long the breaker stays Open before moving to Half-Open.
	RecoveryTimeout time.Duration

	// MinDwellTime is the minimum time between consecutive state transitions,
	// used to prevent rapid oscillation (anti-flapping).
	MinDwellTime time.Duration

	// MinRequests is the minimum number of requests that must be present in
	// the outcome window before the failure rate is evaluated against the
	// threshold, and before a baseline can be captured as valid. It must be
	// between 1 and WindowSize. Default: 20.
	MinRequests int

	// HalfOpenMaxRequests is the number of probe requests allowed through
	// while the circuit is Half-Open. Further requests are rejected with
	// ErrTooManyRequests. The circuit closes once this many probes succeed
	// and reopens on the first failed probe. Must be at least 1. Default: 1.
	HalfOpenMaxRequests int

	// SlowRequestMargin is the multiplier applied to the latency selected by
	// SlowRequestReference to classify a request as degraded.
	// Must be greater than zero. Default: 2.0.
	SlowRequestMargin float64

	// SlowRequestReference selects whether slow requests are classified
	// against the recent rolling percentile or the healthy baseline.
	// Default: SlowRefRecent.
	SlowRequestReference SlowRequestReference

	// RandomSeed is an optional seed for reproducible testbed runs.
	// It is not used by the core library.
	RandomSeed int64
}

// DefaultConfig returns a Config with sensible starting-point values for
// preliminary tuning. All values may be adjusted via the Config fields.
func DefaultConfig() Config {
	return Config{
		Alpha:                0.3,
		WindowSize:           100,
		PercentileTarget:     95,
		W1:                   0.4,
		W2:                   0.6,
		ThetaBase:            0.6,
		ThetaMin:             0.05,
		ThetaMax:             0.9,
		WarmupDuration:       30 * time.Second,
		WarmupMaxFailRate:    0.05,
		RecoveryTimeout:      60 * time.Second,
		MinDwellTime:         5 * time.Second,
		MinRequests:          20,
		HalfOpenMaxRequests:  1,
		SlowRequestMargin:    2.0,
		SlowRequestReference: SlowRefRecent,
		RandomSeed:           0,
	}
}

// Validate returns an error if any Config field violates a required invariant.
//
// Floating-point range checks are written in the positive form !(valid) so
// that NaN, which makes every comparison false, is always rejected.
func (c Config) Validate() error {
	if !(c.Alpha > 0 && c.Alpha <= 1) {
		return errors.New("alpha must be in the range (0, 1]")
	}
	if c.WindowSize <= 0 {
		return errors.New("window size must be greater than zero")
	}
	if c.MinRequests < 1 || c.MinRequests > c.WindowSize {
		return errors.New("min requests must be in the range [1, window size]")
	}
	if c.PercentileTarget != 95 && c.PercentileTarget != 99 {
		return errors.New("percentile target must be 95 or 99")
	}
	if !(c.W1 >= 0 && c.W2 >= 0) {
		return errors.New("weights W1 and W2 must be non-negative")
	}
	if !(abs(c.W1+c.W2-1.0) <= weightSumTolerance) {
		return errors.New("weights W1 and W2 must sum to 1.0")
	}
	if !(c.ThetaMin > 0 && c.ThetaMin <= c.ThetaMax && c.ThetaMax <= 1) {
		return errors.New("theta bounds must satisfy 0 < theta min <= theta max <= 1")
	}
	if !(c.ThetaBase > 0 && c.ThetaBase <= 1) {
		return errors.New("theta base must be in the range (0, 1]")
	}
	if !(c.WarmupMaxFailRate > 0 && c.WarmupMaxFailRate <= 1) {
		return errors.New("warmup max fail rate must be in the range (0, 1]")
	}
	if c.WarmupDuration < 0 {
		return errors.New("warmup duration must be non-negative")
	}
	if c.RecoveryTimeout <= 0 {
		return errors.New("recovery timeout must be greater than zero")
	}
	if c.MinDwellTime < 0 {
		return errors.New("min dwell time must be non-negative")
	}
	if c.HalfOpenMaxRequests < 1 {
		return errors.New("half-open max requests must be at least 1")
	}
	if !(c.SlowRequestMargin > 0) {
		return errors.New("slow request margin must be greater than zero")
	}
	if c.SlowRequestReference != SlowRefRecent && c.SlowRequestReference != SlowRefBaseline {
		return errors.New("slow request reference must be SlowRefRecent or SlowRefBaseline")
	}
	return nil
}

// abs returns the absolute value of x.
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
