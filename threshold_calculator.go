// threshold_calculator.go implements AdaptiveThresholdCalculator, which
// combines EWMA and rolling percentile signals to produce a dynamic
// failure threshold (theta_t).
package fluxgo

// AdaptiveThresholdCalculator computes a dynamic failure-rate threshold
// (theta_t) by comparing current latency signals against a healthy-state
// baseline. All weights and bounds are sourced from Config.
type AdaptiveThresholdCalculator struct {
	cfg Config
}

// NewAdaptiveThresholdCalculator returns a calculator configured with the
// given Config.
func NewAdaptiveThresholdCalculator(cfg Config) *AdaptiveThresholdCalculator {
	return &AdaptiveThresholdCalculator{cfg: cfg}
}

// Calculate returns theta_t, the adaptive failure-rate threshold for the
// current observation window.
//
// Parameters:
//   - St:    current EWMA latency value
//   - Pt:    current rolling percentile value (P95 or P99)
//   - Sbase: healthy-state EWMA baseline from BaselineEstimator
//   - Pbase: healthy-state percentile baseline from BaselineEstimator
//
// If Sbase or Pbase is zero, Calculate returns cfg.ThetaBase directly to
// avoid division by zero.
func (a *AdaptiveThresholdCalculator) Calculate(St, Pt, Sbase, Pbase float64) float64 {
	if Sbase <= 0 || Pbase <= 0 {
		return a.cfg.ThetaBase
	}

	rEWMA := St/Sbase
	rP := Pt/Pbase
	Dt := a.cfg.W1*rEWMA + a.cfg.W2*rP

	if Dt <= 0 {
		return a.cfg.ThetaMax
	}

	return clamp(a.cfg.ThetaBase/Dt, a.cfg.ThetaMin, a.cfg.ThetaMax)
}

// clamp returns x bounded to [min, max].
func clamp(x, min, max float64) float64 {
	if x < min {
		return min
	}
	if x > max {
		return max
	}
	return x
}
