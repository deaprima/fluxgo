// threshold_calculator_test.go tests AdaptiveThresholdCalculator for correct
// threshold computation, clamping behavior, and division-by-zero safety.
package fluxgo

import (
    "math"
    "testing"
)

func TestAdaptiveThresholdCalculator_NormalConditions(t *testing.T) {
    cfg := DefaultConfig() // ThetaBase=0.6, W1=0.4, W2=0.6, Min=0.05, Max=0.9
    calc := NewAdaptiveThresholdCalculator(cfg)

    // St == Sbase, Pt == Pbase → Dt = 1.0 → theta_t = ThetaBase
    got := calc.Calculate(100.0, 120.0, 100.0, 120.0)
    if math.Abs(got-0.6) > 1e-9 {
        t.Errorf("Calculate() = %f, want 0.6 (Dt=1, theta_t=ThetaBase)", got)
    }
}

func TestAdaptiveThresholdCalculator_DegradedLatency_LowersThreshold(t *testing.T) {
    cfg := DefaultConfig()
    calc := NewAdaptiveThresholdCalculator(cfg)

    // St = 2x Sbase, Pt = 2x Pbase → Dt = 2.0 → theta_t = 0.6/2.0 = 0.3
    got := calc.Calculate(200.0, 240.0, 100.0, 120.0)
    want := 0.3
    if math.Abs(got-want) > 1e-9 {
        t.Errorf("Calculate() = %f, want %f (latency 2x baseline)", got, want)
    }
}

func TestAdaptiveThresholdCalculator_ImprovedLatency_RaisesThreshold(t *testing.T) {
    cfg := DefaultConfig()
    calc := NewAdaptiveThresholdCalculator(cfg)

    // St = 0.5x Sbase, Pt = 0.5x Pbase → Dt = 0.5 → theta_t = 0.6/0.5 = 1.2 → clamped to 0.9
    got := calc.Calculate(50.0, 60.0, 100.0, 120.0)
    if got != cfg.ThetaMax {
        t.Errorf("Calculate() = %f, want ThetaMax=%f (latency improved, clamp)", got, cfg.ThetaMax)
    }
}

func TestAdaptiveThresholdCalculator_ClampMin(t *testing.T) {
    cfg := DefaultConfig()
    calc := NewAdaptiveThresholdCalculator(cfg)

    // St = 100x Sbase → Dt very large → theta_t very small → clamped to ThetaMin
    got := calc.Calculate(10000.0, 12000.0, 100.0, 120.0)
    if got != cfg.ThetaMin {
        t.Errorf("Calculate() = %f, want ThetaMin=%f (extreme degradation, clamp)", got, cfg.ThetaMin)
    }
}

func TestAdaptiveThresholdCalculator_ZeroSbase_ReturnsThetaBase(t *testing.T) {
    cfg := DefaultConfig()
    calc := NewAdaptiveThresholdCalculator(cfg)

    got := calc.Calculate(100.0, 120.0, 0, 120.0)
    if got != cfg.ThetaBase {
        t.Errorf("Calculate() = %f, want ThetaBase=%f when Sbase=0", got, cfg.ThetaBase)
    }
}

func TestAdaptiveThresholdCalculator_ZeroPbase_ReturnsThetaBase(t *testing.T) {
    cfg := DefaultConfig()
    calc := NewAdaptiveThresholdCalculator(cfg)

    got := calc.Calculate(100.0, 120.0, 100.0, 0)
    if got != cfg.ThetaBase {
        t.Errorf("Calculate() = %f, want ThetaBase=%f when Pbase=0", got, cfg.ThetaBase)
    }
}
