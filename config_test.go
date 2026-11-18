// config_test.go contains unit tests for the Config struct and its validation.
package fluxgo

import (
	"math"
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("DefaultConfig() produced an invalid config: %v", err)
	}
}

func TestValidateRejectsInvalidAlpha(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Alpha = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for Alpha = 0, got nil")
	}

	cfg.Alpha = 1.5
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for Alpha = 1.5, got nil")
	}
}

func TestValidateRejectsInvalidWeights(t *testing.T) {
	cfg := DefaultConfig()
	cfg.W1 = 0.5
	cfg.W2 = 0.4 // sum = 0.9, bukan 1.0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for W1+W2 != 1.0, got nil")
	}
}

func TestValidateRejectsInvalidThetaRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ThetaMin = 0.9
	cfg.ThetaMax = 0.1 // min > max
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for ThetaMin > ThetaMax, got nil")
	}
}

func TestValidateRejectsInvalidPercentileTarget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PercentileTarget = 50
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for PercentileTarget = 50, got nil")
	}
}

func TestValidateRejectsZeroWindowSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WindowSize = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for WindowSize = 0, got nil")
	}
}

func TestValidateRejectsZeroDurations(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for WarmupDuration = -1, got nil")
	}

	cfg = DefaultConfig()
	cfg.RecoveryTimeout = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for RecoveryTimeout = 0, got nil")
	}

	cfg = DefaultConfig()
	cfg.MinDwellTime = -1 * time.Second
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative MinDwellTime, got nil")
	}
}

func TestDefaultConfig_SlowRequestMargin(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SlowRequestMargin <= 0 {
		t.Errorf("expected SlowRequestMargin > 0, got %v", cfg.SlowRequestMargin)
	}
}

func TestValidate_SlowRequestMargin_Zero(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SlowRequestMargin = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for SlowRequestMargin = 0, got nil")
	}
}

func TestValidate_SlowRequestMargin_Negative(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SlowRequestMargin = -1.0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for SlowRequestMargin = -1.0, got nil")
	}
}

func TestDefaultConfig_MinRequests(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MinRequests != 20 {
		t.Errorf("DefaultConfig().MinRequests = %d, want 20", cfg.MinRequests)
	}
}

func TestValidateRejectsMinRequestsOutOfRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinRequests = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for MinRequests = 0, got nil")
	}

	cfg = DefaultConfig()
	cfg.MinRequests = cfg.WindowSize + 1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for MinRequests > WindowSize, got nil")
	}
}

func TestDefaultConfig_HalfOpenMaxRequests(t *testing.T) {
	if got := DefaultConfig().HalfOpenMaxRequests; got != 1 {
		t.Errorf("DefaultConfig().HalfOpenMaxRequests = %d, want 1", got)
	}
}

func TestDefaultConfig_ConsecutiveFailures(t *testing.T) {
	if got := DefaultConfig().ConsecutiveFailures; got != 5 {
		t.Errorf("DefaultConfig().ConsecutiveFailures = %d, want 5", got)
	}
}

func TestValidateConsecutiveFailures(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConsecutiveFailures = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("ConsecutiveFailures = 0 (disabled): unexpected error %v", err)
	}
	cfg.ConsecutiveFailures = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for ConsecutiveFailures = -1, got nil")
	}
}

func TestDefaultConfig_SlowTripDuration(t *testing.T) {
	if got := DefaultConfig().SlowTripDuration; got != time.Second {
		t.Errorf("DefaultConfig().SlowTripDuration = %v, want 1s", got)
	}
}

func TestValidateSlowTripDuration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SlowTripDuration = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("SlowTripDuration = 0: unexpected error %v", err)
	}
	cfg.SlowTripDuration = -time.Millisecond
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for a negative SlowTripDuration, got nil")
	}
}

func TestValidateRejectsHalfOpenMaxRequestsBelowOne(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HalfOpenMaxRequests = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for HalfOpenMaxRequests = 0, got nil")
	}
}

func TestValidateRejectsThetaMaxAboveOne(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ThetaMax = 1.5
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for ThetaMax = 1.5, got nil")
	}
}

func TestValidateRejectsNonPositiveThetaMin(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ThetaMin = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for ThetaMin = 0, got nil")
	}
}

func TestValidateRejectsWarmupMaxFailRateOutOfRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupMaxFailRate = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for WarmupMaxFailRate = 0, got nil")
	}

	cfg = DefaultConfig()
	cfg.WarmupMaxFailRate = 1.5
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for WarmupMaxFailRate = 1.5, got nil")
	}
}

func TestValidateRejectsNaN(t *testing.T) {
	fields := map[string]func(*Config){
		"Alpha":             func(c *Config) { c.Alpha = math.NaN() },
		"W1":                func(c *Config) { c.W1 = math.NaN() },
		"ThetaBase":         func(c *Config) { c.ThetaBase = math.NaN() },
		"ThetaMin":          func(c *Config) { c.ThetaMin = math.NaN() },
		"ThetaMax":          func(c *Config) { c.ThetaMax = math.NaN() },
		"WarmupMaxFailRate": func(c *Config) { c.WarmupMaxFailRate = math.NaN() },
		"SlowRequestMargin": func(c *Config) { c.SlowRequestMargin = math.NaN() },
	}
	for name, set := range fields {
		cfg := DefaultConfig()
		set(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("expected error for %s = NaN, got nil", name)
		}
	}
}

func TestDefaultConfig_SlowRequestReference(t *testing.T) {
	if got := DefaultConfig().SlowRequestReference; got != SlowRefBaseline {
		t.Errorf("DefaultConfig().SlowRequestReference = %v, want SlowRefBaseline", got)
	}
	if got := DefaultConfig().SlowRequestMargin; got != 3.0 {
		t.Errorf("DefaultConfig().SlowRequestMargin = %v, want 3.0", got)
	}
}

func TestValidateRejectsUnknownSlowRequestReference(t *testing.T) {
	for _, ref := range []SlowRequestReference{-1, SlowRefBaseline + 1} {
		cfg := DefaultConfig()
		cfg.SlowRequestReference = ref
		if err := cfg.Validate(); err == nil {
			t.Errorf("expected error for SlowRequestReference = %d, got nil", ref)
		}
	}
}
