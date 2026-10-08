// config_test.go contains unit tests for the Config struct and its validation.
package fluxgo

import (
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