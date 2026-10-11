// state_machine_test.go tests StateMachine state transitions, anti-flapping,
// half-open probe limiting, generation handling, and thread-safety under
// concurrent access.
package fluxgo

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// smClock returns a controllable clock for StateMachine tests.
func smClock(initial time.Time) (now func() time.Time, advance func(time.Duration)) {
	current := initial
	return func() time.Time { return current },
		func(d time.Duration) { current = current.Add(d) }
}

// tripAndRecover drives sm from Closed to HalfOpen and returns the
// generation of the first admitted probe.
func tripAndRecover(t *testing.T, sm *StateMachine, advance func(time.Duration)) uint64 {
	t.Helper()
	if !sm.EvaluateClosed(sm.Generation(), 0.7, 0.6) {
		t.Fatal("expected Closed -> Open")
	}
	advance(sm.cfg.RecoveryTimeout + time.Second)
	gen, transitioned, err := sm.AllowRequest()
	if err != nil || !transitioned {
		t.Fatalf("AllowRequest() = (%d, %v, %v), want Open -> HalfOpen", gen, transitioned, err)
	}
	return gen
}

func TestStateMachine_InitialState_IsClosed(t *testing.T) {
	now, _ := smClock(time.Now())
	sm := newStateMachineWithClock(DefaultConfig(), now)
	if got := sm.State(); got != StateClosed {
		t.Errorf("State() = %v, want Closed", got)
	}
}

func TestStateMachine_ClosedToOpen_WhenThresholdExceeded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, _ := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	sm.EvaluateClosed(sm.Generation(), 0.7, 0.6)

	if got := sm.State(); got != StateOpen {
		t.Errorf("State() = %v, want Open after threshold exceeded", got)
	}
}

func TestStateMachine_ClosedToOpen_BlockedWhenBelowThreshold(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, _ := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	sm.EvaluateClosed(sm.Generation(), 0.4, 0.6)

	if got := sm.State(); got != StateClosed {
		t.Errorf("State() = %v, want Closed when below threshold", got)
	}
}

func TestStateMachine_OpenToHalfOpen_AfterRecoveryTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	tripAndRecover(t, sm, advance)

	if got := sm.State(); got != StateHalfOpen {
		t.Errorf("State() = %v, want HalfOpen after RecoveryTimeout", got)
	}
}

func TestStateMachine_OpenRejectsRequests_BeforeRecoveryTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, _ := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	sm.EvaluateClosed(sm.Generation(), 0.7, 0.6)

	if _, _, err := sm.AllowRequest(); !errors.Is(err, ErrOpenState) {
		t.Errorf("AllowRequest() error = %v, want ErrOpenState", err)
	}
}

func TestStateMachine_HalfOpenSuccess_TransitionsToClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	gen := tripAndRecover(t, sm, advance)

	if !sm.OnHalfOpenSuccess(gen) {
		t.Fatal("OnHalfOpenSuccess() = false, want true")
	}
	if got := sm.State(); got != StateClosed {
		t.Errorf("State() = %v, want Closed after probe success", got)
	}
}

func TestStateMachine_HalfOpenFailure_TransitionsToOpen(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	gen := tripAndRecover(t, sm, advance)

	if !sm.OnHalfOpenFailure(gen) {
		t.Fatal("OnHalfOpenFailure() = false, want true")
	}
	if got := sm.State(); got != StateOpen {
		t.Errorf("State() = %v, want Open after probe failure", got)
	}
}

func TestStateMachine_HalfOpen_RejectsExcessProbes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	cfg.HalfOpenMaxRequests = 1
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	tripAndRecover(t, sm, advance)

	if _, _, err := sm.AllowRequest(); !errors.Is(err, ErrTooManyRequests) {
		t.Errorf("AllowRequest() error = %v, want ErrTooManyRequests", err)
	}
}

func TestStateMachine_HalfOpen_ClosesAfterAllProbesSucceed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	cfg.HalfOpenMaxRequests = 3
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	gen := tripAndRecover(t, sm, advance)
	for i := 1; i < cfg.HalfOpenMaxRequests; i++ {
		if _, _, err := sm.AllowRequest(); err != nil {
			t.Fatalf("probe %d rejected: %v", i+1, err)
		}
	}

	sm.OnHalfOpenSuccess(gen)
	sm.OnHalfOpenSuccess(gen)
	if got := sm.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v, want HalfOpen after 2 of 3 successes", got)
	}
	if !sm.OnHalfOpenSuccess(gen) {
		t.Fatal("expected the third success to close the circuit")
	}
	if got := sm.State(); got != StateClosed {
		t.Errorf("State() = %v, want Closed", got)
	}
}

func TestStateMachine_IgnoresStaleGeneration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	staleGen, _, _ := sm.AllowRequest()
	tripAndRecover(t, sm, advance)

	if sm.OnHalfOpenSuccess(staleGen) {
		t.Fatal("expected a result from a stale generation to be ignored")
	}
	if got := sm.State(); got != StateHalfOpen {
		t.Errorf("State() = %v, want HalfOpen", got)
	}
}

func TestStateMachine_EvaluateClosed_IgnoresStaleGeneration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	staleGen, _, _ := sm.AllowRequest()
	gen := tripAndRecover(t, sm, advance)
	sm.OnHalfOpenSuccess(gen)

	if sm.EvaluateClosed(staleGen, 1.0, 0.6) {
		t.Fatal("expected EvaluateClosed to ignore a stale generation")
	}
}

func TestStateMachine_AntiFlapping_BlocksRapidTransition(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 5 * time.Second
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	advance(6 * time.Second)
	gen := tripAndRecover(t, sm, advance)
	sm.OnHalfOpenSuccess(gen)

	sm.EvaluateClosed(sm.Generation(), 0.7, 0.6)
	if sm.State() != StateClosed {
		t.Error("expected Closed: anti-flapping should block rapid re-trip")
	}

	advance(6 * time.Second)
	sm.EvaluateClosed(sm.Generation(), 0.7, 0.6)
	if sm.State() != StateOpen {
		t.Error("expected Open after MinDwellTime elapsed")
	}
}

func TestStateMachine_GenerationIncrementsOnTransition(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	now, _ := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	before := sm.Generation()
	sm.EvaluateClosed(before, 0.7, 0.6)

	if after := sm.Generation(); after != before+1 {
		t.Errorf("Generation() = %d, want %d after one transition", after, before+1)
	}
}

func TestStateMachine_ConcurrentAccess_RaceDetector(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	sm := NewStateMachine(cfg)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gen, _, _ := sm.AllowRequest()
			sm.EvaluateClosed(gen, 0.7, 0.6)
			sm.OnHalfOpenSuccess(gen)
			sm.State()
		}()
	}
	wg.Wait()
}

func TestStateMachine_OnHalfOpenSuccess_NoopWhenNotHalfOpen(t *testing.T) {
	sm := NewStateMachine(DefaultConfig())
	if sm.OnHalfOpenSuccess(sm.Generation()) {
		t.Error("OnHalfOpenSuccess() = true, want false when Closed")
	}
	if sm.State() != StateClosed {
		t.Errorf("State() = %v, want Closed (no-op)", sm.State())
	}
}

func TestStateMachine_OnHalfOpenFailure_NoopWhenNotHalfOpen(t *testing.T) {
	sm := NewStateMachine(DefaultConfig())
	if sm.OnHalfOpenFailure(sm.Generation()) {
		t.Error("OnHalfOpenFailure() = true, want false when Closed")
	}
	if sm.State() != StateClosed {
		t.Errorf("State() = %v, want Closed (no-op)", sm.State())
	}
}

func TestStateMachine_AllowRequest_ReportsTransitionOccurred(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	cfg.HalfOpenMaxRequests = 2
	now, advance := smClock(time.Now())
	sm := newStateMachineWithClock(cfg, now)

	tripAndRecover(t, sm, advance)

	_, transitioned, err := sm.AllowRequest()
	if err != nil || transitioned {
		t.Fatalf("AllowRequest() = (%v, %v), want (false, nil) when already HalfOpen", transitioned, err)
	}
}
