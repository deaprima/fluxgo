// state_machine_test.go tests StateMachine state transitions, anti-flapping,
// and thread-safety under concurrent access.
package fluxgo

import (
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

func TestStateMachine_InitialState_IsClosed(t *testing.T) {
    now, _ := smClock(time.Now())
    sm := newStateMachineWithClock(DefaultConfig(), now)
    if got := sm.State(); got != StateClosed {
        t.Errorf("State() = %v, want Closed", got)
    }
}

func TestStateMachine_ClosedToOpen_WhenThresholdExceeded(t *testing.T) {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 0 // disable anti-flapping for this test
    now, _ := smClock(time.Now())
    sm := newStateMachineWithClock(cfg, now)

    sm.EvaluateClosed(0.7, 0.6) // failureRate 70% >= theta_t 60%

    if got := sm.State(); got != StateOpen {
        t.Errorf("State() = %v, want Open after threshold exceeded", got)
    }
}

func TestStateMachine_ClosedToOpen_BlockedWhenBelowThreshold(t *testing.T) {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 0
    now, _ := smClock(time.Now())
    sm := newStateMachineWithClock(cfg, now)

    sm.EvaluateClosed(0.4, 0.6) // failureRate 40% < theta_t 60%

    if got := sm.State(); got != StateClosed {
        t.Errorf("State() = %v, want Closed when below threshold", got)
    }
}

func TestStateMachine_OpenToHalfOpen_AfterRecoveryTimeout(t *testing.T) {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 0
    now, advance := smClock(time.Now())
    sm := newStateMachineWithClock(cfg, now)

    sm.EvaluateClosed(0.7, 0.6) // → Open
    advance(cfg.RecoveryTimeout + time.Second)

    if allowed, _ := sm.AllowRequest(); !allowed {
        t.Error("AllowRequest() = false, want true after RecoveryTimeout (should be HalfOpen)")
    }
    if got := sm.State(); got != StateHalfOpen {
        t.Errorf("State() = %v, want HalfOpen after RecoveryTimeout", got)
    }
}

func TestStateMachine_OpenRejectsRequests_BeforeRecoveryTimeout(t *testing.T) {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 0
    now, _ := smClock(time.Now())
    sm := newStateMachineWithClock(cfg, now)

    sm.EvaluateClosed(0.7, 0.6) // → Open

    if allowed, _ := sm.AllowRequest(); allowed {
        t.Error("AllowRequest() = true, want false while Open before RecoveryTimeout")
    }
}

func TestStateMachine_HalfOpenSuccess_TransitionsToClosed(t *testing.T) {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 0
    now, advance := smClock(time.Now())
    sm := newStateMachineWithClock(cfg, now)

    sm.EvaluateClosed(0.7, 0.6) // → Open
    advance(cfg.RecoveryTimeout + time.Second)
    sm.AllowRequest() // → HalfOpen
    sm.OnHalfOpenSuccess()

    if got := sm.State(); got != StateClosed {
        t.Errorf("State() = %v, want Closed after probe success", got)
    }
}

func TestStateMachine_HalfOpenFailure_TransitionsToOpen(t *testing.T) {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 0
    now, advance := smClock(time.Now())
    sm := newStateMachineWithClock(cfg, now)

    sm.EvaluateClosed(0.7, 0.6) // → Open
    advance(cfg.RecoveryTimeout + time.Second)
    sm.AllowRequest() // → HalfOpen
    sm.OnHalfOpenFailure()

    if got := sm.State(); got != StateOpen {
        t.Errorf("State() = %v, want Open after probe failure", got)
    }
}

func TestStateMachine_AntiFlapping_BlocksRapidTransition(t *testing.T) {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 5 * time.Second
    now, advance := smClock(time.Now())
    sm := newStateMachineWithClock(cfg, now)

    // first trip: after MinDwellTime passes since creation
    advance(6 * time.Second)
    sm.EvaluateClosed(0.7, 0.6) // should open
    if sm.State() != StateOpen {
        t.Fatal("expected Open after MinDwellTime passed")
    }

    // recover to Closed
    advance(cfg.RecoveryTimeout + time.Second)
    sm.AllowRequest() // → HalfOpen
    sm.OnHalfOpenSuccess() // → Closed

    // immediately try to trip again — should be blocked by MinDwellTime
    sm.EvaluateClosed(0.7, 0.6)
    if sm.State() != StateClosed {
        t.Error("expected Closed: anti-flapping should block rapid re-trip")
    }

    // after MinDwellTime, trip should succeed
    advance(6 * time.Second)
    sm.EvaluateClosed(0.7, 0.6)
    if sm.State() != StateOpen {
        t.Error("expected Open after MinDwellTime elapsed")
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
            sm.AllowRequest()
            sm.EvaluateClosed(0.7, 0.6)
            sm.State()
        }()
    }
    wg.Wait()
}

func TestStateMachine_OnHalfOpenSuccess_NoopWhenNotHalfOpen(t *testing.T) {
    sm := NewStateMachine(DefaultConfig())
    sm.OnHalfOpenSuccess() 
    if sm.State() != StateClosed {
        t.Errorf("State() = %v, want Closed (no-op)", sm.State())
    }
}

func TestStateMachine_OnHalfOpenFailure_NoopWhenNotHalfOpen(t *testing.T) {
    sm := NewStateMachine(DefaultConfig())
    sm.OnHalfOpenFailure() 
    if sm.State() != StateClosed {
        t.Errorf("State() = %v, want Closed (no-op)", sm.State())
    }
}

func TestStateMachine_AllowRequest_ReportsTransitionOccurred(t *testing.T) {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 0
    now, advance := smClock(time.Now())
    sm := newStateMachineWithClock(cfg, now)

    sm.EvaluateClosed(0.7, 0.6) // → Open
    advance(cfg.RecoveryTimeout + time.Second)

    allowed, transitioned := sm.AllowRequest()
    if !allowed || !transitioned {
        t.Fatalf("AllowRequest() = (%v, %v), want (true, true) on Open->HalfOpen", allowed, transitioned)
    }

    allowed, transitioned = sm.AllowRequest()
    if !allowed || transitioned {
        t.Fatalf("AllowRequest() = (%v, %v), want (true, false) when already HalfOpen", allowed, transitioned)
    }
}