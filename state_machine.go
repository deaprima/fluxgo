// state_machine.go implements the Closed/Open/Half-Open state machine,
// including anti-flapping protection via a minimum dwell time constraint.
package fluxgo

import (
	"sync"
	"time"
)

// State represent the operational state of a circuit breaker.
type State int 

const (
	// StateClosed is the normal operating state; requests pass through.
	StateClosed State = iota

	// StateOpen is the tripped state; request are rejected immediately.
	StateOpen

	// StateHalfOpen is the recovery probe state; one request is allowed
	// to test whether the downstream service has recovered.
	StateHalfOpen
)

// String return a human-readable name for the state
func (s State) String() string {
	switch s {
	case StateClosed: return "closed"
	case StateOpen: return "open"
	case StateHalfOpen: return "half-open"
	default: return "unknown"
	}
}

// StateMachine manages circuit breaker state transitions for the Closed, Open, and HalfOpen states.
// Antiflaping is enforced via MinDwellTime: the circuit cannot transition to Open until at least
// MinDwellTime has elapsed since the last state transition
type StateMachine struct {
	cfg				Config
	now				func() time.Time

	mu 				sync.Mutex
	state			State
	openedAt		time.Time
	lastTransition	time.Time
}

// NewStateMachine return a StateMachine in the Closed state using the wall clock
func NewStateMachine(cfg Config) *StateMachine {
	return newStateMachineWithClock(cfg, time.Now)
}

// newStateMachineWithClock returns a StateMachine with an injectable clock,
// used exclusively in tests to control time without sleeping.
func newStateMachineWithClock(cfg Config, now func() time.Time) *StateMachine {
    return &StateMachine{
        cfg:            cfg,
        now:            now,
        state:          StateClosed,
        lastTransition: now(),
    }
}

// State return the current circuit state
func (sm *StateMachine) State() State {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

// AllowRequest reports whether the next request may proceed. If the circuit
// is Open and RecoveryTimeout has elapsed, it transitions to HalfOpen and
// reports transitioned as true.
func (sm *StateMachine) AllowRequest() (allowed bool, transitioned bool){
	sm.mu.Lock()
    defer sm.mu.Unlock()
    switch sm.state {
    case StateClosed, StateHalfOpen: 
        return true, false
    case StateOpen:
        if sm.now().Sub(sm.openedAt) >= sm.cfg.RecoveryTimeout {
            sm.setState(StateHalfOpen)
            return true, true
        }
    }
    return false, false
}

// EvaluateClosed checks whether the circuit should transition from Closed to
// Open. It is a no-op if the current state is not Closed. Anti-flapping is
// enforced: the transition is suppressed if less than MinDwellTime has elapsed
// since the last state transition.
func (sm *StateMachine) EvaluateClosed(failureRate, thetaT float64) bool {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    if sm.state != StateClosed || failureRate < thetaT {
        return false
    }

    // anti-flapping: enforce minimum dwell time before tripping to Open
    if sm.now().Sub(sm.lastTransition) < sm.cfg.MinDwellTime {
        return false
    }
    sm.openedAt = sm.now()
    sm.setState(StateOpen)
    return true
}

// OnHalfOpenSuccess records a successful probe request and transitions the
// circuit from Half-Open to Closed. It is a no-op if not in Half-Open state.
func (sm *StateMachine) OnHalfOpenSuccess() bool {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    if sm.state != StateHalfOpen {
        return false
    }
    sm.setState(StateClosed)
    return true
}

// OnHalfOpenFailure records a failed probe request and transitions the circuit
// from Half-Open back to Open. It is a no-op if not in Half-Open state.
func (sm *StateMachine) OnHalfOpenFailure() bool {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    if sm.state != StateHalfOpen {
        return false
    }
    sm.openedAt = sm.now()
    sm.setState(StateOpen)
    return true
}

// setState updates the circuit state and records the transition timestamp.
// It must be called with sm.mu held.
func (sm *StateMachine) setState(next State) {
    sm.state = next
    sm.lastTransition = sm.now()
}