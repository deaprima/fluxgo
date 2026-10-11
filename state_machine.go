// state_machine.go implements the Closed/Open/Half-Open state machine,
// including anti-flapping protection via a minimum dwell time, a bounded
// number of half-open probes, and generation tracking that discards results
// of requests admitted before the latest transition.
package fluxgo

import (
	"sync"
	"time"
)

// State represents the operational state of a circuit breaker.
type State int

const (
	// StateClosed is the normal operating state; requests pass through.
	StateClosed State = iota

	// StateOpen is the tripped state; requests are rejected immediately.
	StateOpen

	// StateHalfOpen is the recovery probe state; at most
	// Config.HalfOpenMaxRequests requests are allowed to test whether the
	// downstream service has recovered.
	StateHalfOpen
)

// String returns a human-readable name for the state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// StateMachine manages circuit breaker state transitions for the Closed,
// Open, and HalfOpen states.
//
// Every transition increments a generation number. AllowRequest returns the
// generation a request is admitted in, and the result methods accept that
// generation back; results from an older generation are ignored. Anti-flapping
// is enforced via MinDwellTime: the circuit cannot transition from Closed to
// Open until at least MinDwellTime has elapsed since the last transition.
type StateMachine struct {
	cfg Config
	now func() time.Time

	mu                sync.Mutex
	state             State
	generation        uint64
	openedAt          time.Time
	lastTransition    time.Time
	halfOpenRequests  int
	halfOpenSuccesses int
}

// NewStateMachine returns a StateMachine in the Closed state using the wall clock.
func NewStateMachine(cfg Config) *StateMachine {
	return newStateMachineWithClock(cfg, time.Now)
}

// newStateMachineWithClock returns a StateMachine with an injectable clock,
// used by tests to control time without sleeping.
func newStateMachineWithClock(cfg Config, now func() time.Time) *StateMachine {
	return &StateMachine{
		cfg:            cfg,
		now:            now,
		state:          StateClosed,
		lastTransition: now(),
	}
}

// State returns the current circuit state.
func (sm *StateMachine) State() State {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

// Generation returns the current generation number. It is incremented on
// every state transition.
func (sm *StateMachine) Generation() uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.generation
}

// AllowRequest reports whether the next request may proceed and returns the
// generation the request belongs to. If the circuit is Open and
// RecoveryTimeout has elapsed, it first transitions to HalfOpen and reports
// transitioned as true. It returns ErrOpenState while the circuit is Open and
// ErrTooManyRequests once HalfOpenMaxRequests probes have been admitted in
// the current half-open period.
func (sm *StateMachine) AllowRequest() (generation uint64, transitioned bool, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state == StateOpen && sm.now().Sub(sm.openedAt) >= sm.cfg.RecoveryTimeout {
		sm.setState(StateHalfOpen)
		transitioned = true
	}
	switch sm.state {
	case StateOpen:
		return sm.generation, transitioned, ErrOpenState
	case StateHalfOpen:
		if sm.halfOpenRequests >= sm.cfg.HalfOpenMaxRequests {
			return sm.generation, transitioned, ErrTooManyRequests
		}
		sm.halfOpenRequests++
	}
	return sm.generation, transitioned, nil
}

// EvaluateClosed transitions the circuit from Closed to Open when failureRate
// reaches thetaT, the result belongs to the current generation, and
// MinDwellTime has elapsed since the last transition. It reports whether the
// transition occurred.
func (sm *StateMachine) EvaluateClosed(generation uint64, failureRate, thetaT float64) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state != StateClosed || generation != sm.generation || failureRate < thetaT {
		return false
	}
	if sm.now().Sub(sm.lastTransition) < sm.cfg.MinDwellTime {
		return false
	}
	sm.openedAt = sm.now()
	sm.setState(StateOpen)
	return true
}

// OnHalfOpenSuccess records a successful probe from the given generation.
// Once HalfOpenMaxRequests probes have succeeded, the circuit transitions to
// Closed. It reports whether the transition occurred.
func (sm *StateMachine) OnHalfOpenSuccess(generation uint64) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state != StateHalfOpen || generation != sm.generation {
		return false
	}
	sm.halfOpenSuccesses++
	if sm.halfOpenSuccesses < sm.cfg.HalfOpenMaxRequests {
		return false
	}
	sm.setState(StateClosed)
	return true
}

// OnHalfOpenFailure records a failed probe from the given generation and
// transitions the circuit back to Open. It reports whether the transition
// occurred.
func (sm *StateMachine) OnHalfOpenFailure(generation uint64) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state != StateHalfOpen || generation != sm.generation {
		return false
	}
	sm.openedAt = sm.now()
	sm.setState(StateOpen)
	return true
}

// setState updates the circuit state, starts a new generation, records the
// transition timestamp, and clears the half-open probe counters.
// It must be called with sm.mu held.
func (sm *StateMachine) setState(next State) {
	sm.state = next
	sm.generation++
	sm.lastTransition = sm.now()
	sm.halfOpenRequests = 0
	sm.halfOpenSuccesses = 0
}
