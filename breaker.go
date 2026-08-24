// breaker.go implements CircuitBreaker, the main public API of fluxgo.
// It wires all internal components into a single thread-safe entry point
// that is API-compatible with gobreaker.
package fluxgo

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrOpenState is returned by Execute when the circuit breaker is open and
// the request is rejected without being forwarded to the downstream
var ErrOpenState = errors.New("circuit breaker is open")

// Settings configures a CircuitBreaker instance
type Settings struct {
	// Name identifies this circuit breaker instance in logs and callbacks
	Name string

	// Config holds all tunable parameters. Use DefaultConfig() as a starting point
	Config Config

	// OnStateChange is an optional callback invoked whenever the circuit transitions
	// between states. It receives the breaker name, the previous state, and new state.
	OnStateChange func(name string, from State, to State)
}

// Counts holds a point-in-time snapshot of request statistics.
// The field names and types are compatible with gobreaker.
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures	 uint32
}

// CircuitBreaker is the main entry point of fluxgo. It orchestrates all
// internal components (EWMA, RollingPercentile, MetricCollector,
// BaselineEstimator, AdaptiveThresholdCalculator, StateMachine) behind
// an API compatible with gobreaker.
type CircuitBreaker struct {
	name	string
	cfg		Config
	onStateChange func(name string, from State, to State)

	ewma	*EWMA
	rolling *RollingPercentile
	collector	*MetricCollector
	estimator	*BaselineEstimator
	calculator	*AdaptiveThresholdCalculator
	machine		*StateMachine

	consecutiveSuccesses int64
	consecutiveFailures	int64
}

// NewCircuitBreaker returns a CircuitBreaker wired and ready to use.
func NewCircuitBreaker(s Settings) *CircuitBreaker {
    cfg := s.Config
    ewma    := NewEWMA(cfg.Alpha)
    rolling := NewRollingPercentile(cfg.WindowSize)
    return &CircuitBreaker{
        name:          s.Name,
        cfg:           cfg,
        onStateChange: s.OnStateChange,
        ewma:          ewma,
        rolling:       rolling,
        collector:     NewMetricCollector(ewma, rolling),
        estimator:     NewBaselineEstimator(cfg),
        calculator:    NewAdaptiveThresholdCalculator(cfg),
        machine:       NewStateMachine(cfg),
    }
}

// Execute runs the given request if the circuit breaker allows it.
// It returns ErrOpenState if the circuit is open. The signature is
// compatible with gobreaker.
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error){
	before := cb.machine.State()

	if !cb.machine.AllowRequest(){
		return nil, ErrOpenState
	}

	// AllowRequest may have triggered Open to HalfOpen
	cb.notifyIfChanged(before, cb.machine.State())
	before = cb.machine.State()

	start := time.Now()
	result, err := req()
	latency := time.Since(start)

	cb.onRequestComplete(latency, err == nil)
	cb.notifyIfChanged(before, cb.machine.State())

	return result, err
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() State {
	return cb.machine.State()
}

// Counts returns a snapshot of the request counter
func (cb *CircuitBreaker) Counts() Counts {
	total	:= cb.collector.TotalRequests()
	failures	:= cb.collector.TotalFailures()
	return Counts{
		Requests:			uint32(total),
		TotalSuccesses: 	uint32(total - failures),
		TotalFailures: 		uint32(failures),
		ConsecutiveSuccesses: 	uint32(atomic.LoadInt64(&cb.consecutiveSuccesses)),
		ConsecutiveFailures: 	uint32(atomic.LoadInt64(&cb.consecutiveFailures)),
	}
}

// onRequestComplete orchestrates the post-request desicion pipeline:
// record metrics, attempt baseline capture if warming up, or evaluate
// the adaptive threshold and notify the state machine
func (cb *CircuitBreaker) onRequestComplete(latency time.Duration, success bool) {
	cb.collector.Record(latency, success)

	if success {
		atomic.StoreInt64(&cb.consecutiveFailures, 0)
		atomic.AddInt64(&cb.consecutiveSuccesses, 1)
	} else {
		atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
		atomic.AddInt64(&cb.consecutiveFailures, 1)
	}

	St := cb.ewma.Value()
	p95, p99 := cb.rolling.Percentiles()
	var Pt float64
	if cb.cfg.PercentileTarget == 99 {
		Pt = p99
	} else {
		Pt = p95
	}
	fr := cb.collector.FailureRate()

	if cb.estimator.IsWarmingUp() {
        cb.estimator.TryCapture(St, Pt, fr)
        return
    }
    Sbase, Pbase := cb.estimator.Baseline()
    thetaT := cb.calculator.Calculate(St, Pt, Sbase, Pbase)
    state := cb.machine.State()
    switch {
    case state == StateHalfOpen && success:
        cb.machine.OnHalfOpenSuccess()
    case state == StateHalfOpen && !success:
        cb.machine.OnHalfOpenFailure()
    case state == StateClosed:
        cb.machine.EvaluateClosed(fr, thetaT)
    }
}

// notifyIfChanged fires the OnStateChange callback if the state changed.
func (cb *CircuitBreaker) notifyIfChanged(from, to State) {
    if cb.onStateChange != nil && from != to {
        cb.onStateChange(cb.name, from, to)
    }
}