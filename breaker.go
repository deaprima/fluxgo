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
	ConsecutiveFailures  uint32
}

// Snapshot is a point-in-time read-only view of the circuit breaker's internal signal
type Snapshot struct {
	State            State
	St               float64 // current ewma latency
	Pt               float64 // current rolling percentile
	Dt               float64 // degradation ratio
	ThetaT           float64 // current adaptive threshold
	Sbase            float64 // healthy state ewma baseline
	Pbase            float64 // healthy state percentile baseline
	FailureRate      float64 // current failure rate in observation window
	DegradedRequests int64   // request classified as slow
	IsWarmingUp      bool    // true if baseline has not yet been captured
}

// CircuitBreaker is the main entry point of fluxgo. It orchestrates all
// internal components (EWMA, RollingPercentile, MetricCollector,
// BaselineEstimator, AdaptiveThresholdCalculator, StateMachine) behind
// an API compatible with gobreaker.
type CircuitBreaker struct {
	name          string
	cfg           Config
	onStateChange func(name string, from State, to State)

	ewma       *EWMA
	rolling    *RollingPercentile
	collector  *MetricCollector
	estimator  *BaselineEstimator
	calculator *AdaptiveThresholdCalculator
	machine    *StateMachine

	consecutiveSuccesses int64
	consecutiveFailures  int64
}

// NewCircuitBreaker returns a CircuitBreaker wired and ready to use.
// It panics if s.Config is invalid.
func NewCircuitBreaker(s Settings) *CircuitBreaker {
	return newCircuitBreakerWithClock(s, time.Now)
}

// newCircuitBreakerWithClock returns a CircuitBreaker whose warm-up and state
// machine use the given clock, used by tests to control time without sleeping.
func newCircuitBreakerWithClock(s Settings, now func() time.Time) *CircuitBreaker {
	if err := s.Config.Validate(); err != nil {
		panic("fluxgo: invalid config: " + err.Error())
	}
	cfg := s.Config
	ewma := NewEWMA(cfg.Alpha)
	rolling := NewRollingPercentile(cfg.WindowSize)
	return &CircuitBreaker{
		name:          s.Name,
		cfg:           cfg,
		onStateChange: s.OnStateChange,
		ewma:          ewma,
		rolling:       rolling,
		collector:     NewMetricCollector(ewma, rolling, NewOutcomeWindow(cfg.WindowSize)),
		estimator:     newBaselineEstimatorWithClock(cfg, now),
		calculator:    NewAdaptiveThresholdCalculator(cfg),
		machine:       newStateMachineWithClock(cfg, now),
	}
}

// Execute runs the given request if the circuit breaker allows it.
// It returns ErrOpenState if the circuit is open. The signature is
// compatible with gobreaker.
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	allowed, toHalfOpen := cb.machine.AllowRequest()
	if !allowed {
		return nil, ErrOpenState
	}
	if toHalfOpen {
		cb.handleTransition(StateOpen, StateHalfOpen)
	}

	start := time.Now()
	result, err := req()
	cb.onRequestComplete(time.Since(start), err == nil)

	return result, err
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() State {
	return cb.machine.State()
}

// Counts returns a snapshot of the request counter
func (cb *CircuitBreaker) Counts() Counts {
	total := cb.collector.TotalRequests()
	failures := cb.collector.TotalFailures()
	return Counts{
		Requests:             uint32(total),
		TotalSuccesses:       uint32(total - failures),
		TotalFailures:        uint32(failures),
		ConsecutiveSuccesses: uint32(atomic.LoadInt64(&cb.consecutiveSuccesses)),
		ConsecutiveFailures:  uint32(atomic.LoadInt64(&cb.consecutiveFailures)),
	}
}

// Snapshot returns a point-in-time view of the circuit breaker's internal
// signals for observability purposes (NF4). It is safe to call concurrently.
func (cb *CircuitBreaker) Snapshot() Snapshot {
	St := cb.ewma.Value()
	Pt := cb.selectPercentile(cb.rolling.Current())
	Sbase, Pbase := cb.estimator.Baseline()

	var Dt float64
	if Sbase > 0 && Pbase > 0 {
		Dt = cb.cfg.W1*(St/Sbase) + cb.cfg.W2*(Pt/Pbase)
	}

	return Snapshot{
		State:            cb.machine.State(),
		St:               St,
		Pt:               Pt,
		Dt:               Dt,
		ThetaT:           cb.calculator.Calculate(St, Pt, Sbase, Pbase),
		Sbase:            Sbase,
		Pbase:            Pbase,
		FailureRate:      cb.collector.FailureRate(),
		DegradedRequests: cb.collector.DegradedRequests(),
		IsWarmingUp:      cb.estimator.IsWarmingUp(),
	}
}

// onRequestComplete runs the post-request decision pipeline: classify slow
// requests, record metrics, feed the baseline estimator, and evaluate the
// state machine against the current threshold.
func (cb *CircuitBreaker) onRequestComplete(latency time.Duration, success bool) {
	latencyMs := latency.Seconds() * 1000

	if !cb.estimator.IsWarmingUp() {
		ptPrev := cb.selectPercentile(cb.rolling.Current())
		if ptPrev > 0 && latencyMs > ptPrev*cb.cfg.SlowRequestMargin {
			success = false
			cb.collector.RecordDegraded()
		}
	}

	p95, p99 := cb.collector.Record(latency, success)
	if success {
		atomic.StoreInt64(&cb.consecutiveFailures, 0)
		atomic.AddInt64(&cb.consecutiveSuccesses, 1)
	} else {
		atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
		atomic.AddInt64(&cb.consecutiveFailures, 1)
	}

	St := cb.ewma.Value()
	Pt := cb.selectPercentile(p95, p99)
	fr, n := cb.collector.WindowStats()
	state := cb.machine.State()

	if cb.estimator.NeedsCapture() {
		cb.estimator.Observe(St, Pt, fr, state == StateClosed)
	}

	Sbase, Pbase := cb.estimator.Baseline()
	thetaT := cb.calculator.Calculate(St, Pt, Sbase, Pbase)

	switch state {
	case StateHalfOpen:
		if success {
			if cb.machine.OnHalfOpenSuccess() {
				cb.handleTransition(StateHalfOpen, StateClosed)
			}
		} else if cb.machine.OnHalfOpenFailure() {
			cb.handleTransition(StateHalfOpen, StateOpen)
		}
	case StateClosed:
		if n >= cb.cfg.MinRequests && cb.machine.EvaluateClosed(fr, thetaT) {
			cb.handleTransition(StateClosed, StateOpen)
		}
	}
}

// handleTransition resets the outcome window and consecutive counters after
// a state transition, then fires the OnStateChange callback if one is set.
func (cb *CircuitBreaker) handleTransition(from, to State) {
	cb.collector.ResetWindow()
	atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
	atomic.StoreInt64(&cb.consecutiveFailures, 0)
	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, from, to)
	}
}

// selectPercentile returns p99 when PercentileTarget is 99 and p95 otherwise.
func (cb *CircuitBreaker) selectPercentile(p95, p99 float64) float64 {
	if cb.cfg.PercentileTarget == 99 {
		return p99
	}
	return p95
}