// breaker.go implements CircuitBreaker, the main public API of fluxgo.
// It wires all internal components into a single thread-safe entry point
// that follows the calling pattern of gobreaker.

package fluxgo

import (
	"errors"
	"math"
	"sync/atomic"
	"time"
)

var (
	// ErrOpenState is returned by Execute when the circuit breaker is open and
	// the request is rejected without being forwarded to the downstream.
	ErrOpenState = errors.New("circuit breaker is open")

	// ErrTooManyRequests is returned by Execute when the circuit breaker is
	// half-open and Config.HalfOpenMaxRequests probes have already been admitted.
	ErrTooManyRequests = errors.New("too many requests")
)

// Settings configures a CircuitBreaker instance.
type Settings struct {
	// Name identifies this circuit breaker instance in logs and callbacks.
	Name string

	// Config holds all tunable parameters. Use DefaultConfig as a starting point.
	Config Config

	// OnStateChange is an optional callback invoked whenever the circuit transitions
	// between states. It receives the breaker name, the previous state, and the new state.
	OnStateChange func(name string, from State, to State)

	// IsSuccessful is an optional function that reports whether the error
	// returned by a request counts as a success. If it is nil, only a nil
	// error counts as a success. It matches gobreaker's Settings.IsSuccessful
	// and can be used to ignore errors that do not indicate a downstream
	// failure, such as context.Canceled.
	IsSuccessful func(err error) bool
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

// Snapshot is a point-in-time read-only view of the circuit breaker's internal signals.
type Snapshot struct {
	State            State
	St               float64 // current EWMA latency
	Pt               float64 // current rolling percentile
	Dt               float64 // degradation ratio
	ThetaT           float64 // current adaptive threshold, applied to the error rate
	Sbase            float64 // healthy-state EWMA baseline
	Pbase            float64 // healthy-state percentile baseline
	FailureRate      float64 // current failure rate in the outcome window
	DegradedRequests int64   // requests classified as slow
	IsWarmingUp      bool    // true if no baseline has been set yet
}

// CircuitBreaker is the main entry point of fluxgo. It orchestrates all
// internal components (EWMA, RollingPercentile, MetricCollector,
// BaselineEstimator, AdaptiveThresholdCalculator, StateMachine).
type CircuitBreaker struct {
	name          string
	cfg           Config
	onStateChange func(name string, from State, to State)
	isSuccessful  func(err error) bool

	ewma       *EWMA
	rolling    *RollingPercentile
	collector  *MetricCollector
	estimator  *BaselineEstimator
	calculator *AdaptiveThresholdCalculator
	machine    *StateMachine

	consecutiveSuccesses int64
	consecutiveFailures  int64

	// consecutiveErrors counts requests in a row that IsSuccessful reported
	// as failed, ignoring slow classification, for Config.ConsecutiveFailures.
	consecutiveErrors int64

	// now is the clock used for Config.SlowTripDuration.
	now func() time.Time
	// slowTripSince is the time, in Unix nanoseconds, since which the failure
	// rate has reached the threshold only with slow requests counted, or
	// noSlowTrip. slowTripRequests is the lifetime request count at that time.
	slowTripSince    int64
	slowTripRequests int64
}

// noSlowTrip marks slowTripSince as unset.
const noSlowTrip = math.MinInt64

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
	isSuccessful := s.IsSuccessful
	if isSuccessful == nil {
		isSuccessful = func(err error) bool { return err == nil }
	}
	ewma := NewEWMA(cfg.Alpha)
	rolling := NewRollingPercentile(cfg.WindowSize)
	return &CircuitBreaker{
		name:          s.Name,
		cfg:           cfg,
		onStateChange: s.OnStateChange,
		isSuccessful:  isSuccessful,
		ewma:          ewma,
		rolling:       rolling,
		collector:     NewMetricCollector(ewma, rolling, NewOutcomeWindow(cfg.WindowSize)),
		estimator:     newBaselineEstimatorWithClock(cfg, now),
		calculator:    NewAdaptiveThresholdCalculator(cfg),
		machine:       newStateMachineWithClock(cfg, now),
		now:           now,
		slowTripSince: noSlowTrip,
	}
}

// Execute runs the given request if the circuit breaker allows it.
// It returns ErrOpenState if the circuit is open and ErrTooManyRequests if
// the circuit is half-open and the probe limit has been reached. If req
// panics or calls runtime.Goexit, the request is recorded as a failure and
// the panic or exit continues.
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	generation, change, err := cb.machine.AllowRequest()
	if change.occurred() {
		cb.handleTransition(change.from, change.to)
	}
	if err != nil {
		return nil, err
	}

	start := time.Now()
	completed := false
	defer func() {
		if !completed {
			cb.onRequestComplete(generation, time.Since(start), false)
		}
	}()

	result, err := req()
	success := cb.isSuccessful(err)
	completed = true
	cb.onRequestComplete(generation, time.Since(start), success)
	return result, err
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() State {
	return cb.machine.State()
}

// Counts returns a snapshot of the request counters. Unlike gobreaker, the
// totals cover the whole lifetime of the circuit breaker and are not cleared
// on state changes, and TotalFailures includes requests classified as slow.
// Values above math.MaxUint32 are reported as math.MaxUint32.
func (cb *CircuitBreaker) Counts() Counts {
	total := cb.collector.TotalRequests()
	failures := cb.collector.TotalFailures()
	return Counts{
		Requests:             saturate(total),
		TotalSuccesses:       saturate(total - failures),
		TotalFailures:        saturate(failures),
		ConsecutiveSuccesses: saturate(atomic.LoadInt64(&cb.consecutiveSuccesses)),
		ConsecutiveFailures:  saturate(atomic.LoadInt64(&cb.consecutiveFailures)),
	}
}

// Snapshot returns a view of the circuit breaker's internal signals for
// observability purposes (NF4). It is safe to call concurrently. Each field
// is read atomically, but fields are not read under a single lock and may
// reflect slightly different instants under concurrent load.
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
		ThetaT:           cb.threshold(St, Pt, Sbase, Pbase),
		Sbase:            Sbase,
		Pbase:            Pbase,
		FailureRate:      cb.collector.FailureRate(),
		DegradedRequests: cb.collector.DegradedRequests(),
		IsWarmingUp:      cb.estimator.IsWarmingUp(),
	}
}

// onRequestComplete runs the post-request decision pipeline for a request
// admitted in the given generation: classify slow requests, record latency
// and outcome, feed the baseline estimator, and evaluate the state machine
// against the current threshold and the run of consecutive errors. success
// reports whether IsSuccessful accepted the request's error; only latencies
// of such requests feed the latency signals, and only if they were admitted
// in the current generation. A Half-Open probe that fails reopens the
// circuit, and one that succeeds without being slow counts toward closing it;
// one that succeeds but is classified as slow is inconclusive: its slot is
// freed for another probe and the circuit stays Half-Open.
func (cb *CircuitBreaker) onRequestComplete(generation uint64, latency time.Duration, success bool) {
	slow := false

	var p95, p99 float64
	if success && generation == cb.machine.Generation() {
		if limit := cb.slowLimit(); limit > 0 && latency.Seconds()*1000 > limit {
			slow = true
			cb.collector.RecordDegraded()
		}
		p95, p99 = cb.collector.RecordLatency(latency)
	} else {
		p95, p99 = cb.rolling.Current()
	}

	failed := !success || slow
	if slow {
		cb.collector.recordSlowOutcome(generation)
	} else {
		cb.collector.RecordOutcome(generation, success)
	}
	if failed {
		atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
		atomic.AddInt64(&cb.consecutiveFailures, 1)
	} else {
		atomic.StoreInt64(&cb.consecutiveFailures, 0)
		atomic.AddInt64(&cb.consecutiveSuccesses, 1)
	}
	streak := cb.recordErrorStreak(generation, success)

	St := cb.ewma.Value()
	Pt := cb.selectPercentile(p95, p99)
	fr, errorRate, n := cb.collector.windowStats()
	state := cb.machine.State()

	if cb.estimator.NeedsCapture() {
		cb.estimator.Observe(St, Pt, fr, n, cb.rolling.Count(), state == StateClosed)
	}

	Sbase, Pbase := cb.estimator.Baseline()
	thetaT := cb.threshold(St, Pt, Sbase, Pbase)

	switch state {
	case StateHalfOpen:
		switch {
		case !success:
			if cb.machine.OnHalfOpenFailure(generation) {
				cb.handleTransition(StateHalfOpen, StateOpen)
			}
		case slow:
			cb.machine.releaseProbe(generation)
		default:
			if cb.machine.OnHalfOpenSuccess(generation) {
				cb.handleTransition(StateHalfOpen, StateClosed)
			}
		}
	case StateClosed:
		if cb.shouldTrip(n, fr, errorRate, thetaT, streak) && cb.machine.tripClosed(generation) {
			cb.handleTransition(StateClosed, StateOpen)
		}
	}
}

// recordErrorStreak updates the run of consecutive errors with a request
// admitted in the given generation and returns the run's length. Results from
// an earlier generation are ignored, because the run restarts on every
// transition.
func (cb *CircuitBreaker) recordErrorStreak(generation uint64, success bool) int64 {
	if generation != cb.machine.Generation() {
		return 0
	}
	if success {
		atomic.StoreInt64(&cb.consecutiveErrors, 0)
		return 0
	}
	return atomic.AddInt64(&cb.consecutiveErrors, 1)
}

// shouldTrip reports whether a Closed circuit should open. With at least
// MinRequests outcomes in the window, either rule opens it:
//   - the error rate reaches thetaT, the adaptive threshold, which falls as
//     latency rises above the baseline;
//   - the failure rate, with slow requests counted as failures, reaches the
//     threshold at baseline latency (slowThreshold) and has done so for
//     Config.SlowTripDuration. Slow requests already measure latency, so the
//     rising latency does not lower this threshold a second time.
//
// Regardless of the window, streak consecutive errors reaching
// Config.ConsecutiveFailures also open it.
func (cb *CircuitBreaker) shouldTrip(outcomes int, failureRate, errorRate, thetaT float64, streak int64) bool {
	if cb.cfg.ConsecutiveFailures > 0 && streak >= int64(cb.cfg.ConsecutiveFailures) {
		return true
	}
	if outcomes >= cb.cfg.MinRequests && errorRate >= thetaT {
		return true
	}
	if outcomes < cb.cfg.MinRequests || failureRate < cb.slowThreshold() {
		// Load first: on the hot path the timer is almost always unset, and
		// a store on every request would contend across cores.
		if atomic.LoadInt64(&cb.slowTripSince) != noSlowTrip {
			atomic.StoreInt64(&cb.slowTripSince, noSlowTrip)
		}
		return false
	}
	if cb.cfg.SlowTripDuration == 0 {
		return true
	}
	return cb.slowTripSustained()
}

// slowThreshold returns the threshold that the failure rate, with slow
// requests counted, must reach: ThetaBase clamped to [ThetaMin, ThetaMax],
// the adaptive threshold at baseline latency.
func (cb *CircuitBreaker) slowThreshold() float64 {
	return clamp(cb.cfg.ThetaBase, cb.cfg.ThetaMin, cb.cfg.ThetaMax)
}

// slowTripSustained reports whether the failure rate has reached the
// threshold, with slow requests counted, for at least SlowTripDuration and
// over at least WindowSize new outcomes, so that none of the outcomes that
// started the condition are still in the window. It starts timing the
// condition on its first call after a reset.
func (cb *CircuitBreaker) slowTripSustained() bool {
	now := cb.now().UnixNano()
	requests := cb.collector.TotalRequests()
	since := atomic.LoadInt64(&cb.slowTripSince)
	if since == noSlowTrip {
		if atomic.CompareAndSwapInt64(&cb.slowTripSince, noSlowTrip, now) {
			atomic.StoreInt64(&cb.slowTripRequests, requests)
		}
		return false
	}
	return now-since >= int64(cb.cfg.SlowTripDuration) &&
		requests-atomic.LoadInt64(&cb.slowTripRequests) >= int64(cb.cfg.WindowSize)
}

// handleTransition moves the outcome window to the new generation, resets
// the consecutive counters, and fires the OnStateChange callback if one is set.
// On recovery from HalfOpen to Closed it also resets the latency signals, which
// still describe the degradation that tripped the circuit, so the recovered
// downstream is judged on fresh latencies. The baseline is kept.
func (cb *CircuitBreaker) handleTransition(from, to State) {
	cb.collector.AdvanceWindow(cb.machine.Generation())
	if from == StateHalfOpen && to == StateClosed {
		cb.ewma.Reset()
		cb.rolling.Reset()
	}
	atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
	atomic.StoreInt64(&cb.consecutiveFailures, 0)
	atomic.StoreInt64(&cb.consecutiveErrors, 0)
	atomic.StoreInt64(&cb.slowTripSince, noSlowTrip)
	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, from, to)
	}
}

// slowLimit returns the latency in milliseconds above which a successful
// request is classified as slow, or 0 while classification is inactive. The
// limit is taken before the request's own latency enters the rolling window,
// so a request is never compared with itself.
func (cb *CircuitBreaker) slowLimit() float64 {
	if cb.estimator.IsWarmingUp() {
		return 0
	}
	if cb.cfg.SlowRequestReference == SlowRefBaseline && !cb.estimator.NeedsCapture() {
		_, Pbase := cb.estimator.Baseline()
		return Pbase * cb.cfg.SlowRequestMargin
	}
	if !cb.signalsReady() {
		return 0
	}
	return cb.selectPercentile(cb.rolling.Current()) * cb.cfg.SlowRequestMargin
}

// signalsReady reports whether the rolling window holds at least MinRequests
// latencies, enough for the latency signals to be compared with the baseline.
// It is false while the signals refill after being reset on recovery.
func (cb *CircuitBreaker) signalsReady() bool {
	return cb.rolling.Count() >= cb.cfg.MinRequests
}

// threshold returns the adaptive threshold for the given signals, or
// ThetaBase while the signals are not ready.
func (cb *CircuitBreaker) threshold(St, Pt, Sbase, Pbase float64) float64 {
	if !cb.signalsReady() {
		return cb.cfg.ThetaBase
	}
	return cb.calculator.Calculate(St, Pt, Sbase, Pbase)
}

// selectPercentile returns p99 when PercentileTarget is 99 and p95 otherwise.
func (cb *CircuitBreaker) selectPercentile(p95, p99 float64) float64 {
	if cb.cfg.PercentileTarget == 99 {
		return p99
	}
	return p95
}

// saturate converts n to uint32, clamping it to the range [0, math.MaxUint32].
func saturate(n int64) uint32 {
	if n <= 0 {
		return 0
	}
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}
