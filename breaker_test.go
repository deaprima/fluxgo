// breaker_test.go tests CircuitBreaker end-to-end behavior including
// Execute, state transitions, warmup, and the OnStateChange callback.
package fluxgo

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errDownstream = errors.New("downstream failure")

func defaultSettings() Settings {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0            // disable anti-flapping in most tests
	cfg.WarmupDuration = 0          // skip warmup in most tests
	cfg.RecoveryTimeout = time.Hour // prevent accidental Open->HalfOpen
	cfg.MinRequests = 1             // disable the minimum-volume gate in most tests
	return Settings{Name: "test", Config: cfg}
}

func TestCircuitBreaker_Execute_SuccessPassesThrough(t *testing.T) {
	cb := NewCircuitBreaker(defaultSettings())
	result, err := cb.Execute(func() (interface{}, error) {
		return "ok", nil
	})
	if err != nil || result != "ok" {
		t.Errorf("Execute() = (%v, %v), want (ok, nil)", result, err)
	}
}

func TestCircuitBreaker_Execute_ReturnsErrWhenOpen(t *testing.T) {
	s := defaultSettings()
	s.Config.ThetaBase = 0.5
	cb := NewCircuitBreaker(s)

	// drive failure rate above threshold
	for i := 0; i < 10; i++ {
		cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	}

	if cb.State() != StateOpen {
		t.Fatal("expected Open state after sustained failures")
	}

	_, err := cb.Execute(func() (interface{}, error) { return "ok", nil })
	if !errors.Is(err, ErrOpenState) {
		t.Errorf("Execute() error = %v, want ErrOpenState", err)
	}
}

func TestCircuitBreaker_Counts_AccumulateCorrectly(t *testing.T) {
	s := defaultSettings()
	s.Config.ThetaBase = 1.0
	cb := NewCircuitBreaker(s)

	cb.Execute(func() (interface{}, error) { return nil, nil })
	cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	cb.Execute(func() (interface{}, error) { return nil, errDownstream })

	c := cb.Counts()
	if c.Requests != 3 {
		t.Errorf("Counts().Requests = %d, want 3", c.Requests)
	}
	if c.TotalFailures != 2 {
		t.Errorf("Counts().TotalFailures = %d, want 2", c.TotalFailures)
	}
	if c.TotalSuccesses != 1 {
		t.Errorf("Counts().TotalSuccesses = %d, want 1", c.TotalSuccesses)
	}
	if c.ConsecutiveFailures != 2 {
		t.Errorf("Counts().ConsecutiveFailures = %d, want 2", c.ConsecutiveFailures)
	}
}

func TestCircuitBreaker_OnStateChange_Callback(t *testing.T) {
	var transitions []string
	s := defaultSettings()
	s.Config.ThetaBase = 0.5
	s.OnStateChange = func(name string, from, to State) {
		transitions = append(transitions, from.String()+"->"+to.String())
	}
	cb := NewCircuitBreaker(s)

	for i := 0; i < 10; i++ {
		cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	}

	if len(transitions) == 0 {
		t.Error("expected OnStateChange to be called, got no transitions")
	}
	if transitions[0] != "closed->open" {
		t.Errorf("first transition = %q, want %q", transitions[0], "closed->open")
	}
}

func TestCircuitBreaker_ConcurrentExecute_RaceDetector(t *testing.T) {
	cb := NewCircuitBreaker(defaultSettings())
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cb.Execute(func() (interface{}, error) {
				if id%3 == 0 {
					return nil, errDownstream
				}
				return "ok", nil
			})
		}(i)
	}
	wg.Wait()
}

func TestCircuitBreaker_HalfOpenProbe_Success_TransitionsToClosed(t *testing.T) {
	s := defaultSettings()
	s.Config.ThetaBase = 0.5
	s.Config.RecoveryTimeout = time.Millisecond // timeout singkat
	cb := NewCircuitBreaker(s)

	for i := 0; i < 10; i++ {
		cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	}
	if cb.State() != StateOpen {
		t.Fatal("expected Open state")
	}

	time.Sleep(5 * time.Millisecond) // tunggu recovery timeout

	_, err := cb.Execute(func() (interface{}, error) { return "ok", nil })
	if err != nil {
		t.Errorf("Execute() = %v, want nil on HalfOpen probe", err)
	}
	if cb.State() != StateClosed {
		t.Errorf("State() = %v, want Closed after successful probe", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenProbe_Failure_TransitionsToOpen(t *testing.T) {
	s := defaultSettings()
	s.Config.ThetaBase = 0.5
	s.Config.RecoveryTimeout = time.Millisecond
	cb := NewCircuitBreaker(s)

	for i := 0; i < 10; i++ {
		cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	}
	if cb.State() != StateOpen {
		t.Fatal("expected Open state")
	}

	time.Sleep(5 * time.Millisecond)

	cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	if cb.State() != StateOpen {
		t.Errorf("State() = %v, want Open after failed probe", cb.State())
	}
}

// executeWithLatency runs one request through the same admission and
// post-request pipeline as Execute, but with a synthetic latency so that
// latency-sensitive tests do not depend on time.Sleep precision, which varies
// across operating systems and Go versions.
func executeWithLatency(cb *CircuitBreaker, latency time.Duration, success bool) error {
	generation, change, err := cb.machine.AllowRequest()
	if change.occurred() {
		cb.handleTransition(change.from, change.to)
	}
	if err != nil {
		return err
	}
	cb.onRequestComplete(generation, latency, success)
	return nil
}

// newWarmedUpBreaker returns a breaker whose rolling window is filled with
// 10ms requests and whose warm-up has completed with a captured baseline.
func newWarmedUpBreaker(t *testing.T, margin float64) *CircuitBreaker {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SlowRequestMargin = margin
	return newWarmedUpBreakerWithConfig(t, cfg)
}

func newWarmedUpBreakerWithConfig(t *testing.T, cfg Config) *CircuitBreaker {
	t.Helper()
	clock := newFakeClock()
	cfg.WarmupDuration = 50 * time.Millisecond

	cb := newCircuitBreakerWithClock(Settings{Name: "test-slow", Config: cfg}, clock.Now)

	for i := 0; i < 110; i++ {
		executeWithLatency(cb, 10*time.Millisecond, true)
	}
	clock.Advance(cfg.WarmupDuration)
	executeWithLatency(cb, 10*time.Millisecond, true)

	if cb.Snapshot().IsWarmingUp {
		t.Fatal("expected warm-up to be complete")
	}
	return cb
}

func TestCircuitBreaker_SlowRequest_ClassifiedAsDegraded(t *testing.T) {
	cb := newWarmedUpBreaker(t, 2.0)
	degradedBefore := cb.collector.DegradedRequests()

	// 10ms * 2.0 margin = 20ms threshold; 30ms > 20ms → degraded.
	executeWithLatency(cb, 30*time.Millisecond, true)

	degradedAfter := cb.collector.DegradedRequests()
	if degradedAfter <= degradedBefore {
		t.Errorf("expected DegradedRequests to increase, before=%d after=%d",
			degradedBefore, degradedAfter)
	}
}

func TestCircuitBreaker_SlowRequest_InactiveDuringWarmup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = 10 * time.Second
	cfg.SlowRequestMargin = 2.0
	cb := newCircuitBreakerWithClock(Settings{Name: "test-warmup", Config: cfg}, newFakeClock().Now)

	// Establish a valid Pt so that only the warm-up rule can suppress
	// classification.
	for i := 0; i < 110; i++ {
		executeWithLatency(cb, 10*time.Millisecond, true)
	}
	executeWithLatency(cb, 500*time.Millisecond, true)

	if !cb.Snapshot().IsWarmingUp {
		t.Fatal("expected warm-up to still be active")
	}
	if got := cb.collector.DegradedRequests(); got != 0 {
		t.Errorf("expected DegradedRequests = 0 during warmup, got %d", got)
	}
}

func TestCircuitBreaker_NormalRequest_NotClassifiedAsDegraded(t *testing.T) {
	cb := newWarmedUpBreaker(t, 2.0)
	degradedBefore := cb.collector.DegradedRequests()

	// 10ms * 2.0 margin = 20ms threshold; 15ms < 20ms → normal.
	executeWithLatency(cb, 15*time.Millisecond, true)

	if got := cb.collector.DegradedRequests(); got != degradedBefore {
		t.Errorf("expected DegradedRequests unchanged for normal request, got %d", got)
	}
}

func TestCircuitBreaker_Snapshot_FieldsConsistent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = 50 * time.Millisecond
	cb := NewCircuitBreaker(Settings{Name: "snap-test", Config: cfg})

	// Populate some data.
	for i := 0; i < 20; i++ {
		cb.Execute(func() (interface{}, error) {
			time.Sleep(5 * time.Millisecond)
			return nil, nil
		})
	}

	snap := cb.Snapshot()

	if snap.State != StateClosed {
		t.Errorf("expected State = Closed, got %v", snap.State)
	}
	if snap.St < 0 {
		t.Errorf("expected St >= 0, got %v", snap.St)
	}
	if snap.FailureRate < 0 || snap.FailureRate > 1 {
		t.Errorf("expected FailureRate in [0,1], got %v", snap.FailureRate)
	}
	if snap.ThetaT < cfg.ThetaMin || snap.ThetaT > cfg.ThetaMax {
		t.Errorf("expected ThetaT in [%v,%v], got %v", cfg.ThetaMin, cfg.ThetaMax, snap.ThetaT)
	}
}

func TestCircuitBreaker_Snapshot_WarmingUpFlag(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = 10 * time.Second
	cb := NewCircuitBreaker(Settings{Name: "snap-warmup", Config: cfg})

	if snap := cb.Snapshot(); !snap.IsWarmingUp {
		t.Error("expected IsWarmingUp = true immediately after creation")
	}
}

func TestCircuitBreaker_Snapshot_ThreadSafe(t *testing.T) {
	cb := NewCircuitBreaker(Settings{Name: "snap-race", Config: DefaultConfig()})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			cb.Execute(func() (interface{}, error) { return nil, nil })
		}()
		go func() {
			defer wg.Done()
			_ = cb.Snapshot()
		}()
	}
	wg.Wait()
}

func TestCircuitBreaker_IdleBeforeTraffic_CapturesBaseline(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cb := newCircuitBreakerWithClock(Settings{Name: "test", Config: cfg}, clock.Now)
	ok := func() (interface{}, error) {
		time.Sleep(time.Millisecond)
		return nil, nil
	}

	clock.Advance(10 * time.Minute)
	for i := 0; i < cfg.WindowSize; i++ {
		cb.Execute(ok)
	}
	if !cb.Snapshot().IsWarmingUp {
		t.Fatal("expected warm-up to start at the first request")
	}

	clock.Advance(cfg.WarmupDuration)
	cb.Execute(ok)

	snap := cb.Snapshot()
	if snap.IsWarmingUp {
		t.Fatal("expected warm-up to be complete")
	}
	if snap.Sbase <= 0 || snap.Pbase <= 0 {
		t.Fatalf("expected baseline to be captured, got Sbase=%v Pbase=%v", snap.Sbase, snap.Pbase)
	}
}

func TestCircuitBreaker_BaselineNotCapturedFromTinySample(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cb := newCircuitBreakerWithClock(Settings{Name: "test", Config: cfg}, clock.Now)

	// Two requests spanning the warm-up period, the second one a latency
	// spike, must not become the baseline.
	executeWithLatency(cb, 10*time.Millisecond, true)
	clock.Advance(cfg.WarmupDuration + time.Second)
	executeWithLatency(cb, 500*time.Millisecond, true)

	snap := cb.Snapshot()
	if !snap.IsWarmingUp {
		t.Fatalf("expected warm-up to continue, got Sbase=%v Pbase=%v", snap.Sbase, snap.Pbase)
	}
}

func TestCircuitBreaker_WarmupExtendsUntilRollingWindowIsFull(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cb := newCircuitBreakerWithClock(Settings{Name: "test", Config: cfg}, clock.Now)

	executeWithLatency(cb, 10*time.Millisecond, true)
	clock.Advance(cfg.WarmupDuration)
	for i := 1; i < cfg.WindowSize-1; i++ {
		executeWithLatency(cb, 10*time.Millisecond, true)
	}
	if !cb.Snapshot().IsWarmingUp {
		t.Fatal("expected warm-up to continue while the rolling window is not full")
	}

	executeWithLatency(cb, 10*time.Millisecond, true)
	snap := cb.Snapshot()
	if snap.IsWarmingUp || snap.Pbase != 10 {
		t.Fatalf("expected a valid baseline once the window is full, got IsWarmingUp=%v Pbase=%v",
			snap.IsWarmingUp, snap.Pbase)
	}
}

func TestCircuitBreaker_FailureRateIsWindowed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = 0
	cfg.MinDwellTime = 0
	cb := NewCircuitBreaker(Settings{Name: "test", Config: cfg})
	ok := func() (interface{}, error) { return nil, nil }
	fail := func() (interface{}, error) { return nil, errors.New("downstream failure") }

	for i := 0; i < 1000; i++ {
		cb.Execute(ok)
	}
	for i := 0; i < cfg.WindowSize && cb.State() == StateClosed; i++ {
		cb.Execute(fail)
	}

	if cb.State() != StateOpen {
		t.Fatal("expected the circuit to open within one window of failures")
	}
}

func TestCircuitBreaker_MinRequestsPreventsEarlyTrip(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = 0
	cfg.MinDwellTime = 0
	cb := NewCircuitBreaker(Settings{Name: "test", Config: cfg})
	fail := func() (interface{}, error) { return nil, errors.New("downstream failure") }

	for i := 0; i < cfg.MinRequests-1; i++ {
		cb.Execute(fail)
	}
	if cb.State() != StateClosed {
		t.Fatal("expected the circuit to stay closed below MinRequests")
	}

	cb.Execute(fail)
	if cb.State() != StateOpen {
		t.Fatal("expected the circuit to open once MinRequests is reached")
	}
}

func TestCircuitBreaker_CanTripDuringWarmup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = time.Hour
	cfg.MinDwellTime = 0
	cb := NewCircuitBreaker(Settings{Name: "test", Config: cfg})
	fail := func() (interface{}, error) { return nil, errors.New("downstream failure") }

	for i := 0; i < cfg.MinRequests; i++ {
		cb.Execute(fail)
	}

	if cb.State() != StateOpen {
		t.Fatal("expected the circuit to trip on ThetaBase during warm-up")
	}
	if !cb.Snapshot().IsWarmingUp {
		t.Fatal("expected warm-up to still be active")
	}
}

func TestCircuitBreaker_WindowResetsOnTransition(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cfg.WarmupDuration = 0
	cfg.MinDwellTime = 0
	var transitions int
	cb := newCircuitBreakerWithClock(Settings{
		Name:          "test",
		Config:        cfg,
		OnStateChange: func(string, State, State) { transitions++ },
	}, clock.Now)
	ok := func() (interface{}, error) { return nil, nil }
	fail := func() (interface{}, error) { return nil, errors.New("downstream failure") }

	for i := 0; i < cfg.MinRequests; i++ {
		cb.Execute(fail)
	}
	clock.Advance(cfg.RecoveryTimeout)
	cb.Execute(ok)
	if cb.State() != StateClosed {
		t.Fatalf("expected closed after a successful probe, got %v", cb.State())
	}

	cb.Execute(fail)

	if cb.State() != StateClosed {
		t.Fatal("expected failures from before the trip to be discarded")
	}
	if transitions != 3 {
		t.Fatalf("expected 3 transitions (closed-open, open-half-open, half-open-closed), got %d", transitions)
	}
}

func TestCircuitBreaker_HalfOpen_RejectsExcessProbes(t *testing.T) {
	clock := newFakeClock()
	s := defaultSettings()
	cb := newCircuitBreakerWithClock(s, clock.Now)

	cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	if cb.State() != StateOpen {
		t.Fatal("expected Open state")
	}
	clock.Advance(s.Config.RecoveryTimeout)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		cb.Execute(func() (interface{}, error) {
			close(started)
			<-release
			return "ok", nil
		})
	}()
	<-started

	_, err := cb.Execute(func() (interface{}, error) { return "ok", nil })
	if !errors.Is(err, ErrTooManyRequests) {
		t.Errorf("Execute() error = %v, want ErrTooManyRequests while a probe is in flight", err)
	}

	close(release)
	<-done
	if cb.State() != StateClosed {
		t.Errorf("State() = %v, want Closed after the probe succeeds", cb.State())
	}
}

func TestCircuitBreaker_PanicRecordedAsFailure(t *testing.T) {
	cb := NewCircuitBreaker(defaultSettings())

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the panic to propagate to the caller")
			}
		}()
		cb.Execute(func() (interface{}, error) { panic("boom") })
	}()

	if got := cb.Counts().TotalFailures; got != 1 {
		t.Errorf("Counts().TotalFailures = %d, want 1 after a panic", got)
	}
	if cb.State() != StateOpen {
		t.Errorf("State() = %v, want Open after a panicking request", cb.State())
	}
}

func TestCircuitBreaker_FailedRequestLatencyNotRecorded(t *testing.T) {
	s := defaultSettings()
	s.Config.ThetaBase = 1.0
	cb := NewCircuitBreaker(s)

	cb.Execute(func() (interface{}, error) {
		time.Sleep(5 * time.Millisecond)
		return nil, nil
	})
	before := cb.Snapshot().St

	for i := 0; i < 3; i++ {
		cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	}

	if after := cb.Snapshot().St; after != before {
		t.Errorf("St changed from %v to %v; failed requests must not feed latency signals", before, after)
	}
}

func TestCircuitBreaker_StaleResultIgnoredAfterTransition(t *testing.T) {
	clock := newFakeClock()
	s := defaultSettings()
	cb := newCircuitBreakerWithClock(s, clock.Now)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		cb.Execute(func() (interface{}, error) {
			close(started)
			<-release
			return nil, errDownstream
		})
	}()
	<-started

	cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	clock.Advance(s.Config.RecoveryTimeout)
	cb.Execute(func() (interface{}, error) { return "ok", nil })
	if cb.State() != StateClosed {
		t.Fatalf("State() = %v, want Closed after a successful probe", cb.State())
	}

	close(release)
	<-done

	if cb.State() != StateClosed {
		t.Errorf("State() = %v, want Closed; a stale failure must not trip the circuit", cb.State())
	}
	if _, n := cb.collector.WindowStats(); n != 0 {
		t.Errorf("window count = %d, want 0; a stale outcome must be discarded", n)
	}
}

func newRecoveredBreaker(t *testing.T) *CircuitBreaker {
	t.Helper()
	clock := newFakeClock()
	cfg := DefaultConfig()
	cb := newCircuitBreakerWithClock(Settings{Name: "test-recovery", Config: cfg}, clock.Now)

	executeWithLatency(cb, 10*time.Millisecond, true)
	clock.Advance(cfg.WarmupDuration)
	for i := 0; i < cfg.WindowSize; i++ {
		executeWithLatency(cb, 10*time.Millisecond, true)
	}
	if cb.Snapshot().IsWarmingUp {
		t.Fatal("expected a captured baseline")
	}

	for i := 0; cb.State() == StateClosed; i++ {
		if i == 1000 {
			t.Fatal("expected the degradation to trip the circuit")
		}
		executeWithLatency(cb, 100*time.Millisecond, i%10 >= 3)
	}

	clock.Advance(cfg.RecoveryTimeout)
	executeWithLatency(cb, 10*time.Millisecond, true)
	if cb.State() != StateClosed {
		t.Fatalf("expected Closed after a successful probe, got %v", cb.State())
	}
	clock.Advance(cfg.MinDwellTime)
	return cb
}

func TestCircuitBreaker_Recovery_ResetsSignalsAndKeepsBaseline(t *testing.T) {
	cb := newRecoveredBreaker(t)

	snap := cb.Snapshot()
	if snap.St != 0 || snap.Pt != 0 {
		t.Errorf("expected latency signals to be reset, got St=%v Pt=%v", snap.St, snap.Pt)
	}
	if snap.Sbase != 10 || snap.Pbase != 10 {
		t.Errorf("expected the baseline to be kept, got Sbase=%v Pbase=%v", snap.Sbase, snap.Pbase)
	}
	if snap.ThetaT != cb.cfg.ThetaBase {
		t.Errorf("expected ThetaT = ThetaBase while signals refill, got %v", snap.ThetaT)
	}
}

func TestCircuitBreaker_Recovery_NoFalseRetrip(t *testing.T) {
	cb := newRecoveredBreaker(t)

	// The downstream is back at its baseline latency, with an error rate
	// well below ThetaBase.
	for i := 0; i < 300; i++ {
		executeWithLatency(cb, 10*time.Millisecond, i%10 != 0)
	}

	if cb.State() != StateClosed {
		t.Fatalf("expected the recovered circuit to stay closed, got %v", cb.State())
	}
}

func TestCircuitBreaker_Recovery_RetripsWhenStillFailing(t *testing.T) {
	cb := newRecoveredBreaker(t)

	for i := 0; i < 300 && cb.State() == StateClosed; i++ {
		executeWithLatency(cb, 10*time.Millisecond, i%10 >= 7)
	}

	if cb.State() != StateOpen {
		t.Fatal("expected the circuit to trip again on a 70% error rate")
	}
}

func TestCircuitBreaker_Recovery_SlowRequestInactiveWhileSignalsRefill(t *testing.T) {
	cb := newRecoveredBreaker(t)
	before := cb.collector.DegradedRequests()

	// 30ms is above 10ms * 2.0 margin, but only one latency has been seen
	// since the reset.
	executeWithLatency(cb, 10*time.Millisecond, true)
	executeWithLatency(cb, 30*time.Millisecond, true)

	if got := cb.collector.DegradedRequests(); got != before {
		t.Errorf("expected no slow request classification while signals refill, got %d new", got-before)
	}
}

func TestCircuitBreaker_StaleLatencyIgnoredAfterRecovery(t *testing.T) {
	clock := newFakeClock()
	s := defaultSettings()
	cb := newCircuitBreakerWithClock(s, clock.Now)

	// A request admitted before the trip that is still in flight.
	staleGeneration, _, _ := cb.machine.AllowRequest()

	executeWithLatency(cb, 10*time.Millisecond, false)
	clock.Advance(s.Config.RecoveryTimeout)
	executeWithLatency(cb, 10*time.Millisecond, true)
	if cb.State() != StateClosed {
		t.Fatalf("State() = %v, want Closed after a successful probe", cb.State())
	}

	cb.onRequestComplete(staleGeneration, 2*time.Second, true)

	if st := cb.Snapshot().St; st != 0 {
		t.Errorf("St = %v, want 0; a stale latency must not feed the reset signals", st)
	}
}

func rampLatency(cb *CircuitBreaker) time.Duration {
	for ms := 10.0; ms <= 1480; ms *= 1.02 {
		latency := time.Duration(ms * float64(time.Millisecond))
		executeWithLatency(cb, latency, true)
		if cb.State() == StateOpen {
			return latency
		}
	}
	return 0
}

func TestCircuitBreaker_SlowRefRecent_MissesGradualDegradation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	cb := newWarmedUpBreakerWithConfig(t, cfg)

	if latency := rampLatency(cb); latency != 0 {
		t.Fatalf("expected SlowRefRecent to stay closed on a gradual ramp, opened at %v", latency)
	}
	if got := cb.collector.DegradedRequests(); got != 0 {
		t.Errorf("expected no slow requests, got %d", got)
	}
}

func TestCircuitBreaker_SlowRefBaseline_DetectsGradualDegradation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	cfg.SlowRequestReference = SlowRefBaseline
	cb := newWarmedUpBreakerWithConfig(t, cfg)

	latency := rampLatency(cb)
	if latency == 0 {
		t.Fatal("expected SlowRefBaseline to open the circuit on a gradual ramp")
	}
	t.Logf("opened at %v (%.1fx the 10ms baseline)", latency, latency.Seconds()*1000/10)
	if latency > 50*time.Millisecond {
		t.Errorf("expected the circuit to open below 5x the baseline, opened at %v", latency)
	}
}

func TestCircuitBreaker_SlowRefBaseline_ShortSpikeDoesNotTrip(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDwellTime = 0
	cfg.SlowRequestReference = SlowRefBaseline
	cb := newWarmedUpBreakerWithConfig(t, cfg)

	// A 2.5x spike shorter than MinRequests requests, then back to normal.
	for i := 0; i < cfg.MinRequests-1; i++ {
		executeWithLatency(cb, 25*time.Millisecond, true)
	}
	for i := 0; i < cfg.WindowSize; i++ {
		executeWithLatency(cb, 10*time.Millisecond, true)
	}

	if cb.State() != StateClosed {
		t.Fatalf("expected a short spike not to trip the circuit, got %v", cb.State())
	}
	if got := cb.collector.DegradedRequests(); got != int64(cfg.MinRequests-1) {
		t.Errorf("expected every spike request to be classified as slow, got %d", got)
	}
}

func TestCircuitBreaker_SlowRefBaseline_UsesRecentWithoutValidBaseline(t *testing.T) {
	clock := newFakeClock()
	cfg := DefaultConfig()
	cfg.SlowRequestReference = SlowRefBaseline
	cb := newCircuitBreakerWithClock(Settings{Name: "test", Config: cfg}, clock.Now)

	// Warm up at 100ms with 10% errors, so only a fallback baseline is set.
	executeWithLatency(cb, 100*time.Millisecond, true)
	clock.Advance(cfg.WarmupDuration)
	for i := 1; i < 120; i++ {
		executeWithLatency(cb, 100*time.Millisecond, i%10 != 0)
	}
	if cb.Snapshot().IsWarmingUp || !cb.estimator.NeedsCapture() {
		t.Fatal("expected a fallback baseline")
	}

	// Latency drops to 10ms while errors persist, so no valid capture occurs.
	for i := 0; i < 120; i++ {
		executeWithLatency(cb, 10*time.Millisecond, i%10 != 0)
	}
	before := cb.collector.DegradedRequests()

	// 50ms is below the fallback Pbase × margin (200ms) but above the recent
	// Pt × margin (20ms).
	executeWithLatency(cb, 50*time.Millisecond, true)

	if got := cb.collector.DegradedRequests(); got != before+1 {
		t.Errorf("expected classification against the recent percentile, got %d new slow requests", got-before)
	}
}

func TestCircuitBreaker_HalfOpen_StuckProbeReopensAfterRecoveryTimeout(t *testing.T) {
	clock := newFakeClock()
	s := defaultSettings()
	var transitions []string
	s.OnStateChange = func(_ string, from, to State) {
		transitions = append(transitions, from.String()+"->"+to.String())
	}
	cb := newCircuitBreakerWithClock(s, clock.Now)

	cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	clock.Advance(s.Config.RecoveryTimeout)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		cb.Execute(func() (interface{}, error) {
			close(started)
			<-release
			return "ok", nil
		})
	}()
	<-started

	clock.Advance(s.Config.RecoveryTimeout)
	if _, err := cb.Execute(func() (interface{}, error) { return "ok", nil }); !errors.Is(err, ErrOpenState) {
		t.Fatalf("Execute() error = %v, want ErrOpenState once the probe has hung for RecoveryTimeout", err)
	}

	clock.Advance(s.Config.RecoveryTimeout)
	if _, err := cb.Execute(func() (interface{}, error) { return "ok", nil }); err != nil {
		t.Fatalf("Execute() error = %v, want a new probe to be admitted", err)
	}
	close(release)
	<-done

	want := []string{"closed->open", "open->half-open", "half-open->open", "open->half-open", "half-open->closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", transitions, want)
		}
	}
}

func TestCircuitBreaker_IsSuccessful_IgnoresSelectedErrors(t *testing.T) {
	s := defaultSettings()
	s.IsSuccessful = func(err error) bool {
		return err == nil || errors.Is(err, context.Canceled)
	}
	cb := NewCircuitBreaker(s)

	for i := 0; i < 50; i++ {
		_, err := cb.Execute(func() (interface{}, error) { return nil, context.Canceled })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want the original error returned to the caller", err)
		}
	}

	if cb.State() != StateClosed {
		t.Errorf("State() = %v, want Closed when every error is counted as a success", cb.State())
	}
	if got := cb.Counts().TotalFailures; got != 0 {
		t.Errorf("Counts().TotalFailures = %d, want 0", got)
	}
}

func TestCircuitBreaker_IsSuccessful_CountsOtherErrorsAsFailures(t *testing.T) {
	s := defaultSettings()
	s.IsSuccessful = func(err error) bool {
		return err == nil || errors.Is(err, context.Canceled)
	}
	cb := NewCircuitBreaker(s)

	cb.Execute(func() (interface{}, error) { return nil, errDownstream })

	if cb.State() != StateOpen {
		t.Errorf("State() = %v, want Open after a downstream failure", cb.State())
	}
}

func TestCircuitBreaker_Counts_SaturateAtMaxUint32(t *testing.T) {
	cb := NewCircuitBreaker(defaultSettings())
	atomic.StoreInt64(&cb.collector.totalRequests, math.MaxUint32+10)
	atomic.StoreInt64(&cb.collector.totalFailures, 5)

	c := cb.Counts()
	if c.Requests != math.MaxUint32 || c.TotalSuccesses != math.MaxUint32 || c.TotalFailures != 5 {
		t.Errorf("Counts() = %+v, want Requests and TotalSuccesses capped at MaxUint32, TotalFailures = 5", c)
	}
}

func TestCircuitBreaker_GoexitRecordedAsFailure(t *testing.T) {
	clock := newFakeClock()
	s := defaultSettings()
	cb := newCircuitBreakerWithClock(s, clock.Now)

	cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	clock.Advance(s.Config.RecoveryTimeout)

	done := make(chan struct{})
	go func() {
		defer close(done)
		cb.Execute(func() (interface{}, error) {
			runtime.Goexit()
			return nil, nil
		})
	}()
	<-done

	if cb.State() != StateOpen {
		t.Errorf("State() = %v, want Open after the probe exited via runtime.Goexit", cb.State())
	}
	if got := cb.Counts().TotalFailures; got != 2 {
		t.Errorf("Counts().TotalFailures = %d, want 2", got)
	}
}
