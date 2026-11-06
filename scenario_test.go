// scenario_test.go runs CircuitBreaker through complete traffic scenarios on a
// fake clock, as a regression suite for the behavior measured.
package fluxgo

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// requestGap is the fake time between scenario requests (10 requests/second).
const requestGap = 100 * time.Millisecond

// scenarioRequest returns the latency and outcome of the i-th request.
type scenarioRequest func(i int) (latency time.Duration, success bool)

// scenario drives a CircuitBreaker with synthetic traffic and records its
// state transitions.
type scenario struct {
	cb          *CircuitBreaker
	clock       *fakeClock
	transitions []string
}

func newScenario(cfg Config) *scenario {
	s := &scenario{clock: newFakeClock()}
	s.cb = newCircuitBreakerWithClock(Settings{
		Name:   "scenario",
		Config: cfg,
		OnStateChange: func(_ string, from, to State) {
			s.transitions = append(s.transitions, from.String()+"->"+to.String())
		},
	}, s.clock.Now)
	return s
}

// drive sends n requests, one every requestGap, and returns how many of them
// the circuit rejected.
func (s *scenario) drive(n int, request scenarioRequest) (rejected int) {
	for i := 0; i < n; i++ {
		latency, success := request(i)
		if executeWithLatency(s.cb, latency, success) != nil {
			rejected++
		}
		s.clock.Advance(requestGap)
	}
	return rejected
}

// driveUntilOpen sends at most limit requests and returns how many were sent
// when the circuit opened, or 0 if it stayed closed.
func (s *scenario) driveUntilOpen(limit int, request scenarioRequest) int {
	for i := 0; i < limit; i++ {
		latency, success := request(i)
		executeWithLatency(s.cb, latency, success)
		s.clock.Advance(requestGap)
		if s.cb.State() == StateOpen {
			return i + 1
		}
	}
	return 0
}

// warmUp sends 40 seconds of healthy traffic at 10 to 12ms, enough to capture
// a valid baseline.
func (s *scenario) warmUp(t *testing.T) {
	t.Helper()
	s.drive(400, healthyRequest)
	if s.cb.estimator.NeedsCapture() {
		t.Fatal("expected a valid baseline after warm-up")
	}
}

func healthyRequest(i int) (time.Duration, bool) {
	return time.Duration(10+i%3) * time.Millisecond, true
}

func assertTransitions(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("transitions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", got, want)
		}
	}
}

func TestScenario_HealthyTraffic(t *testing.T) {
	cfg := DefaultConfig()
	s := newScenario(cfg)

	s.warmUp(t)
	s.drive(600, healthyRequest)

	assertTransitions(t, s.transitions)
	snap := s.cb.Snapshot()
	if snap.DegradedRequests != 0 {
		t.Errorf("DegradedRequests = %d, want 0", snap.DegradedRequests)
	}
	if snap.Pbase != 12 {
		t.Errorf("Pbase = %v, want 12", snap.Pbase)
	}
	if snap.ThetaT < 0.5 || snap.ThetaT > 0.7 {
		t.Errorf("ThetaT = %v, want close to ThetaBase %v", snap.ThetaT, cfg.ThetaBase)
	}
}

func TestScenario_GradualDegradationWithErrors(t *testing.T) {
	s := newScenario(DefaultConfig())
	s.warmUp(t)

	// A 20% error rate can never reach the static ThetaBase of 0.6, so only
	// the lowered adaptive threshold can open the circuit.
	ms := 11.0
	sent := s.driveUntilOpen(1000, func(i int) (time.Duration, bool) {
		ms *= 1.01
		return time.Duration(ms * float64(time.Millisecond)), i%5 != 0
	})

	if sent == 0 {
		t.Fatal("expected the adaptive threshold to open the circuit")
	}
	t.Logf("opened after %d requests at %.1fms (%.1fx normal)", sent, ms, ms/11)
	assertTransitions(t, s.transitions, "closed->open")
}

func TestScenario_SuddenFailure(t *testing.T) {
	cfg := DefaultConfig()
	s := newScenario(cfg)
	s.warmUp(t)

	// Failed requests do not feed the latency signals, so the threshold stays
	// near ThetaBase and the window must fill to that failure rate.
	sent := s.driveUntilOpen(cfg.WindowSize, func(int) (time.Duration, bool) {
		return 10 * time.Millisecond, false
	})

	if sent == 0 {
		t.Fatal("expected the circuit to open within one window of failures")
	}
	t.Logf("opened after %d failed requests", sent)
	if limit := int(cfg.ThetaMax * float64(cfg.WindowSize)); sent > limit {
		t.Errorf("opened after %d failed requests, want at most %d", sent, limit)
	}
}

func TestScenario_FullFailureAndRecoveryCycle(t *testing.T) {
	cfg := DefaultConfig()
	s := newScenario(cfg)
	s.warmUp(t)
	pbase := s.cb.Snapshot().Pbase

	// Degradation trips the circuit.
	if s.driveUntilOpen(1000, func(i int) (time.Duration, bool) {
		return 100 * time.Millisecond, i%10 >= 3
	}) == 0 {
		t.Fatal("expected the degradation to open the circuit")
	}

	// Requests are rejected while the circuit is open.
	if got := s.drive(10, healthyRequest); got != 10 {
		t.Errorf("rejected %d of 10 requests while open", got)
	}

	// The first probe fails because the downstream is still broken.
	s.clock.Advance(cfg.RecoveryTimeout)
	s.drive(1, func(int) (time.Duration, bool) { return 100 * time.Millisecond, false })
	if s.cb.State() != StateOpen {
		t.Fatalf("State() = %v, want Open after a failed probe", s.cb.State())
	}

	// The next probe succeeds and the recovered downstream stays closed,
	// despite an error rate that the pre-recovery signals would have tripped on.
	s.clock.Advance(cfg.RecoveryTimeout)
	s.drive(1, healthyRequest)
	s.drive(300, func(i int) (time.Duration, bool) {
		latency, _ := healthyRequest(i)
		return latency, i%10 != 0
	})

	assertTransitions(t, s.transitions,
		"closed->open", "open->half-open", "half-open->open", "open->half-open", "half-open->closed")
	if got := s.cb.Snapshot().Pbase; got != pbase {
		t.Errorf("Pbase = %v, want %v kept across recovery", got, pbase)
	}
}

// TestScenario_ConcurrentFullCycle exercises every transition, including the
// signal reset on recovery, from many goroutines. It is most useful under the
// race detector.
func TestScenario_ConcurrentFullCycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WindowSize = 20
	cfg.MinRequests = 5
	cfg.WarmupDuration = 0
	cfg.MinDwellTime = 0
	cfg.RecoveryTimeout = time.Millisecond
	cfg.HalfOpenMaxRequests = 2
	var recoveries int64
	clock := newFakeClock()
	cb := newCircuitBreakerWithClock(Settings{
		Name:   "concurrent",
		Config: cfg,
		OnStateChange: func(_ string, _, to State) {
			if to == StateClosed {
				atomic.AddInt64(&recoveries, 1)
			}
		},
	}, clock.Now)

	var admitted int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				// Alternating bursts of failures and successes trip and
				// recover the circuit regardless of the real latencies.
				failing := i/100%2 == 0
				clock.Advance(time.Millisecond)
				cb.Execute(func() (interface{}, error) {
					atomic.AddInt64(&admitted, 1)
					if failing {
						return nil, errDownstream
					}
					return nil, nil
				})
				if i%50 == 0 {
					_ = cb.Snapshot()
					_ = cb.Counts()
				}
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt64(&recoveries) == 0 {
		t.Error("expected the traffic to trip and recover the circuit")
	}
	if got, want := int64(cb.Counts().Requests), atomic.LoadInt64(&admitted); got != want {
		t.Errorf("Counts().Requests = %d, want %d admitted requests", got, want)
	}
}
