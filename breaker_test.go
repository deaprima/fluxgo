// breaker_test.go tests CircuitBreaker end-to-end behavior including
// Execute, state transitions, warmup, and the OnStateChange callback.
package fluxgo

import (
    "errors"
    "sync"
    "testing"
    "time"
)

var errDownstream = errors.New("downstream failure")

func defaultSettings() Settings {
    cfg := DefaultConfig()
    cfg.MinDwellTime = 0           // disable anti-flapping in most tests
    cfg.WarmupDuration = 0         // skip warmup in most tests
    cfg.RecoveryTimeout = time.Hour // prevent accidental Open→HalfOpen
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
    cb := NewCircuitBreaker(defaultSettings())

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

func TestCircuitBreaker_WarmupPreventsAdaptiveDecision(t *testing.T) {
    cfg := DefaultConfig()
    cfg.WarmupDuration = 30 * time.Second
    cfg.ThetaBase = 0.1
    cb := NewCircuitBreaker(Settings{Name: "warmup-test", Config: cfg})

    for i := 0; i < 20; i++ {
        cb.Execute(func() (interface{}, error) { return nil, errDownstream })
    }

    if cb.State() != StateClosed {
        t.Errorf("State() = %v, want Closed during warmup", cb.State())
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

func TestCircuitBreaker_SlowRequest_ClassifiedAsDegraded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = 50 * time.Millisecond
	cfg.SlowRequestMargin = 2.0
	cb := NewCircuitBreaker(Settings{Name: "test-slow", Config: cfg})

	// Fill rolling window with 10ms baseline requests to establish Pt.
	for i := 0; i < 110; i++ {
		cb.Execute(func() (interface{}, error) {
			time.Sleep(10 * time.Millisecond)
			return nil, nil
		})
	}

	// Wait for warm-up to finish.
	time.Sleep(60 * time.Millisecond)

	degradedBefore := cb.collector.DegradedRequests()

	// Send a slow request: 10ms * 2.0 margin = 20ms threshold; 30ms > 20ms → degraded.
	cb.Execute(func() (interface{}, error) {
		time.Sleep(30 * time.Millisecond)
		return nil, nil
	})

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
	cb := NewCircuitBreaker(Settings{Name: "test-warmup", Config: cfg})

	cb.Execute(func() (interface{}, error) {
		time.Sleep(500 * time.Millisecond)
		return nil, nil
	})

	if got := cb.collector.DegradedRequests(); got != 0 {
		t.Errorf("expected DegradedRequests = 0 during warmup, got %d", got)
	}
}

func TestCircuitBreaker_NormalRequest_NotClassifiedAsDegraded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WarmupDuration = 50 * time.Millisecond
	cfg.SlowRequestMargin = 2.0
	cb := NewCircuitBreaker(Settings{Name: "test-normal", Config: cfg})

	// Establish baseline.
	for i := 0; i < 110; i++ {
		cb.Execute(func() (interface{}, error) {
			time.Sleep(10 * time.Millisecond)
			return nil, nil
		})
	}
	time.Sleep(60 * time.Millisecond)

	degradedBefore := cb.collector.DegradedRequests()

	// Request within 2x margin (10ms baseline, 15ms < 20ms threshold → normal).
	cb.Execute(func() (interface{}, error) {
		time.Sleep(15 * time.Millisecond)
		return nil, nil
	})

	if got := cb.collector.DegradedRequests(); got != degradedBefore {
		t.Errorf("expected DegradedRequests unchanged for normal request, got %d", got)
	}
}
