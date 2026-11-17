// testbed_scenario_test.go replays the fault-injection scenarios of
// fluxgo-testbed on a fake clock, so their outcome can be checked in seconds
// before the full experiment runs. The same request stream is fed to fluxgo
// and to a model of the gobreaker configuration used by the testbed.
package fluxgo

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Parameters copied from fluxgo-testbed (runner/orchestrator.py,
// k6/scenarios, internal/client and internal/cb/env_config.go).
const (
	tbClientTimeout = 10 * time.Second // http.Client timeout in the testbed
	tbJitterMs      = 10               // Toxiproxy latency jitter

	// tbMinGap caps the request rate at 1000 requests per second. Without it,
	// 250 closed-loop virtual users at 15ms would send about 16,000 requests per
	// second, which the four-service chain cannot serve.
	tbMinGap = time.Millisecond
)

// tbRequest describes the downstream at time t since the run started: the
// latency and outcome of a request sent then, and the number of k6 virtual
// users. done reports that the scenario has ended.
type tbRequest func(t time.Duration, rng *rand.Rand) (latency time.Duration, success bool, vus int, done bool)

// tbResult holds when each breaker first opened, measured from faultStart,
// or -1 if it never opened.
type tbResult struct {
	fluxgo, gobreaker time.Duration
}

func (r tbResult) String() string {
	return fmt.Sprintf("fluxgo=%s gobreaker=%s", fmtOpen(r.fluxgo), fmtOpen(r.gobreaker))
}

func fmtOpen(d time.Duration) string {
	if d < 0 {
		return "never"
	}
	return "+" + d.Round(100*time.Millisecond).String()
}

// gobreakerModel reproduces the ReadyToTrip function of the testbed's
// gobreaker adapter: trip on ConsecutiveFailures consecutive failures, or once
// MinRequests requests are counted and the failure ratio reaches
// FailureRatio. Interval is 0, so counts are never cleared while Closed.
type gobreakerModel struct {
	requests, failures, consecutive uint32
}

func (g *gobreakerModel) record(success bool) (tripped bool) {
	const consecutiveFailures, minRequests, failureRatio = 5, 20, 0.6
	g.requests++
	if success {
		g.consecutive = 0
		return false
	}
	g.failures++
	g.consecutive++
	return g.consecutive >= consecutiveFailures ||
		(g.requests >= minRequests && float64(g.failures)/float64(g.requests) >= failureRatio)
}

// runTestbedScenario feeds the request stream to both breakers until each has
// opened or the scenario ends. k6 runs closed-loop virtual users, so requests
// complete at a rate of vus/latency, up to 1/tbMinGap. The stream does not react to the
// breakers: only the time of the first trip is measured, not what happens
// after it.
func runTestbedScenario(cfg Config, faultStart time.Duration, request tbRequest) tbResult {
	clock := newFakeClock()
	cb := newCircuitBreakerWithClock(Settings{Name: "testbed", Config: cfg}, clock.Now)
	rng := rand.New(rand.NewSource(1))
	var gb gobreakerModel
	res := tbResult{fluxgo: -1, gobreaker: -1}

	for elapsed := time.Duration(0); res.fluxgo < 0 || res.gobreaker < 0; {
		latency, success, vus, done := request(elapsed, rng)
		if done {
			break
		}
		if res.fluxgo < 0 {
			executeWithLatency(cb, latency, success)
			if cb.State() == StateOpen {
				res.fluxgo = elapsed - faultStart
			}
		}
		if res.gobreaker < 0 && gb.record(success) {
			res.gobreaker = elapsed - faultStart
		}
		gap := latency / time.Duration(vus)
		if gap < tbMinGap {
			gap = tbMinGap
		}
		clock.Advance(gap)
		elapsed += gap
	}
	return res
}

// tbHealthyMs returns the synthetic latency of the notification service,
// 10 to 20ms.
func tbHealthyMs(rng *rand.Rand) float64 {
	return 10 + rng.Float64()*10
}

func tbJitter(rng *rand.Rand) float64 {
	return (rng.Float64()*2 - 1) * tbJitterMs
}

func ms(v float64) time.Duration {
	return time.Duration(v * float64(time.Millisecond))
}

// tbGradualDegradation mirrors inject_gradual_degradation: 50 VUs, then from
// t=30s a latency toxic of +50ms that grows by 50ms every 25s up to +500ms,
// held for 25s. Every request still succeeds.
func tbGradualDegradation(t time.Duration, rng *rand.Rand) (time.Duration, bool, int, bool) {
	const start, step = 30 * time.Second, 25 * time.Second
	if t >= start+10*step {
		return 0, false, 0, true
	}
	latency := tbHealthyMs(rng)
	if t >= start {
		added := float64(50 * (1 + int((t-start)/step)))
		if added > 500 {
			added = 500
		}
		latency += added + tbJitter(rng)
	}
	return ms(latency), true, 50, false
}

// tbTrafficSpike mirrors traffic_spike.js and inject_traffic_spike_bandwidth:
// 10 VUs for 60s, a ramp to 250 VUs over 10s, and a 60s peak during which a
// bandwidth toxic multiplies latency by factor and errorRate of the requests
// fail. The testbed requires the downstream success rate to stay above 95%.
func tbTrafficSpike(factor, errorRate float64) tbRequest {
	return func(t time.Duration, rng *rand.Rand) (time.Duration, bool, int, bool) {
		const healthy, ramp, peak = 60 * time.Second, 10 * time.Second, 60 * time.Second
		if t >= healthy+ramp+peak {
			return 0, false, 0, true
		}
		latency := tbHealthyMs(rng)
		switch {
		case t < healthy:
			return ms(latency), true, 10, false
		case t < healthy+ramp:
			vus := 10 + int(240*(t-healthy)/ramp)
			return ms(latency), true, vus, false
		default:
			return ms(latency * factor), rng.Float64() >= errorRate, 250, false
		}
	}
}

// tbSuddenFailure mirrors inject_sudden_failure: 50 VUs, and from t=60s a
// timeout toxic that holds every request until the 10s client timeout fails it.
func tbSuddenFailure(t time.Duration, rng *rand.Rand) (time.Duration, bool, int, bool) {
	const start, end = 60 * time.Second, 120 * time.Second
	if t >= end {
		return 0, false, 0, true
	}
	if t < start {
		return ms(tbHealthyMs(rng)), true, 50, false
	}
	return tbClientTimeout, false, 50, false
}

func withSlowRef(ref SlowRequestReference, margin float64) Config {
	cfg := DefaultConfig()
	cfg.SlowRequestReference = ref
	cfg.SlowRequestMargin = margin
	return cfg
}

func TestTestbedScenario_GradualDegradation(t *testing.T) {
	const faultStart = 30 * time.Second

	recent := runTestbedScenario(withSlowRef(SlowRefRecent, 2), faultStart, tbGradualDegradation)
	t.Logf("SlowRefRecent:   %v", recent)
	baseline := runTestbedScenario(withSlowRef(SlowRefBaseline, 2), faultStart, tbGradualDegradation)
	t.Logf("SlowRefBaseline: %v", baseline)

	// Without errors, gobreaker has nothing to count, and with SlowRefRecent
	// the slow-request reference rises with the latency.
	if recent.gobreaker >= 0 {
		t.Errorf("gobreaker opened at %v, want never: the scenario has no errors", recent.gobreaker)
	}
	if recent.fluxgo >= 0 {
		t.Errorf("SlowRefRecent opened at %v, want never", recent.fluxgo)
	}
}

func TestTestbedScenario_TrafficSpike(t *testing.T) {
	const faultStart = 70 * time.Second
	for _, factor := range []float64{1.5, 2, 3, 5, 10} {
		for _, ref := range []SlowRequestReference{SlowRefRecent, SlowRefBaseline} {
			res := runTestbedScenario(withSlowRef(ref, 2), faultStart, tbTrafficSpike(factor, 0.03))
			t.Logf("latency x%-4v %-15v %v", factor, slowRefName(ref), res)
		}
	}
}

func TestTestbedScenario_SuddenFailure(t *testing.T) {
	const faultStart = 60 * time.Second
	res := runTestbedScenario(DefaultConfig(), faultStart, tbSuddenFailure)
	t.Logf("%v", res)
	if res.fluxgo < 0 || res.gobreaker < 0 {
		t.Fatalf("%v, want both breakers to open", res)
	}
}

// TestTestbedScenario_SlowRefBaselineMarginSweep shows how SlowRequestMargin
// trades detection of the gradual degradation against false trips during the
// traffic spike.
func TestTestbedScenario_SlowRefBaselineMarginSweep(t *testing.T) {
	for _, margin := range []float64{2, 3, 5, 8, 12} {
		cfg := withSlowRef(SlowRefBaseline, margin)
		gradual := runTestbedScenario(cfg, 30*time.Second, tbGradualDegradation)
		line := fmt.Sprintf("margin %-3v gradual: %-7s spike:", margin, fmtOpen(gradual.fluxgo))
		for _, factor := range []float64{1.5, 2, 3, 5, 10} {
			spike := runTestbedScenario(cfg, 70*time.Second, tbTrafficSpike(factor, 0.03))
			line += fmt.Sprintf(" x%v=%s", factor, fmtOpen(spike.fluxgo))
		}
		t.Log(line)
	}
}

func slowRefName(ref SlowRequestReference) string {
	if ref == SlowRefBaseline {
		return "SlowRefBaseline"
	}
	return "SlowRefRecent"
}
