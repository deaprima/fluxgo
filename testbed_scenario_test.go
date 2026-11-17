// testbed_scenario_test.go replays the fault-injection scenarios of
// fluxgo-testbed on a fake clock, so their outcome can be checked in seconds
// before the full experiment runs. Each scenario is replayed once with fluxgo
// and once with a model of the gobreaker configuration used by the testbed.
//
// The replay is a closed-loop, event-driven simulation of a two-hop chain:
// k6 virtual users call the outer hop, which calls the inner hop, the link
// where the testbed injects its fault. Requests overlap in time as in the
// testbed, a rejection by the inner breaker reaches the outer breaker as an
// error (a 503 from the service in between), and the simulation keeps running
// after the first trip, so time spent Open and recovery are measured too.
package fluxgo

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// Parameters copied from fluxgo-testbed (runner/orchestrator.py, k6/scenarios,
// internal/client and internal/cb/env_config.go).
const (
	tbClientTimeout = 10 * time.Second // http.Client timeout of every hop

	// tbHopOverhead is the time a hop adds on top of its downstream when the
	// containers are idle. With the 10-20ms notification sleep it gives the
	// healthy P50 and P95 measured at each hop (about 16 and 21ms).
	tbHopOverhead = time.Millisecond

	// tbRejectLatency is the time a rejected request takes end to end.
	tbRejectLatency = 200 * time.Microsecond

	// tbIdleChainMs is the end-to-end latency used to turn a number of k6
	// virtual users into a request rate: rate = VUs / (latency + sleep).
	tbIdleChainMs = 17

	// Containers saturate their CPU limit between these request rates.
	// Below tbLoadLow (normal load is about 750 requests per second) the
	// profile has no effect.
	tbLoadLow  = 1500.0
	tbLoadFull = 6750.0 // 250 VUs with the spike's 20ms sleep

	// tbCFSPeriod is the CFS bandwidth period of the Linux scheduler. A
	// container that spends its CPU quota is throttled until the period ends.
	tbCFSPeriod = 100 * time.Millisecond
	// A throttling stall lasts between these bounds; the longest bursts of
	// slow requests measured on spec-b lasted about 130ms.
	tbStallMinMs = 20.0
	tbStallMaxMs = 130.0
)

// tbProfile describes the CPU limit of the testbed containers. At full load
// it adds an exponential queueing delay to every request, and stalls a share
// of the CFS periods: every request that would complete during a stall waits
// until the stall ends. Stalls are what fill a count-based window with slow
// requests within milliseconds.
type tbProfile struct {
	name string
	// innerStall and outerStall are the probabilities that a CFS period
	// stalls the inner hop or adds a stall on the outer hop, at full load.
	innerStall, outerStall float64
	// innerQueueMs and outerQueueMs are the mean queueing delays, in
	// milliseconds, that each hop adds at full load.
	innerQueueMs, outerQueueMs float64
}

var (
	// tbSpecA (2 vCPU per container) did not saturate during the spike: the
	// P95 rose by 7%, which the replay treats as no effect.
	tbSpecA = tbProfile{name: "spec-a"}

	// tbSpecB (0.5 vCPU per container) ran at its CPU limit during the spike.
	// Calibrated on the gobreaker run of 2026-11-17, in which every request
	// succeeded; TestTestbedModel_TrafficSpikeLatency checks the fit.
	tbSpecB = tbProfile{
		name:         "spec-b",
		innerStall:   0.005,
		outerStall:   0.045,
		innerQueueMs: 4.5,
		outerQueueMs: 4,
	}
)

// tbScenario describes a testbed scenario: the k6 load, and the fault that
// Toxiproxy injects on the inner hop between faultStart and faultEnd. In the
// traffic spike the "fault" is the load itself.
type tbScenario struct {
	name string
	// inner and outer name the breakers of the faulted link and of its caller.
	inner, outer              string
	faultStart, faultEnd, end time.Duration

	maxVUs int
	vus    func(t time.Duration) int // active k6 virtual users at time t
	think  time.Duration             // k6 sleep between iterations

	// toxic returns the latency that the fault adds to a request sent to the
	// inner hop at time t, and whether the request fails instead.
	toxic func(t time.Duration, rng *rand.Rand) (added time.Duration, fail bool)
}

// load returns how close the containers are to their CPU limit at time t,
// from 0 (no effect) to 1.
func (sc tbScenario) load(t time.Duration) float64 {
	rate := float64(sc.vus(t)) / (tbIdleChainMs/1000.0 + sc.think.Seconds())
	return clamp((rate-tbLoadLow)/(tbLoadFull-tbLoadLow), 0, 1)
}

// Phases of a scenario, by the time a request is sent.
const (
	tbBefore = iota // before the fault
	tbDuring        // while the fault is active
	tbAfter         // after the fault has ended
)

func (sc tbScenario) phase(t time.Duration) int {
	switch {
	case t < sc.faultStart:
		return tbBefore
	case t < sc.faultEnd:
		return tbDuring
	default:
		return tbAfter
	}
}

func constVUs(n int) func(time.Duration) int {
	return func(time.Duration) int { return n }
}

func noToxic(time.Duration, *rand.Rand) (time.Duration, bool) { return 0, false }

// tbGradualDegradation mirrors inject_gradual_degradation: 50 VUs, and after
// 60s healthy a latency toxic on user -> notification of +10ms (+-2ms jitter)
// that grows by 10ms every 25s up to +100ms, held for 25s, then removed,
// followed by a 90s tail. Every request still succeeds.
func tbGradualDegradation() tbScenario {
	const healthy, step, steps, tail = 60 * time.Second, 25 * time.Second, 10, 90 * time.Second
	const faultEnd = healthy + steps*step
	return tbScenario{
		name:  "gradual degradation",
		inner: "user-to-notification", outer: "auth-to-user",
		faultStart: healthy, faultEnd: faultEnd, end: faultEnd + tail,
		maxVUs: 50, vus: constVUs(50), think: 50 * time.Millisecond,
		toxic: func(t time.Duration, rng *rand.Rand) (time.Duration, bool) {
			if t < healthy || t >= faultEnd {
				return 0, false
			}
			added := float64(10*(1+int((t-healthy)/step))) + (rng.Float64()*2-1)*2
			return ms(added), false
		},
	}
}

// tbSuddenFailure mirrors inject_sudden_failure: 50 VUs, and from t=60s to
// t=120s a timeout toxic on user -> notification that holds every request
// until the 10s client timeout fails it.
func tbSuddenFailure() tbScenario {
	const start, end = 60 * time.Second, 120 * time.Second
	return tbScenario{
		name:  "sudden failure",
		inner: "user-to-notification", outer: "auth-to-user",
		faultStart: start, faultEnd: end, end: 180 * time.Second,
		maxVUs: 50, vus: constVUs(50), think: 50 * time.Millisecond,
		toxic: func(t time.Duration, _ *rand.Rand) (time.Duration, bool) {
			return 0, t >= start && t < end
		},
	}
}

// tbTrafficSpike mirrors k6/scenarios/traffic_spike.js: 10 VUs for 60s, a
// ramp to 250 VUs over 10s, a 60s peak, a ramp back to 10 VUs over 10s and
// 20s at 10 VUs. The fault window covers both ramps and the peak. The
// testbed's bandwidth toxic had no measurable effect and is not modeled.
func tbTrafficSpike() tbScenario {
	const healthy, ramp, peak = 60 * time.Second, 10 * time.Second, 60 * time.Second
	const down = healthy + ramp + peak
	return tbScenario{
		name:  "traffic spike",
		inner: "auth-to-user", outer: "gateway-to-auth",
		faultStart: healthy, faultEnd: down + ramp, end: down + ramp + 20*time.Second,
		maxVUs: 250, think: 20 * time.Millisecond, toxic: noToxic,
		vus: func(t time.Duration) int {
			switch {
			case t < healthy:
				return 10
			case t < healthy+ramp:
				return 10 + int(240*(t-healthy)/ramp)
			case t < down:
				return 250
			case t < down+ramp:
				return 250 - int(240*(t-down)/ramp)
			default:
				return 10
			}
		},
	}
}

// tbNormalLoad mirrors k6/scenarios/normal_load.js: 50 VUs for 5 minutes
// without any fault.
func tbNormalLoad() tbScenario {
	const end = 300 * time.Second
	return tbScenario{
		name:  "normal load",
		inner: "user-to-notification", outer: "auth-to-user",
		faultStart: end, faultEnd: end, end: end,
		maxVUs: 50, vus: constVUs(50), think: 50 * time.Millisecond, toxic: noToxic,
	}
}

// tbStalls generates the CPU throttling stalls of one hop. Each CFS period
// is drawn from its own seed, so a stall does not depend on the order in
// which requests query it.
type tbStalls struct {
	seed    int64
	prob    float64
	load    func(time.Duration) float64
	periods map[int64]tbStall
}

type tbStall struct{ start, end time.Duration }

func newTbStalls(seed int64, prob float64, load func(time.Duration) float64) *tbStalls {
	return &tbStalls{seed: seed, prob: prob, load: load, periods: map[int64]tbStall{}}
}

// delay returns how long a request that would complete at time at waits for
// a stall to end.
func (s *tbStalls) delay(at time.Duration) time.Duration {
	if s.prob == 0 {
		return 0
	}
	var d time.Duration
	k := int64(at / tbCFSPeriod)
	// A stall can outlast the period it starts in by up to two periods.
	for p := k - 2; p <= k; p++ {
		if st := s.period(p); at >= st.start && at < st.end && st.end-at > d {
			d = st.end - at
		}
	}
	return d
}

func (s *tbStalls) period(k int64) tbStall {
	if st, ok := s.periods[k]; ok {
		return st
	}
	var st tbStall
	if k >= 0 {
		rng := rand.New(rand.NewSource(s.seed*1_000_003 + k))
		start := time.Duration(k) * tbCFSPeriod
		if rng.Float64() < s.prob*s.load(start) {
			st.start = start + time.Duration(rng.Int63n(int64(tbCFSPeriod)))
			st.end = st.start + ms(tbStallMinMs+rng.Float64()*(tbStallMaxMs-tbStallMinMs))
		}
	}
	s.periods[k] = st
	return st
}

// tbBreaker is a circuit breaker driven by the replay: admit is called when
// a request is sent, and complete with the token admit returned when it ends.
type tbBreaker interface {
	admit() (token uint64, ok bool)
	complete(token uint64, latency time.Duration, success bool)
}

// tbBreakerFactory builds the breaker of one hop. onChange must be called on
// every state transition.
type tbBreakerFactory func(name string, now func() time.Time, onChange func(to State)) tbBreaker

// tbFluxgo returns a factory of fluxgo breakers with the given Config. The
// breaker is driven exactly as Execute drives it, with the simulated latency.
func tbFluxgo(cfg Config) tbBreakerFactory {
	return func(name string, now func() time.Time, onChange func(to State)) tbBreaker {
		return tbFluxgoBreaker{newCircuitBreakerWithClock(Settings{
			Name:          name,
			Config:        cfg,
			OnStateChange: func(_ string, _, to State) { onChange(to) },
		}, now)}
	}
}

type tbFluxgoBreaker struct{ cb *CircuitBreaker }

func (b tbFluxgoBreaker) admit() (uint64, bool) {
	generation, change, err := b.cb.machine.AllowRequest()
	if change.occurred() {
		b.cb.handleTransition(change.from, change.to)
	}
	return generation, err == nil
}

func (b tbFluxgoBreaker) complete(generation uint64, latency time.Duration, success bool) {
	b.cb.onRequestComplete(generation, latency, success)
}

// tbGobreaker is a factory of models of sony/gobreaker with the testbed's
// settings: MaxRequests 1, Interval 0 (counts are cleared only on state
// changes), Timeout 60s, and a ReadyToTrip that opens after 5 consecutive
// failures or once 20 requests are counted and the failure ratio reaches 0.6.
func tbGobreaker(name string, now func() time.Time, onChange func(to State)) tbBreaker {
	return &tbGobreakerModel{now: now, onChange: onChange}
}

type tbGobreakerModel struct {
	now      func() time.Time
	onChange func(to State)

	state      State
	generation uint64
	expiry     time.Time

	requests, failures, consecutiveFailures, consecutiveSuccesses uint32
}

func (g *tbGobreakerModel) setState(s State) {
	g.state = s
	g.generation++
	g.requests, g.failures, g.consecutiveFailures, g.consecutiveSuccesses = 0, 0, 0, 0
	if s == StateOpen {
		g.expiry = g.now().Add(60 * time.Second)
	}
	g.onChange(s)
}

func (g *tbGobreakerModel) admit() (uint64, bool) {
	if g.state == StateOpen && !g.now().Before(g.expiry) {
		g.setState(StateHalfOpen)
	}
	if g.state == StateOpen || (g.state == StateHalfOpen && g.requests >= 1) {
		return g.generation, false
	}
	g.requests++
	return g.generation, true
}

func (g *tbGobreakerModel) complete(generation uint64, _ time.Duration, success bool) {
	if generation != g.generation {
		return
	}
	if success {
		g.consecutiveFailures = 0
		g.consecutiveSuccesses++
		if g.state == StateHalfOpen && g.consecutiveSuccesses >= 1 {
			g.setState(StateClosed)
		}
		return
	}
	g.failures++
	g.consecutiveFailures++
	g.consecutiveSuccesses = 0
	switch g.state {
	case StateClosed:
		if g.consecutiveFailures >= 5 ||
			(g.requests >= 20 && float64(g.failures)/float64(g.requests) >= 0.6) {
			g.setState(StateOpen)
		}
	case StateHalfOpen:
		g.setState(StateOpen)
	}
}

// tbPassThrough is a factory of breakers that admit every request, used to
// observe the latency model alone.
func tbPassThrough(string, func() time.Time, func(State)) tbBreaker { return tbNoBreaker{} }

type tbNoBreaker struct{}

func (tbNoBreaker) admit() (uint64, bool)                { return 0, true }
func (tbNoBreaker) complete(uint64, time.Duration, bool) {}

// tbEvent is a simulation event run at time at; seq orders events at the
// same time by scheduling order.
type tbEvent struct {
	at  time.Duration
	seq uint64
	fn  func()
}

type tbEvents []tbEvent

func (e tbEvents) Len() int { return len(e) }
func (e tbEvents) Less(i, j int) bool {
	if e[i].at != e[j].at {
		return e[i].at < e[j].at
	}
	return e[i].seq < e[j].seq
}
func (e tbEvents) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e *tbEvents) Push(x any)   { *e = append(*e, x.(tbEvent)) }
func (e *tbEvents) Pop() any {
	old := *e
	x := old[len(old)-1]
	*e = old[:len(old)-1]
	return x
}

// Hops of the simulated chain.
const (
	tbInner = iota
	tbOuter
)

type tbTransition struct {
	at time.Duration
	to State
}

// tbHopStats summarizes what one breaker did during a run.
type tbHopStats struct {
	name      string
	opened    bool
	firstOpen time.Duration // first transition to Open, since faultStart
	// notClosed is the time spent Open or Half-Open in each phase.
	notClosed   [3]time.Duration
	closedAtEnd bool
}

func (s tbHopStats) String() string {
	open := "never opened"
	if s.opened {
		open = fmt.Sprintf("opened at %+.1fs", s.firstOpen.Seconds())
	}
	end := "closed at end"
	if !s.closedAtEnd {
		end = "NOT closed at end"
	}
	return fmt.Sprintf("%-21s %-16s not closed %3.0fs during, %3.0fs after; %s",
		s.name, open, s.notClosed[tbDuring].Seconds(), s.notClosed[tbAfter].Seconds(), end)
}

// tbResult is the outcome of one replay.
type tbResult struct {
	hops [2]tbHopStats
	// requests counts the k6 iterations sent in each phase, and rejected
	// those that a breaker rejected, at either hop.
	requests, rejected [3]int
}

// rejectedShare returns the share of the requests sent in phase that a
// breaker rejected.
func (r tbResult) rejectedShare(phase int) float64 {
	if r.requests[phase] == 0 {
		return 0
	}
	return float64(r.rejected[phase]) / float64(r.requests[phase])
}

func (r tbResult) String() string {
	return fmt.Sprintf("checkouts rejected: %4.1f%% before, %4.1f%% during, %4.1f%% after the fault (%d sent)\n  %v\n  %v",
		100*r.rejectedShare(tbBefore), 100*r.rejectedShare(tbDuring), 100*r.rejectedShare(tbAfter),
		r.requests[tbBefore]+r.requests[tbDuring]+r.requests[tbAfter], r.hops[tbInner], r.hops[tbOuter])
}

// runTestbed replays sc on profile prof with breakers built by newBreaker. If
// observe is not nil, it is called with the completion time and latency of
// every request admitted at a hop, in completion order.
func runTestbed(sc tbScenario, prof tbProfile, newBreaker tbBreakerFactory, observe func(hop int, at, latency time.Duration)) tbResult {
	clock := newFakeClock()
	rng := rand.New(rand.NewSource(1))
	var (
		now    time.Duration
		events tbEvents
		seq    uint64
	)
	schedule := func(at time.Duration, fn func()) {
		seq++
		heap.Push(&events, tbEvent{at: at, seq: seq, fn: fn})
	}

	var transitions [2][]tbTransition
	var breakers [2]tbBreaker
	for hop, name := range [2]string{sc.inner, sc.outer} {
		hop := hop
		breakers[hop] = newBreaker(name, clock.Now, func(to State) {
			transitions[hop] = append(transitions[hop], tbTransition{at: now, to: to})
		})
	}
	stalls := [2]*tbStalls{newTbStalls(11, prof.innerStall, sc.load), newTbStalls(23, prof.outerStall, sc.load)}
	queueMs := [2]float64{prof.innerQueueMs, prof.outerQueueMs}

	// latencies returns the latency at each hop of a request sent at start.
	latencies := func(start time.Duration) (inner, outer time.Duration, success bool) {
		added, fail := sc.toxic(start, rng)
		if fail {
			return tbClientTimeout, tbClientTimeout, false
		}
		load := sc.load(start)
		inner = ms(10+rng.Float64()*10) + tbHopOverhead + added + ms(rng.ExpFloat64()*queueMs[tbInner]*load)
		inner += stalls[tbInner].delay(start + inner)
		outer = inner + tbHopOverhead + ms(rng.ExpFloat64()*queueMs[tbOuter]*load)
		outer += stalls[tbOuter].delay(start + outer)
		if outer >= tbClientTimeout {
			return min(inner, tbClientTimeout), tbClientTimeout, false
		}
		return inner, outer, true
	}
	completeAt := func(hop int, token uint64, at, latency time.Duration, success bool) {
		schedule(at, func() {
			if observe != nil {
				observe(hop, at, latency)
			}
			breakers[hop].complete(token, latency, success)
		})
	}

	var res tbResult
	var iterate func(vu int)
	iterate = func(vu int) {
		start := now
		if start >= sc.end {
			return
		}
		if vu >= sc.vus(start) {
			schedule(start+50*time.Millisecond, func() { iterate(vu) })
			return
		}
		phase := sc.phase(start)
		res.requests[phase]++
		next := func(latency time.Duration) {
			schedule(start+latency+sc.think, func() { iterate(vu) })
		}

		outerToken, ok := breakers[tbOuter].admit()
		if !ok {
			res.rejected[phase]++
			next(tbRejectLatency)
			return
		}
		innerToken, ok := breakers[tbInner].admit()
		if !ok {
			// The service in between answers 503 without calling its downstream.
			res.rejected[phase]++
			completeAt(tbOuter, outerToken, start+tbHopOverhead, tbHopOverhead, false)
			next(tbHopOverhead)
			return
		}
		inner, outer, success := latencies(start)
		completeAt(tbInner, innerToken, start+inner, inner, success)
		completeAt(tbOuter, outerToken, start+outer, outer, success)
		next(outer)
	}

	iteration := time.Duration(tbIdleChainMs)*time.Millisecond + sc.think
	for vu := 0; vu < sc.maxVUs; vu++ {
		vu := vu
		schedule(time.Duration(rng.Int63n(int64(iteration))), func() { iterate(vu) })
	}
	for events.Len() > 0 {
		ev := heap.Pop(&events).(tbEvent)
		clock.Advance(ev.at - now)
		now = ev.at
		ev.fn()
	}

	for hop := range transitions {
		res.hops[hop] = tbSummarize(sc, [2]string{sc.inner, sc.outer}[hop], transitions[hop])
	}
	return res
}

// tbSummarize computes the statistics of one hop from its transitions.
func tbSummarize(sc tbScenario, name string, transitions []tbTransition) tbHopStats {
	s := tbHopStats{name: name}
	bounds := [4]time.Duration{0, sc.faultStart, sc.faultEnd, sc.end}
	addNotClosed := func(from, to time.Duration) {
		for p := 0; p < 3; p++ {
			if lo, hi := max(from, bounds[p]), min(to, bounds[p+1]); hi > lo {
				s.notClosed[p] += hi - lo
			}
		}
	}
	state, since := StateClosed, time.Duration(0)
	for _, tr := range transitions {
		if tr.at >= sc.end {
			break
		}
		if state != StateClosed {
			addNotClosed(since, tr.at)
		}
		if tr.to == StateOpen && !s.opened {
			s.opened, s.firstOpen = true, tr.at-sc.faultStart
		}
		state, since = tr.to, tr.at
	}
	if state != StateClosed {
		addNotClosed(since, sc.end)
	}
	s.closedAtEnd = state == StateClosed
	return s
}

func ms(v float64) time.Duration {
	return time.Duration(v * float64(time.Millisecond))
}

// tbLatencyStats summarizes the latencies one hop completed in a time range.
type tbLatencyStats struct {
	n                       int
	rate                    float64 // requests per second
	p50, p95, p99           float64 // milliseconds
	slowShare               float64 // share above the slow limit
	maxSlowIn100            int     // most slow requests among 100 consecutive ones
	maxSlowShare1s          float64 // highest slow share in a one-second bin
	windowsWith60PctSlow    int     // runs of 100 consecutive requests with at least 60 slow
	longestSlowWindowStreak time.Duration
}

type tbSample struct{ at, latency time.Duration }

func tbLatencyStatsOf(samples []tbSample, from, to time.Duration, slowMs float64) tbLatencyStats {
	var in []tbSample
	for _, s := range samples {
		if s.at >= from && s.at < to {
			in = append(in, s)
		}
	}
	st := tbLatencyStats{n: len(in), rate: float64(len(in)) / (to - from).Seconds()}
	if len(in) == 0 {
		return st
	}
	lat := make([]float64, len(in))
	slow := 0
	bins := map[int64][2]int{}
	streakStart := time.Duration(-1)
	inWindow := 0
	for i, s := range in {
		v := s.latency.Seconds() * 1000
		lat[i] = v
		isSlow := v > slowMs
		if isSlow {
			slow++
			inWindow++
		}
		if i >= 100 && in[i-100].latency.Seconds()*1000 > slowMs {
			inWindow--
		}
		if i >= 99 && inWindow >= 60 {
			if streakStart < 0 {
				streakStart = s.at
				st.windowsWith60PctSlow++
			}
			st.longestSlowWindowStreak = max(st.longestSlowWindowStreak, s.at-streakStart)
		} else {
			streakStart = -1
		}
		st.maxSlowIn100 = max(st.maxSlowIn100, inWindow)
		b := bins[int64(s.at/time.Second)]
		b[0]++
		if isSlow {
			b[1]++
		}
		bins[int64(s.at/time.Second)] = b
	}
	for _, b := range bins {
		if b[0] >= 20 {
			st.maxSlowShare1s = math.Max(st.maxSlowShare1s, float64(b[1])/float64(b[0]))
		}
	}
	sort.Float64s(lat)
	pct := func(p float64) float64 { return lat[min(len(lat)-1, int(p/100*float64(len(lat))))] }
	st.p50, st.p95, st.p99 = pct(50), pct(95), pct(99)
	st.slowShare = float64(slow) / float64(len(in))
	return st
}

func (s tbLatencyStats) String() string {
	return fmt.Sprintf("%5.0f/s p50 %5.1f p95 %5.1f p99 %6.1f slow %5.2f%% max-slow/100 %3d (>=60: %3d, longest %3.0fms) max-slow/1s %.2f",
		s.rate, s.p50, s.p95, s.p99, 100*s.slowShare, s.maxSlowIn100, s.windowsWith60PctSlow,
		s.longestSlowWindowStreak.Seconds()*1000, s.maxSlowShare1s)
}

// TestTestbedModel_TrafficSpikeLatency checks the latency model of the traffic
// spike against the testbed's gobreaker runs of 2026-11-17, in which nothing
// was rejected. "Slow" means above three times the healthy P95 of the hop,
// the slow limit of fluxgo's default config. On spec-b, measured at the peak:
//
//	gateway (outer): 4890/s, p50 21.9, p95 46.6, p99 72.6, slow 1.68%,
//	                 29 runs of 100 requests with >=60 slow (longest 132ms),
//	                 at most 13% slow in any one-second bin
//	auth (inner):    p50 18.6, p95 31.5, p99 38.1, slow 0.22%, 4 such runs
//	healthy:         p50 16.4-16.8, p95 21.4-21.9 (inner-outer)
func TestTestbedModel_TrafficSpikeLatency(t *testing.T) {
	sc := tbTrafficSpike()
	for _, prof := range []tbProfile{tbSpecA, tbSpecB} {
		var samples [2][]tbSample
		runTestbed(sc, prof, tbPassThrough, func(hop int, at, latency time.Duration) {
			samples[hop] = append(samples[hop], tbSample{at, latency})
		})
		var healthy, peak [2]tbLatencyStats
		for hop, name := range [2]string{"inner", "outer"} {
			healthy[hop] = tbLatencyStatsOf(samples[hop], 30*time.Second, 60*time.Second, math.Inf(1))
			slowMs := 3 * healthy[hop].p95
			t.Logf("%s %s (slow > %.1fms)", prof.name, name, slowMs)
			for _, ph := range []struct {
				name     string
				from, to time.Duration
			}{
				{"healthy", 30 * time.Second, 60 * time.Second},
				{"ramp", 60 * time.Second, 70 * time.Second},
				{"peak", 70 * time.Second, 130 * time.Second},
				{"down", 130 * time.Second, 140 * time.Second},
				{"tail", 140 * time.Second, 160 * time.Second},
			} {
				st := tbLatencyStatsOf(samples[hop], ph.from, ph.to, slowMs)
				t.Logf("  %-7s %v", ph.name, st)
				if ph.name == "peak" {
					peak[hop] = st
				}
			}
		}

		for hop := range healthy {
			if h := healthy[hop]; h.p50 < 14 || h.p50 > 19 || h.p95 < 19 || h.p95 > 24 {
				t.Errorf("%s hop %d healthy p50 %.1f p95 %.1f, want about 16.5 and 21.5", prof.name, hop, h.p50, h.p95)
			}
		}
		if peak[tbOuter].rate < 4000 {
			t.Errorf("%s peak rate %.0f/s, want at least 4000/s", prof.name, peak[tbOuter].rate)
		}
		if prof == tbSpecA {
			if r := peak[tbOuter].p95 / healthy[tbOuter].p95; r > 1.2 {
				t.Errorf("spec-a outer peak P95 x%.2f, want at most x1.2", r)
			}
			continue
		}
		out, in := peak[tbOuter], peak[tbInner]
		if r := out.p95 / healthy[tbOuter].p95; r < 1.8 || r > 2.8 {
			t.Errorf("spec-b outer peak P95 x%.2f of healthy, want x1.8-2.8 (measured x2.1)", r)
		}
		if r := out.p99 / healthy[tbOuter].p95; r < 2.5 || r > 5 {
			t.Errorf("spec-b outer peak P99 x%.2f of healthy P95, want x2.5-5 (measured x3.3)", r)
		}
		if r := in.p95 / healthy[tbInner].p95; r < 1.2 || r > 1.9 {
			t.Errorf("spec-b inner peak P95 x%.2f of healthy, want x1.2-1.9 (measured x1.5)", r)
		}
		if out.slowShare < 0.005 || out.slowShare > 0.04 {
			t.Errorf("spec-b outer peak slow share %.2f%%, want 0.5-4%% (measured 1.68%%)", 100*out.slowShare)
		}
		if in.slowShare > 0.01 {
			t.Errorf("spec-b inner peak slow share %.2f%%, want at most 1%% (measured 0.22%%)", 100*in.slowShare)
		}
		// The property behind the false trips: short bursts fill a window of
		// 100 requests with slow ones, while over a second slow requests
		// stay a small minority.
		for hop, st := range peak {
			if st.windowsWith60PctSlow == 0 {
				t.Errorf("spec-b hop %d: no run of 100 requests with >=60 slow, want bursts as measured", hop)
			}
			if st.maxSlowShare1s > 0.25 {
				t.Errorf("spec-b hop %d: %.0f%% slow in one second, want at most 25%% (measured 13%%)", hop, 100*st.maxSlowShare1s)
			}
		}
	}
}

// TestTestbedScenario_GradualDegradation checks that fluxgo detects a slow
// downstream that never returns an error, which gobreaker cannot, and that it
// closes again once the toxic is removed.
func TestTestbedScenario_GradualDegradation(t *testing.T) {
	sc := tbGradualDegradation()
	cfg := DefaultConfig()
	flux := runTestbed(sc, tbSpecB, tbFluxgo(cfg), nil)
	gb := runTestbed(sc, tbSpecB, tbGobreaker, nil)
	t.Logf("fluxgo:    %v", flux)
	t.Logf("gobreaker: %v", gb)

	for _, h := range gb.hops {
		if h.opened {
			t.Errorf("gobreaker %s opened, want never: the scenario has no errors", h.name)
		}
	}
	in := flux.hops[tbInner]
	if !in.opened || in.firstOpen <= 0 || in.firstOpen >= sc.faultEnd-sc.faultStart {
		t.Errorf("fluxgo %s: want it to open during the ramp, before the toxic is removed", in.name)
	}
	for _, h := range flux.hops {
		if !h.closedAtEnd || h.notClosed[tbAfter] > cfg.RecoveryTimeout+time.Second {
			t.Errorf("fluxgo %s: want Closed within RecoveryTimeout of the toxic's removal", h.name)
		}
	}
}

// TestTestbedScenario_SuddenFailure checks that fluxgo opens on a total
// failure no later than gobreaker and recovers once the fault ends.
func TestTestbedScenario_SuddenFailure(t *testing.T) {
	sc := tbSuddenFailure()
	flux := runTestbed(sc, tbSpecB, tbFluxgo(DefaultConfig()), nil)
	gb := runTestbed(sc, tbSpecB, tbGobreaker, nil)
	t.Logf("fluxgo:    %v", flux)
	t.Logf("gobreaker: %v", gb)

	fin, gin := flux.hops[tbInner], gb.hops[tbInner]
	if !fin.opened || !gin.opened {
		t.Fatalf("fluxgo opened %v, gobreaker opened %v, want both to open", fin.opened, gin.opened)
	}
	if fin.firstOpen > gin.firstOpen {
		t.Errorf("fluxgo opened at %+.1fs, want no later than gobreaker at %+.1fs",
			fin.firstOpen.Seconds(), gin.firstOpen.Seconds())
	}
	for _, r := range []tbResult{flux, gb} {
		for _, h := range r.hops {
			if !h.closedAtEnd {
				t.Errorf("%s not Closed at the end, want recovery after the fault", h.name)
			}
		}
	}
}

// TestTestbedScenario_TrafficSpike checks that a healthy downstream that only
// slows down under a 19x load spike is not cut off. On spec-b, latencies rise
// in short bursts while the containers are throttled, and every request still
// succeeds. A brief trip is tolerated; rejecting traffic after the spike is not.
func TestTestbedScenario_TrafficSpike(t *testing.T) {
	sc := tbTrafficSpike()
	for _, prof := range []tbProfile{tbSpecA, tbSpecB} {
		flux := runTestbed(sc, prof, tbFluxgo(DefaultConfig()), nil)
		gb := runTestbed(sc, prof, tbGobreaker, nil)
		t.Logf("%s fluxgo:    %v", prof.name, flux)
		t.Logf("%s gobreaker: %v", prof.name, gb)

		for _, h := range gb.hops {
			if h.opened {
				t.Errorf("%s gobreaker %s opened, want never: no request fails", prof.name, h.name)
			}
		}
		if s := flux.rejectedShare(tbDuring); s > 0.05 {
			t.Errorf("%s fluxgo rejected %.1f%% of checkouts during the spike, want at most 5%%", prof.name, 100*s)
		}
		if n := flux.rejected[tbAfter]; n > 0 {
			t.Errorf("%s fluxgo rejected %d checkouts after the spike, want none", prof.name, n)
		}
		for _, h := range flux.hops {
			if !h.closedAtEnd {
				t.Errorf("%s fluxgo %s not Closed at the end", prof.name, h.name)
			}
		}
	}
}

// TestTestbedScenario_NormalLoad checks that neither breaker trips under the
// testbed's normal load.
func TestTestbedScenario_NormalLoad(t *testing.T) {
	sc := tbNormalLoad()
	for name, newBreaker := range map[string]tbBreakerFactory{
		"fluxgo":    tbFluxgo(DefaultConfig()),
		"gobreaker": tbGobreaker,
	} {
		r := runTestbed(sc, tbSpecB, newBreaker, nil)
		t.Logf("%-9s %v", name, r)
		for _, h := range r.hops {
			if h.opened {
				t.Errorf("%s %s opened, want no trip under normal load", name, h.name)
			}
		}
	}
}
