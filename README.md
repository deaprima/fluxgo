# fluxgo

An adaptive circuit breaker for Go services, API-compatible with
[gobreaker](https://github.com/sony/gobreaker).

fluxgo computes a dynamic failure-rate threshold (**θₜ**) from real-time latency
signals (EWMA + P95/P99 rolling percentile) instead of relying on a fixed
threshold. The result is a circuit breaker that opens earlier when latency is
degrading and stays tolerant when the service is performing well.

## Installation

```bash
go get github.com/deaprima/fluxgo
```

## Quick Start

```go
import "github.com/deaprima/fluxgo"

cb := fluxgo.NewCircuitBreaker(fluxgo.Settings{
    Name:   "payment-service",
    Config: fluxgo.DefaultConfig(),
    OnStateChange: func(name string, from, to fluxgo.State) {
        log.Printf("[%s] %s -> %s", name, from, to)
    },
})

result, err := cb.Execute(func() (interface{}, error) {
    return callDownstream()
})
if errors.Is(err, fluxgo.ErrOpenState) {
    // circuit is open — apply fallback
} else if errors.Is(err, fluxgo.ErrTooManyRequests) {
    // circuit is half-open and probe quota is saturated
}
```

## Config Parameters

| Parameter | Default | Description |
|---|---|---|
| `Alpha` | `0.3` | EWMA smoothing factor (0 < α ≤ 1). Locked in range 0.1–0.3. |
| `WindowSize` | `100` | Rolling percentile window size. Locked; not a tuning parameter. |
| `PercentileTarget` | `95` | Latency percentile to track: 95 or 99. Locked at 95. |
| `W1` | `0.4` | Weight of EWMA signal in degradation ratio |
| `W2` | `0.6` | Weight of P95/P99 signal (W1 + W2 = 1) |
| `ThetaBase` | `0.6` | Baseline failure-rate threshold |
| `ThetaMin` | `0.05` | Minimum adaptive threshold (clamp floor) |
| `ThetaMax` | `0.9` | Maximum adaptive threshold (clamp ceiling) |
| `WarmupDuration` | `30s` | Observation period before adaptive threshold activates. 0 = no warmup. |
| `WarmupMaxFailRate` | `0.05` | Max failure rate for a valid warmup capture |
| `RecoveryTimeout` | `60s` | Time Open before transitioning to Half-Open |
| `MinDwellTime` | `5s` | Minimum time between state transitions (anti-flapping) |
| `MinRequests` | `20` | Minimum volume in outcome window before evaluating failure rate |
| `HalfOpenMaxRequests` | `1` | Max concurrent probe requests admitted while Half-Open |
| `SlowRequestMargin` | `2.0` | Multiplier on Pt; requests with latency > Pt × margin are classified as degraded. Locked at 2.0. |
| `SlowRequestReference` | `SlowRefRecent` | Reference for slow request classification: `SlowRefRecent` (current Pt) or `SlowRefBaseline` (healthy Pbase once a valid baseline is captured). See [Slow Request Classification](#slow-request-classification). |
| `RandomSeed` | `0` | Optional seed for reproducible testbed runs |

## Adaptive Threshold Formula

```
R_ewma  = St / Sbase           (EWMA ratio vs healthy baseline)
R_p     = Pt / Pbase           (P95/P99 ratio vs healthy baseline)
Dt      = W1*R_ewma + W2*R_p   (weighted degradation ratio)
theta_t = clamp(ThetaBase/Dt, ThetaMin, ThetaMax)
```

When latency doubles (`Dt = 2`), `theta_t` halves — the circuit trips at half
the failure rate compared to a static threshold. When latency improves
(`Dt < 1`), `theta_t` rises — the circuit becomes more tolerant.

## Warmup Period

Warm-up starts at the first request (not at construction) and lasts at least
`WarmupDuration` (default 30s). It also continues until the rolling percentile
window holds `WindowSize` latency samples, so the baseline is never taken from
a handful of requests; under low traffic, warm-up can therefore last longer
than `WarmupDuration`. During warm-up, the circuit trips on the static
`ThetaBase` threshold, preventing false trips on cold-start traffic.

A baseline is captured as valid only when the circuit is Closed, the outcome
window holds at least `MinRequests` outcomes, and the failure rate is below
`WarmupMaxFailRate`. Otherwise the current signals are kept as a temporary
fallback baseline, and fluxgo keeps trying to capture a valid one.

## Anti-Flapping

fluxgo enforces a minimum dwell time (`MinDwellTime`) between consecutive
state transitions. After any transition, the circuit cannot trip to Open again
until `MinDwellTime` has elapsed. This prevents rapid oscillation when the
failure rate hovers near the adaptive threshold.

> **Note:** `MinDwellTime` is an implementation safeguard not described in the
> original circuit breaker design. It is documented as a justified deviation
> because it prevents instability at the cost of slightly delaying re-trip.

## Half-Open Timeout

If every Half-Open probe slot is taken and no probe has completed within
`RecoveryTimeout` (for example, because the downstream hangs and the caller
sets no timeout), the circuit returns to Open instead of rejecting requests
with `ErrTooManyRequests` forever. A late result from such a probe is ignored,
and a new probe is admitted after the next `RecoveryTimeout`.

## Recovery Behavior

When a probe succeeds and the circuit moves from Half-Open to Closed, the EWMA
and rolling percentile are reset, because they still describe the degradation
that tripped the circuit. The healthy baseline (`Sbase`, `Pbase`) is kept.

Until the rolling window holds `MinRequests` new latency samples, the circuit
trips on the static `ThetaBase` threshold and slow request classification is
inactive. After that, the adaptive threshold resumes with signals that reflect
the downstream's current behavior. This prevents a recovered service from being
tripped again by stale latency data.

Latencies of requests admitted before the latest state transition are ignored,
just like their outcomes.

## Slow Request Classification

Beyond explicit errors, fluxgo classifies a request as **degraded** if its
latency exceeds `Pt × SlowRequestMargin` (default: P95 × 2.0). A degraded
request is counted as a failure in the internal failure rate.

```
latency > P95 × 2.0  →  treated as failure internally
                          result/error returned to caller is unchanged
```

**Rules:**
- Classification is inactive during warmup and while the rolling window holds fewer than `MinRequests` latencies (for example, right after recovery)
- The percentile snapshot is taken **before** the current latency enters the window (no self-comparison)
- The result and error values returned by `Execute()` to the caller are **not affected** — gobreaker API compatibility is preserved
- Degraded requests are observable via `Snapshot().DegradedRequests`

**Reference (`SlowRequestReference`):**

| Mode | Reference | Behavior |
|---|---|---|
| `SlowRefRecent` (default) | `Pt × SlowRequestMargin` | Tolerant of traffic spikes, but the reference rises with the latency, so a degradation without errors never trips the circuit |
| `SlowRefBaseline` | `Pbase × SlowRequestMargin` | Detects gradual degradation; sustained spikes above the margin are also classified as slow |

`SlowRefBaseline` uses the baseline only once a valid one has been captured;
during warm-up and while only a fallback baseline exists, it behaves like
`SlowRefRecent`.

> **Note:** fluxgo needs a failure signal (an error or a slow request) to trip.
> Rising latency on its own only lowers the threshold `theta_t`; with
> `SlowRefRecent`, a downstream that becomes slower without returning errors
> keeps the circuit closed.

## Ignoring Errors

By default, any non-nil error counts as a failure. Use `Settings.IsSuccessful`
(same semantics as gobreaker) to count errors that do not indicate a
downstream failure as successes:

```go
cb := fluxgo.NewCircuitBreaker(fluxgo.Settings{
    Config: fluxgo.DefaultConfig(),
    IsSuccessful: func(err error) bool {
        return err == nil || errors.Is(err, context.Canceled)
    },
})
```

The error is still returned to the caller unchanged. If the request panics or
calls `runtime.Goexit`, it is always recorded as a failure.

## Observability

`Snapshot()` returns a point-in-time read-only view of all internal signals:

```go
snap := cb.Snapshot()
fmt.Println(snap.State)            // Closed / Open / HalfOpen
fmt.Println(snap.St)               // current EWMA latency (ms)
fmt.Println(snap.Pt)               // current P95/P99 (ms)
fmt.Println(snap.ThetaT)           // current adaptive threshold
fmt.Println(snap.FailureRate)      // current failure rate
fmt.Println(snap.DegradedRequests) // slow requests classified as degraded
fmt.Println(snap.IsWarmingUp)      // warmup still active?
```

This is intended for the testbed's `GET /debug/cb` endpoint and external
dashboard tooling. All fields are read under their respective component locks.

## Algorithm Complexity

| Component | Time | Space |
|---|---|---|
| EWMA update | O(1) | O(1) |
| Rolling Percentile update | O(n) | O(n) (zero-allocation) |
| Rolling Percentile read (`Percentiles`/`Current`) | O(1) | O(1) |
| Threshold calculation | O(1) | O(1) |
| `Execute()` total overhead | O(n) per call | O(n) |

where `n = Config.WindowSize` (default 100).

Benchmark on Intel Core i5-11300H @ 3.10GHz:

| Benchmark / State | Latency/call | Allocations |
|---|---|---|
| `RollingPercentile.Update()` | ~65 ns | 0 allocs / 0 B |
| `Execute()` Closed (full pipeline) | ~656 ns | 0 allocs / 0 B |
| `Execute()` Open (fast reject) | ~57 ns | 0 allocs / 0 B |

## API Compatibility with gobreaker

| gobreaker | fluxgo | Note |
|---|---|---|
| `NewCircuitBreaker(Settings)` | `NewCircuitBreaker(Settings)` | Same signature; panics on invalid Config |
| `cb.Execute(req)` | `cb.Execute(req)` | Identical |
| `cb.State()` | `cb.State()` | Identical |
| `cb.Counts()` | `cb.Counts()` | Same fields, different meaning: totals cover the breaker's lifetime (not cleared on state change), `TotalFailures` includes slow requests, and values saturate at `math.MaxUint32` |
| `Settings.IsSuccessful` | `Settings.IsSuccessful` | Same semantics; nil counts only nil errors as success |
| `ErrOpenState` | `ErrOpenState` | Same sentinel error |
| `ErrTooManyRequests` | `ErrTooManyRequests` | Same sentinel error when Half-Open probe quota is reached |
| — | `cb.Snapshot()` | fluxgo-only: observability accessor |

## Mode Switching for Testbed (CB_MODE)

For experiment comparison, the testbed application uses a `CB_MODE` environment
variable to select between `fluxgo` and `gobreaker` at startup without changing
application code:

```bash
CB_MODE=fluxgo    ./testbed   # use adaptive circuit breaker
CB_MODE=gobreaker ./testbed   # use static circuit breaker (control group)
```

> **Note:** `CB_MODE` is read at the **testbed level only** — it is not part of
> the fluxgo library. fluxgo has no dependency on gobreaker.

## Author

fluxgo is developed as part of a thesis research project on adaptive circuit
breakers for Go microservices.

**Dea Primatama**
GitHub: [@deaprima](https://github.com/deaprima)

For questions or feedback, open an issue on
[GitHub Issues](https://github.com/deaprima/fluxgo/issues).


