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
}
```

## Config Parameters

| Parameter | Default | Description |
|---|---|---|
| `Alpha` | `0.3` | EWMA smoothing factor (0 < α ≤ 1) |
| `WindowSize` | `100` | Rolling percentile window size |
| `PercentileTarget` | `95` | Latency percentile to track: 95 or 99 |
| `W1` | `0.4` | Weight of EWMA signal in degradation ratio |
| `W2` | `0.6` | Weight of P95/P99 signal (W1 + W2 = 1) |
| `ThetaBase` | `0.6` | Baseline failure-rate threshold |
| `ThetaMin` | `0.05` | Minimum adaptive threshold (clamp floor) |
| `ThetaMax` | `0.9` | Maximum adaptive threshold (clamp ceiling) |
| `WarmupDuration` | `30s` | Observation period before adaptive threshold activates |
| `WarmupMaxFailRate` | `0.05` | Max failure rate for a valid warmup capture |
| `RecoveryTimeout` | `60s` | Time Open before transitioning to Half-Open |
| `MinDwellTime` | `5s` | Minimum time between state transitions (anti-flapping) |
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

On startup, fluxgo observes the system for `WarmupDuration` (default 30s)
before activating the adaptive threshold. During warmup, the circuit behaves
identically to a static gobreaker threshold, preventing false trips on
cold-start traffic.

## Anti-Flapping

fluxgo enforces a minimum dwell time (`MinDwellTime`) between consecutive
state transitions. After any transition, the circuit cannot trip to Open again
until `MinDwellTime` has elapsed. This prevents rapid oscillation when the
failure rate hovers near the adaptive threshold.

## Algorithm Complexity

| Component | Time | Space |
|---|---|---|
| EWMA update | O(1) | O(1) |
| Rolling Percentile update | O(n log n) | O(n) |
| Threshold calculation | O(1) | O(1) |
| `Execute()` total overhead | O(n log n) per call | O(n) |

where `n = Config.WindowSize` (default 100).

Benchmark on Intel Core i5-11300H @ 3.10GHz:

| State | Latency/call | Allocations |
|---|---|---|
| Closed (full pipeline) | 1,294 ns | 2 allocs / 1,791 B |
| Open (fast reject) | 99 ns | 0 allocs |

## API Compatibility with gobreaker

| gobreaker | fluxgo | Note |
|---|---|---|
| `NewCircuitBreaker(Settings)` | `NewCircuitBreaker(Settings)` | Same signature |
| `cb.Execute(req)` | `cb.Execute(req)` | Identical |
| `cb.State()` | `cb.State()` | Identical |
| `cb.Counts()` | `cb.Counts()` | Compatible fields |
| `ErrOpenState` | `ErrOpenState` | Same sentinel error |

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


