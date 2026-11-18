# Changelog

## v0.3.0 - Unreleased

### Changed
- By default, a successful request is classified as slow when its latency
  exceeds the healthy baseline P95 × 3.0 (`SlowRefBaseline`,
  `SlowRequestMargin` 3.0), instead of the current P95 × 2.0. With the current
  P95 as reference, a downstream that slowed down without returning errors
  never opened the circuit. Set `SlowRequestReference` to `SlowRefRecent` and
  `SlowRequestMargin` to 2.0 for the previous behavior.
- A baseline is captured only once the rolling window is full, and a valid
  capture also needs `MinRequests` outcomes. Warm-up can therefore last longer
  than `WarmupDuration` under low traffic.
- On recovery from Half-Open to Closed, the EWMA and rolling percentile are reset
  and the baseline is kept. Until `MinRequests` new latencies arrive, the circuit
  uses `ThetaBase`, and `SlowRefRecent` classification is inactive.
- Latencies of requests admitted before the latest transition are ignored.
- A Half-Open probe is judged by `IsSuccessful` only: a probe that succeeds
  but is classified as slow now closes the circuit instead of reopening it for
  another `RecoveryTimeout`. During a traffic spike, one probe just above the
  slow limit kept a healthy downstream cut off after the spike had ended. If
  the downstream is still slow, the circuit opens again after `MinDwellTime`
  and `SlowTripDuration`.
- The adaptive threshold `theta_t = ThetaBase / Dt` now applies to the error
  rate only. The failure rate with slow requests counted is compared with
  `ThetaBase` (clamped to `[ThetaMin, ThetaMax]`). Before, rising latency both
  turned requests slow and lowered the threshold they were compared with, so
  during a traffic spike slow requests making up 13% of the window were enough
  to open the circuit. Without errors, the circuit now opens once 60% of the
  window is slow, at any latency.
- A panic in `req` now propagates without being recovered and re-raised.
- The minimum Go version is lowered from 1.26.4 to 1.21.

### Added
- `Config.ConsecutiveFailures` (default 5): a Closed circuit opens after that
  many errors in a row, regardless of the failure rate and `MinRequests`. Slow
  requests do not count. Before, a total failure after a healthy period needed
  about 60 failed requests to fill the outcome window. Set it to 0 for the
  previous behavior.
- `Config.SlowRequestReference`: classify slow requests against the recent
  percentile (`SlowRefRecent`) or the healthy baseline (`SlowRefBaseline`,
  default).
- `Config.SlowTripDuration` (default 1s): the failure rate with slow requests
  counted must stay at or above its threshold for this long, over at least
  `WindowSize` new outcomes, before the circuit opens. Errors still open it at
  once. At high throughput the outcome window
  covers a few milliseconds, so a single CPU throttling stall during a traffic
  spike filled it with slow requests and opened the circuit on a healthy
  downstream. Set it to 0 for the previous behavior.
- `Settings.IsSuccessful`, with the same semantics as gobreaker.
- MIT license.
- Fuzz test, scenario tests, runnable examples, and a benchmark comparison with
  gobreaker in the separate `bench` module.

### Fixed
- Half-Open no longer gets stuck when a probe never completes: once every probe
  slot is taken and `RecoveryTimeout` has passed, the circuit returns to Open.
- `runtime.Goexit` inside `req` is recorded as a failure and no longer leaks a
  probe slot.
- `Counts` saturates at `math.MaxUint32` instead of wrapping around.

### Breaking changes
- `Config.RandomSeed` is removed. The library never used it; seed the testbed
  or load generator instead.
- These exported component methods changed. Code that only uses
  `CircuitBreaker` is not affected by them:
  - `BaselineEstimator.Observe` now takes `outcomes` and `samples` arguments.
  - `StateMachine.AllowRequest` now returns a transition instead of a `bool`.

## v0.2.2 - 2026-10-12
- `RollingPercentile.Update` uses in-place insertion: O(n), zero allocations.

## v0.2.1 - 2026-10-11
- Limit Half-Open probes with `Config.HalfOpenMaxRequests` and `ErrTooManyRequests`.
- Track generations to discard results of stale requests.
- Record a panicking request as a failure.
- Only latencies of successful requests feed the latency signals.
- Stricter `Config.Validate`.

## v0.2.0 - 2026-10-08
- Warm-up starts at the first request.
- Failure rate is computed over a sliding `OutcomeWindow`.
- The circuit can trip during warm-up.

## v0.1.0 - 2026-10-01
- First release: adaptive threshold from EWMA and rolling percentile, baseline
  estimator, Closed/Open/Half-Open state machine, slow request classification,
  and `Snapshot`.
