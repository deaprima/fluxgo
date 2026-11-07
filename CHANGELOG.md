# Changelog

## v0.3.0 - Unreleased

### Changed
- A baseline is captured only once the rolling window is full, and a valid
  capture also needs `MinRequests` outcomes. Warm-up can therefore last longer
  than `WarmupDuration` under low traffic.
- On recovery from Half-Open to Closed, the EWMA and rolling percentile are reset
  and the baseline is kept. Until `MinRequests` new latencies arrive, the circuit
  uses `ThetaBase`, and `SlowRefRecent` classification is inactive.
- Latencies of requests admitted before the latest transition are ignored.
- A panic in `req` now propagates without being recovered and re-raised.
- The minimum Go version is lowered from 1.26.4 to 1.21.

### Added
- `Config.SlowRequestReference`: classify slow requests against the recent
  percentile (`SlowRefRecent`, default) or the healthy baseline (`SlowRefBaseline`).
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
Code that only uses `CircuitBreaker` is not affected. These exported component
methods changed:
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
