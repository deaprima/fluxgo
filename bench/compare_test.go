// Package bench compares the overhead of fluxgo and gobreaker. It lives in its
// own module so that gobreaker never becomes a dependency of fluxgo.
package bench

import (
	"errors"
	"testing"

	"github.com/deaprima/fluxgo"
	"github.com/sony/gobreaker"
)

var errDownstream = errors.New("downstream failure")

func succeed() (interface{}, error) { return nil, nil }
func fail() (interface{}, error)    { return nil, errDownstream }

func newFluxgo() *fluxgo.CircuitBreaker {
	cfg := fluxgo.DefaultConfig()
	cfg.WarmupDuration = 0
	cfg.MinDwellTime = 0
	cfg.MinRequests = 1
	cfg.ThetaBase = 0.5
	return fluxgo.NewCircuitBreaker(fluxgo.Settings{Name: "bench", Config: cfg})
}

func newGobreaker() *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "bench",
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.TotalFailures*2 >= c.Requests
		},
	})
}

// executor is the Execute method shared by both circuit breakers.
type executor interface {
	Execute(func() (interface{}, error)) (interface{}, error)
}

func benchmarkSerial(b *testing.B, cb executor) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Execute(succeed)
	}
}

func benchmarkParallel(b *testing.B, cb executor) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(succeed)
		}
	})
}

func benchmarkOpen(b *testing.B, cb executor) {
	cb.Execute(fail)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(succeed)
		}
	})
}

func BenchmarkClosedSerial_Fluxgo(b *testing.B)      { benchmarkSerial(b, newFluxgo()) }
func BenchmarkClosedSerial_Gobreaker(b *testing.B)   { benchmarkSerial(b, newGobreaker()) }
func BenchmarkClosedParallel_Fluxgo(b *testing.B)    { benchmarkParallel(b, newFluxgo()) }
func BenchmarkClosedParallel_Gobreaker(b *testing.B) { benchmarkParallel(b, newGobreaker()) }
func BenchmarkOpen_Fluxgo(b *testing.B)              { benchmarkOpen(b, newFluxgo()) }
func BenchmarkOpen_Gobreaker(b *testing.B)           { benchmarkOpen(b, newGobreaker()) }
