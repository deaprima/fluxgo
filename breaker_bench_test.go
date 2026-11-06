// breaker_bench_test.go benchmarks Execute() overhead to verify NF2
// (latency overhead < 1ms per request).
package fluxgo

import "testing"

func BenchmarkExecute_Closed(b *testing.B) {
	cb := NewCircuitBreaker(defaultSettings())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(func() (interface{}, error) {
				return nil, nil
			})
		}
	})
}

func BenchmarkExecute_Open(b *testing.B) {
	s := defaultSettings()
	s.Config.ThetaBase = 0.5
	cb := NewCircuitBreaker(s)
	for i := 0; i < 10; i++ {
		cb.Execute(func() (interface{}, error) { return nil, errDownstream })
	}
	// circuit is now Open
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(func() (interface{}, error) { return nil, nil })
		}
	})
}

// BenchmarkExecute_Closed_Serial measures the pipeline without lock contention,
// as a reference for the parallel BenchmarkExecute_Closed.
func BenchmarkExecute_Closed_Serial(b *testing.B) {
	cb := NewCircuitBreaker(defaultSettings())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Execute(func() (interface{}, error) {
			return nil, nil
		})
	}
}
