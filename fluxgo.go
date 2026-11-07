// Package fluxgo provides an adaptive circuit breaker for Go services.
// It computes dynamic failure thresholds based on real-time latency
// distribution using EWMA and rolling percentile (P95/P99), and follows the
// calling pattern of gobreaker (Execute, State, Counts).
package fluxgo
