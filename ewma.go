// ewma.go implements the Exponentially Weighted Moving Average engine
// used to compute a smoothed real-time latency signal.
package fluxgo

import "sync"

// EWMA computes an exponentially weighted moving average of observed latencies.
type EWMA struct {
	alpha		float64
	value		float64
	initialized	bool
	mu			sync.RWMutex
}

// NewEWMA returns a new EWMA engine using the given smoothing factor alpha.
func NewEWMA(alpha float64) *EWMA{
	return &EWMA{alpha: alpha}
}

// Update incorporates a new latency observation and returns the updated smoothed value.
func (e *EWMA) Update(latency float64) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.initialized {
		e.value = latency 
		e.initialized = true
		return e.value
	}

	e.value = e.alpha*latency + (1-e.alpha)*e.value
	return e.value
}

// Value returns the current smoothed value without incorporating a new observation.
func (e *EWMA) Value() float64{
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.value
}

// Reset clears the EWMA state, discarding all previous observations
func (e *EWMA) Reset(){
	e.mu.Lock()
	defer e.mu.Unlock()
	e.value = 0
	e.initialized = false
}