// threshold_calculator.go implements AdaptiveThresholdCalculator, which
// combines EWMA and rolling percentile signals to produce a dynamic
// failure threshold (theta_t).
package fluxgo
