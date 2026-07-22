// metric_collector.go implements MetricCollector, which records per-request
// latency and success status, forwarding data to the EWMA and rolling
// percentile engines.
package fluxgo
