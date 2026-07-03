package adsreceiver

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const ringOverflowMetricName = "otelcol_ads.ring.overflow_total"

// newRingOverflowCounter creates the shared self-telemetry counter instrument
// used by both signals to report ring buffer overflows. Called once per
// signal at construction time from its own MeterProvider.
func newRingOverflowCounter(mp metric.MeterProvider) (metric.Int64Counter, error) {
	meter := mp.Meter("github.com/nepionic/otelcol-ads/receiver/adsreceiver")
	return meter.Int64Counter(ringOverflowMetricName,
		metric.WithDescription("Entries lost to ring buffer overflow (consumer fell behind, or PLC-reported), by ring type."),
		metric.WithUnit("1"),
	)
}

// reportRingOverflow is the single place both signals report a ring buffer
// overflow. This is self-telemetry only (zap log + counter) - ring overflow
// describes the health of the collection mechanism itself, not the PLC (the
// actual target of the telemetry), so it deliberately does not ride in the
// business logs:/metrics: pipelines. ringType is "log" or "metric". counter
// may be nil (if instrument creation failed), in which case only the zap log
// fires.
func reportRingOverflow(logger *zap.Logger, counter metric.Int64Counter, ringType, symbol string, lostCount, capacity, plcOverflowTotal uint32) {
	logger.Warn("Ring buffer overflowed, entries lost",
		zap.String("ring", ringType),
		zap.String("symbol", symbol),
		zap.Uint32("lost_count", lostCount),
		zap.Uint32("capacity", capacity),
		zap.Uint32("plc_reported_overflow_total", plcOverflowTotal),
	)
	if counter != nil {
		counter.Add(context.Background(), int64(lostCount),
			metric.WithAttributes(attribute.String("ring", ringType), attribute.String("symbol", symbol)),
		)
	}
}
