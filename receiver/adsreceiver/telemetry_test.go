package adsreceiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewRingOverflowCounter(t *testing.T) {
	counter, err := newRingOverflowCounter(noop.NewMeterProvider())
	require.NoError(t, err)
	require.NotNil(t, counter)
}

func TestReportRingOverflow(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	counter, err := newRingOverflowCounter(noop.NewMeterProvider())
	require.NoError(t, err)

	// Should not panic with a real (noop) counter.
	reportRingOverflow(logger, counter, "log", "Main.log_appender.ring", 5, 128, 12)

	require.Equal(t, 1, logs.Len())
	entry := logs.All()[0]
	require.Equal(t, "Ring buffer overflowed, entries lost", entry.Message)

	fields := entry.ContextMap()
	require.Equal(t, "log", fields["ring"])
	require.Equal(t, "Main.log_appender.ring", fields["symbol"])
}

func TestReportRingOverflow_NilCounter(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	// Must not panic when counter is nil (e.g. instrument creation failed).
	reportRingOverflow(logger, nil, "metric", "OtelBridge.MetricRing", 3, 2048, 0)

	require.Equal(t, 1, logs.Len())
}
