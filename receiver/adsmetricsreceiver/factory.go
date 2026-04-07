package adsmetricsreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr = "adsmetrics"
)

// NewFactory creates a new receiver.Factory for the adsmetrics receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		RouterPort:           48898,
		PLCPort:              851,
		StatePollingInterval: 2 * time.Second,
		PushRing: PushRingConfig{
			Enabled:               false,
			Symbol:                "OtelBridge.MetricRing",
			SubscriptionCycleTime: 100 * time.Millisecond,
		},
	}
}

func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	rCfg := cfg.(*Config)
	return newMetricsReceiver(set, rCfg, nextConsumer), nil
}
