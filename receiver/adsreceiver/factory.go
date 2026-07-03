package adsreceiver

import (
	"context"
	"fmt"
	"time"

	"github.com/nepionic/otelcol-ads/internal/sharedcomponent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const typeStr = "ads"

// receivers caches one *adsCore per receiver instance (component.ID), so that
// when the same receiver ID is referenced from both a logs pipeline and a
// metrics pipeline, exactly one ADS connection is created and shared.
var receivers = sharedcomponent.NewMap[component.ID, *adsCore]()

// NewFactory creates a new receiver.Factory for the ads receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha),
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		RouterPort:                  48898,
		PLCPort:                     851,
		StatePollingInterval:        2 * time.Second,
		ConnectRetryInitialInterval: 1 * time.Second,
		ConnectRetryMaxInterval:     30 * time.Second,
		// Logs/Metrics intentionally left nil - see Config.Unmarshal.
	}
}

func getOrCreateCore(set receiver.Settings, cfg *Config) (*sharedcomponent.Component[*adsCore], error) {
	return receivers.LoadOrStore(set.ID, func() (*adsCore, error) {
		return newCore(set.ID, cfg, set.TelemetrySettings.Logger), nil
	})
}

func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Logs,
) (receiver.Logs, error) {
	rCfg := cfg.(*Config)
	if rCfg.Logs == nil {
		return nil, fmt.Errorf("ads receiver %q: used in a logs pipeline but no logs: block is configured", set.ID)
	}

	comp, err := getOrCreateCore(set, rCfg)
	if err != nil {
		return nil, err
	}

	signal := newLogsSignal(rCfg.Logs, comp.Unwrap(), set, next)
	if err := comp.Unwrap().registerLogs(signal); err != nil {
		return nil, err
	}
	return comp, nil
}

func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (receiver.Metrics, error) {
	rCfg := cfg.(*Config)
	if rCfg.Metrics == nil {
		return nil, fmt.Errorf("ads receiver %q: used in a metrics pipeline but no metrics: block is configured", set.ID)
	}

	comp, err := getOrCreateCore(set, rCfg)
	if err != nil {
		return nil, err
	}

	signal := newMetricsSignal(rCfg.Metrics, comp.Unwrap(), set, next)
	if err := comp.Unwrap().registerMetrics(signal); err != nil {
		return nil, err
	}
	return comp, nil
}
