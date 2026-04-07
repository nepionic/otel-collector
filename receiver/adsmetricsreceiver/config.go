package adsmetricsreceiver

import (
	"errors"
	"fmt"
	"time"

	"github.com/nepionic/otelcol-ads/internal/adsbridge"
	"go.opentelemetry.io/collector/component"
)

// MetricType mirrors the ADS flags field in ST_OtelMetricSlot and the
// human-readable type in SubscriptionConfig.
type MetricType string

const (
	MetricTypeGauge         MetricType = "gauge"
	MetricTypeCounter       MetricType = "counter"
	MetricTypeUpDownCounter MetricType = "updowncounter"
)

// SubscriptionConfig describes a single PLC variable to subscribe to (pull path).
type SubscriptionConfig struct {
	// Symbol is the ADS symbol path (e.g. "MAIN.temperature").
	Symbol string `mapstructure:"symbol"`
	// Name is the OTel metric name (e.g. "plc.reactor.temperature").
	Name string `mapstructure:"name"`
	// Unit is the OTel metric unit string (e.g. "Cel", "ms", "{items}").
	Unit string `mapstructure:"unit"`
	// Description is the OTel metric description (optional).
	Description string `mapstructure:"description"`
	// Type determines the OTel instrument kind (default: gauge).
	Type MetricType `mapstructure:"type"`
	// CycleTime is how often the PLC variable is sampled (default: 500ms).
	CycleTime time.Duration `mapstructure:"cycle_time"`
	// SendOnChange only emits a data point when the value changes (default: true).
	SendOnChange bool `mapstructure:"send_on_change"`
}

// PushRingConfig controls the optional push ring buffer path.
type PushRingConfig struct {
	// Enabled activates the push path (default: false).
	Enabled bool `mapstructure:"enabled"`
	// Symbol is the ADS symbol path of the ST_OtelMetricRing global variable
	// (default: "OtelBridge.MetricRing").
	Symbol string `mapstructure:"symbol"`
	// SubscriptionCycleTime is the ADS notification cycle time for write_index
	// (default: 100ms).
	SubscriptionCycleTime time.Duration `mapstructure:"subscription_cycle_time"`
}

// Config holds the full configuration for the ADS metrics receiver.
type Config struct {
	// TargetNetID is the AMS Net ID of the target TwinCAT system (required).
	TargetNetID string `mapstructure:"target_net_id"`
	// RouterAddr is the IP address of the ADS router (empty = local router).
	RouterAddr string `mapstructure:"router_addr"`
	// RouterPort is the TCP port of the ADS router (default: 48898).
	RouterPort uint16 `mapstructure:"router_port"`
	// PLCPort is the ADS port of the PLC runtime (default: 851).
	PLCPort uint16 `mapstructure:"plc_port"`
	// StatePollingInterval controls TwinCAT restart detection polling (default: 2s).
	StatePollingInterval time.Duration `mapstructure:"state_polling_interval"`

	// Subscriptions lists PLC variables to subscribe to (pull path).
	// At least one subscription or PushRing.Enabled must be set.
	Subscriptions []SubscriptionConfig `mapstructure:"subscriptions"`

	// PushRing configures the push ring buffer path.
	PushRing PushRingConfig `mapstructure:"push_ring"`
}

var _ component.Config = (*Config)(nil)

// Validate returns an error if the configuration is invalid.
func (c *Config) Validate() error {
	if c.TargetNetID == "" {
		return errors.New("adsmetricsreceiver: target_net_id is required")
	}
	if len(c.Subscriptions) == 0 && !c.PushRing.Enabled {
		return errors.New("adsmetricsreceiver: at least one subscription or push_ring.enabled must be configured")
	}
	for i, s := range c.Subscriptions {
		if s.Symbol == "" {
			return fmt.Errorf("adsmetricsreceiver: subscriptions[%d].symbol is required", i)
		}
		if s.Name == "" {
			return fmt.Errorf("adsmetricsreceiver: subscriptions[%d].name is required", i)
		}
	}
	if c.PushRing.Enabled && c.PushRing.Symbol == "" {
		return errors.New("adsmetricsreceiver: push_ring.symbol must not be empty when push_ring.enabled is true")
	}
	return nil
}

// toBridgeConfig converts the receiver Config to the shared adsbridge.Config.
func (c *Config) toBridgeConfig() adsbridge.Config {
	return adsbridge.Config{
		TargetNetID:          c.TargetNetID,
		RouterAddr:           c.RouterAddr,
		RouterPort:           c.RouterPort,
		PLCPort:              c.PLCPort,
		StatePollingInterval: c.StatePollingInterval,
	}
}
