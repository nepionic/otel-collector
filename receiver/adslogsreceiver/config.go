package adslogsreceiver

import (
	"errors"
	"time"

	"github.com/nepionic/otelcol-ads/internal/adsbridge"
	"go.opentelemetry.io/collector/component"
)

// Config holds the configuration for the ADS logs receiver.
type Config struct {
	// TargetNetID is the AMS Net ID of the target TwinCAT system (required).
	// Examples: "192.168.1.1.1.1", "localhost", "127.0.0.1.1.1"
	TargetNetID string `mapstructure:"target_net_id"`

	// RouterAddr is the IP address or hostname of the ADS router.
	// Leave empty when a local TwinCAT router (Windows) is available.
	// Set to the PLC's IP address for direct TCP connections (Linux/BSD).
	RouterAddr string `mapstructure:"router_addr"`

	// RouterPort is the TCP port of the ADS router (default: 48898).
	RouterPort uint16 `mapstructure:"router_port"`

	// PLCPort is the ADS port of the PLC runtime (default: 851 for TC3 runtime 1).
	PLCPort uint16 `mapstructure:"plc_port"`

	// LogRingSymbol is the ADS symbol path of the ST_OtelLogRing variable
	// published by the TwinCAT companion library (default: "OtelBridge.LogRing").
	LogRingSymbol string `mapstructure:"log_ring_symbol"`

	// StatePollingInterval controls how often TwinCAT state is polled for
	// restart detection (default: 2s).
	StatePollingInterval time.Duration `mapstructure:"state_polling_interval"`

	// SubscriptionCycleTime is the ADS notification cycle time used for the
	// write_index subscription (default: 100ms).
	SubscriptionCycleTime time.Duration `mapstructure:"subscription_cycle_time"`
}

var _ component.Config = (*Config)(nil)

// Validate checks that required fields are set and values are in range.
func (c *Config) Validate() error {
	if c.TargetNetID == "" {
		return errors.New("adslogsreceiver: target_net_id is required")
	}
	if c.LogRingSymbol == "" {
		return errors.New("adslogsreceiver: log_ring_symbol must not be empty")
	}
	if c.SubscriptionCycleTime < 10*time.Millisecond {
		return errors.New("adslogsreceiver: subscription_cycle_time must be at least 10ms")
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
