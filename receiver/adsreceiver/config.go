package adsreceiver

import (
	"errors"
	"fmt"
	"time"

	"github.com/nepionic/otelcol-ads/internal/adsbridge"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

// Config holds the configuration for the ADS receiver. TargetNetID and the
// connection fields are shared by both signals; Logs and Metrics are nil
// unless their corresponding YAML key is present, which is what lets a single
// receiver instance be used for logs only, metrics only, or both.
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

	// StatePollingInterval controls how often TwinCAT state is polled for
	// restart detection (default: 2s).
	StatePollingInterval time.Duration `mapstructure:"state_polling_interval"`

	// ConnectRetryInitialInterval is the first backoff interval after a failed
	// connection attempt (default: 1s).
	ConnectRetryInitialInterval time.Duration `mapstructure:"connect_retry_initial_interval"`

	// ConnectRetryMaxInterval caps the exponential backoff (default: 30s).
	ConnectRetryMaxInterval time.Duration `mapstructure:"connect_retry_max_interval"`

	// Logs configures the log-ring/TwinCAT-logger signal. Leave the `logs:`
	// key absent entirely to disable this signal for this receiver instance.
	Logs *LogsConfig `mapstructure:"logs"`

	// Metrics configures the pull-subscription/push-ring signal. Leave the
	// `metrics:` key absent entirely to disable this signal for this
	// receiver instance.
	Metrics *MetricsConfig `mapstructure:"metrics"`
}

// LogsConfig configures the logs signal of the ADS receiver. PushRing
// controls the PLC log ring path; the remaining fields are collector- and
// TwinCAT-generated events that work independently of the ring (they still
// fire even when push_ring.enabled is false).
type LogsConfig struct {
	// PLCStateChanges emits a log record on each TwinCAT ADS state transition,
	// e.g. Run → Stop, Config → Run, or when the initial state is first read.
	// Useful for correlating application failures with PLC lifecycle events.
	// Default: true.
	PLCStateChanges bool `mapstructure:"plc_state_changes"`

	// ConnectionEvents emits log records when the collector establishes a
	// connection, loses the connection, or reconnects after a TwinCAT restart.
	// Default: true.
	ConnectionEvents bool `mapstructure:"connection_events"`

	// TCLogger subscribes to the TwinCAT system logger (ADS port 100) and
	// forwards its messages as structured log records. This captures runtime
	// messages from TwinCAT subsystems (TCNC, PLC task manager, system
	// services) that are not written by PLC application code and therefore
	// never appear in the OtelBridge ring buffer.
	// Default: false.
	TCLogger bool `mapstructure:"twincat_logger"`

	// PushRing configures the PLC log ring buffer path.
	PushRing LogRingConfig `mapstructure:"push_ring"`
}

// LogRingConfig controls the log ring buffer path — the mechanism for
// reading structured PLC application log records (requires the
// Nepionic_Log_OTel PLC library).
type LogRingConfig struct {
	// Enabled activates the log ring path (default: true). Set to false to
	// rely solely on connection_events/plc_state_changes/twincat_logger
	// without requiring the OtelLogRing PLC library at all.
	Enabled bool `mapstructure:"enabled"`

	// Symbol is the ADS symbol path of the OTelLogRing variable.
	// Leave empty (default) to auto-discover via the PLC pragma
	// {attribute 'otelcol_role' := 'log_ring'}.
	Symbol string `mapstructure:"symbol"`

	// SubscriptionCycleTime is the ADS notification cycle time used for the
	// write_index subscription (default: 100ms).
	SubscriptionCycleTime time.Duration `mapstructure:"subscription_cycle_time"`

	// RingOverflows emits a warning log record when the PLC writes entries to
	// the ring faster than the collector drains them, causing data loss.
	// Default: true.
	RingOverflows bool `mapstructure:"ring_overflows"`

	// Heartbeat reports a otelcol_ads.plc.heartbeat_epoch_s self-telemetry
	// gauge from the ring header's heartbeat field, whenever the PLC has ever
	// called its OTelLogAppender's Heartbeat() method (default: false -
	// opt-in, since the PLC side must also actively populate the field for
	// this to mean anything).
	Heartbeat bool `mapstructure:"heartbeat"`
}

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
	// Attributes are static key/value pairs stamped onto every data point for
	// this subscription. Use these to add dimensionality when subscribing to
	// the same metric on multiple axes, stations, or products.
	//
	// Example:
	//   attributes:
	//     axis.id: "3"
	//     axis.name: "feed"
	Attributes map[string]string `mapstructure:"attributes"`
}

// MetricsConfig configures the metrics signal of the ADS receiver.
type MetricsConfig struct {
	// Subscriptions lists PLC variables to subscribe to (pull path).
	// At least one subscription or PushRing.Enabled must be set.
	Subscriptions []SubscriptionConfig `mapstructure:"subscriptions"`

	// PushRing configures the push ring buffer path.
	PushRing PushRingConfig `mapstructure:"push_ring"`
}

// PushRingConfig controls the optional push ring buffer path.
type PushRingConfig struct {
	// Enabled activates the push path (default: false).
	Enabled bool `mapstructure:"enabled"`
	// Symbol is the ADS symbol path of the OTelMetricRing variable.
	// Leave empty (default) to auto-discover via the PLC pragma
	// {attribute 'otelcol_role' := 'metric_ring'}, falling back to scanning
	// for a variable declared as OTelMetricRing. There should only ever be
	// one metric ring on a PLC, so auto-discovery is preferred over pinning
	// an explicit path.
	Symbol string `mapstructure:"symbol"`
	// SubscriptionCycleTime is the ADS notification cycle time for write_index
	// (default: 100ms).
	SubscriptionCycleTime time.Duration `mapstructure:"subscription_cycle_time"`

	// Heartbeat emits a otelcol_ads.plc.heartbeat_epoch_s gauge metric from the
	// ring header's heartbeat field, whenever the PLC has ever called its
	// OTelMetricCore's Heartbeat() method (default: false - opt-in, since the
	// PLC side must also actively populate the field for this to mean
	// anything).
	Heartbeat bool `mapstructure:"heartbeat"`
}

var (
	_ component.Config    = (*Config)(nil)
	_ confmap.Unmarshaler = (*Config)(nil)
)

// Unmarshal implements confmap.Unmarshaler. It allocates Logs/Metrics (with
// their own field defaults) only when the corresponding YAML key is present.
// This is what makes their nil-ness a reliable signal of "was this signal
// configured" for Validate() to check — createDefaultConfig cannot pre-fill
// these fields without making them permanently non-nil. The self-recursive
// call to conf.Unmarshal(c) is the documented confmap pattern for this: confmap
// detects it is already inside this type's Unmarshal and performs a plain
// field decode instead of calling back into Unmarshal again.
func (c *Config) Unmarshal(conf *confmap.Conf) error {
	if conf.IsSet("logs") && c.Logs == nil {
		c.Logs = defaultLogsConfig()
	}
	if conf.IsSet("metrics") && c.Metrics == nil {
		c.Metrics = defaultMetricsConfig()
	}
	return conf.Unmarshal(c)
}

func defaultLogsConfig() *LogsConfig {
	return &LogsConfig{
		PLCStateChanges:  true,
		ConnectionEvents: true,
		PushRing: LogRingConfig{
			Enabled:               true,
			SubscriptionCycleTime: 100 * time.Millisecond,
			RingOverflows:         true,
		},
	}
}

func defaultMetricsConfig() *MetricsConfig {
	return &MetricsConfig{
		PushRing: PushRingConfig{
			SubscriptionCycleTime: 100 * time.Millisecond,
		},
	}
}

// Validate checks that required fields are set and values are in range.
func (c *Config) Validate() error {
	if c.TargetNetID == "" {
		return errors.New("ads: target_net_id is required")
	}
	if c.Logs == nil && c.Metrics == nil {
		return errors.New("ads: at least one of logs or metrics must be configured")
	}
	if c.Logs != nil && c.Logs.PushRing.SubscriptionCycleTime < 10*time.Millisecond {
		return errors.New("ads: logs.push_ring.subscription_cycle_time must be at least 10ms")
	}
	if c.Metrics != nil {
		if len(c.Metrics.Subscriptions) == 0 && !c.Metrics.PushRing.Enabled {
			return errors.New("ads: metrics requires at least one subscription or push_ring.enabled")
		}
		for i, s := range c.Metrics.Subscriptions {
			if s.Symbol == "" {
				return fmt.Errorf("ads: metrics.subscriptions[%d].symbol is required", i)
			}
			if s.Name == "" {
				return fmt.Errorf("ads: metrics.subscriptions[%d].name is required", i)
			}
		}
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
