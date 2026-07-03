package adsreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func validMetrics() *MetricsConfig {
	return &MetricsConfig{Subscriptions: []SubscriptionConfig{{Symbol: "MAIN.x", Name: "plc.x"}}}
}

func TestValidate_MissingTargetNetID(t *testing.T) {
	c := &Config{Logs: defaultLogsConfig()}
	err := c.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target_net_id")
}

func TestValidate_RequiresLogsOrMetrics(t *testing.T) {
	c := &Config{TargetNetID: "1.1.1.1.1.1"}
	err := c.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one of logs or metrics")
}

func TestValidate_LogsOnly_OK(t *testing.T) {
	c := &Config{TargetNetID: "1.1.1.1.1.1", Logs: defaultLogsConfig()}
	assert.NoError(t, c.Validate())
}

func TestValidate_MetricsOnly_OK(t *testing.T) {
	c := &Config{TargetNetID: "1.1.1.1.1.1", Metrics: validMetrics()}
	assert.NoError(t, c.Validate())
}

func TestValidate_Both_OK(t *testing.T) {
	c := &Config{TargetNetID: "1.1.1.1.1.1", Logs: defaultLogsConfig(), Metrics: validMetrics()}
	assert.NoError(t, c.Validate())
}

func TestValidate_LogsSubscriptionCycleTimeTooLow(t *testing.T) {
	logs := defaultLogsConfig()
	logs.PushRing.SubscriptionCycleTime = 5 * time.Millisecond
	c := &Config{TargetNetID: "1.1.1.1.1.1", Logs: logs}
	err := c.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subscription_cycle_time")
}

func TestValidate_MetricsRequiresSubscriptionOrPushRing(t *testing.T) {
	c := &Config{TargetNetID: "1.1.1.1.1.1", Metrics: &MetricsConfig{}}
	err := c.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one subscription or push_ring.enabled")
}

func TestValidate_MetricsSubscriptionMissingSymbol(t *testing.T) {
	c := &Config{TargetNetID: "1.1.1.1.1.1", Metrics: &MetricsConfig{
		Subscriptions: []SubscriptionConfig{{Name: "plc.x"}},
	}}
	err := c.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "symbol is required")
}

func TestValidate_MetricsSubscriptionMissingName(t *testing.T) {
	c := &Config{TargetNetID: "1.1.1.1.1.1", Metrics: &MetricsConfig{
		Subscriptions: []SubscriptionConfig{{Symbol: "MAIN.x"}},
	}}
	err := c.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name is required")
}

func TestValidate_PushRingEnabledWithoutSymbol_OK(t *testing.T) {
	// An empty symbol means auto-discover at subscribe time; it must not be
	// a validation error.
	c := &Config{TargetNetID: "1.1.1.1.1.1", Metrics: &MetricsConfig{
		PushRing: PushRingConfig{Enabled: true},
	}}
	assert.NoError(t, c.Validate())
}

func TestConfig_Unmarshal_LogsBlockAbsent_LeavesNil(t *testing.T) {
	c := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"target_net_id": "1.1.1.1.1.1",
	})
	require.NoError(t, conf.Unmarshal(c))
	assert.Nil(t, c.Logs)
	assert.Nil(t, c.Metrics)
}

func TestConfig_Unmarshal_LogsBlockPresent_AppliesDefaults(t *testing.T) {
	c := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"target_net_id": "1.1.1.1.1.1",
		"logs":          map[string]any{},
	})
	require.NoError(t, conf.Unmarshal(c))
	require.NotNil(t, c.Logs)
	assert.True(t, c.Logs.PushRing.Enabled)
	assert.True(t, c.Logs.ConnectionEvents)
	assert.True(t, c.Logs.PLCStateChanges)
	assert.Equal(t, 100*time.Millisecond, c.Logs.PushRing.SubscriptionCycleTime)
	assert.Nil(t, c.Metrics)
}

func TestConfig_Unmarshal_MetricsBlockPresent_AppliesDefaults(t *testing.T) {
	c := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"target_net_id": "1.1.1.1.1.1",
		"metrics": map[string]any{
			"subscriptions": []any{
				map[string]any{"symbol": "MAIN.x", "name": "plc.x"},
			},
		},
	})
	require.NoError(t, conf.Unmarshal(c))
	require.NotNil(t, c.Metrics)
	assert.Empty(t, c.Metrics.PushRing.Symbol, "empty by default means auto-discover")
	assert.Equal(t, 100*time.Millisecond, c.Metrics.PushRing.SubscriptionCycleTime)
	assert.Nil(t, c.Logs)
}
