// Package adsbridge provides a shared ADS client wrapper used by both
// the adslogsreceiver and adsmetricsreceiver packages.
package adsbridge

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jarmocluyse/ads-go/pkg/ads"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config holds the ADS connection parameters shared between both receivers.
type Config struct {
	// TargetNetID is the AMS Net ID of the target TwinCAT system (e.g. "192.168.1.1.1.1").
	TargetNetID string `mapstructure:"target_net_id"`
	// RouterAddr is the IP address or hostname of the ADS router. Leave empty to
	// use the local router (Windows with TwinCAT installed).
	RouterAddr string `mapstructure:"router_addr"`
	// RouterPort is the ADS router TCP port (default 48898).
	RouterPort uint16 `mapstructure:"router_port"`
	// PLCPort is the ADS port of the PLC runtime (default 851 for TC3 runtime 1).
	PLCPort uint16 `mapstructure:"plc_port"`
	// StatePollingInterval controls how often the ADS client polls TwinCAT state
	// for restart detection (default 2s).
	StatePollingInterval time.Duration `mapstructure:"state_polling_interval"`
}

// DefaultConfig returns a Config populated with sensible defaults.
func DefaultConfig() Config {
	return Config{
		RouterPort:           48898,
		PLCPort:              851,
		StatePollingInterval: 2 * time.Second,
	}
}

// ReconnectFunc is called (with the live ads.Client) whenever TwinCAT returns
// to Run mode after a restart or connection loss. Implementations should
// re-resolve symbol handles and re-register subscriptions.
type ReconnectFunc func(client *ads.Client) error

// ManagedClient wraps an ads.Client with automatic reconnect support and
// bridges ads-go's log/slog interface to OTel's *zap.Logger.
type ManagedClient struct {
	cfg         Config
	zapLogger   *zap.Logger
	inner       *ads.Client
	reconnectFn ReconnectFunc
	mu          sync.RWMutex
}

// NewManagedClient creates a ManagedClient. The optional reconnectFn is invoked
// each time the TwinCAT runtime returns to Run state.
func NewManagedClient(cfg Config, logger *zap.Logger, reconnectFn ReconnectFunc) *ManagedClient {
	mc := &ManagedClient{
		cfg:         cfg,
		zapLogger:   logger,
		reconnectFn: reconnectFn,
	}

	settings := ads.ClientSettings{
		TargetNetID:          cfg.TargetNetID,
		RouterHost:           cfg.RouterAddr,
		RouterPort:           int(cfg.RouterPort),
		StatePollingInterval: cfg.StatePollingInterval,
		OnConnectionLost:     mc.onConnectionLost,
	}

	mc.inner = ads.NewClient(settings, mc.newSlogAdapter())
	return mc
}

// Connect establishes the ADS connection.
func (mc *ManagedClient) Connect() error {
	return mc.inner.Connect()
}

// Reset recreates the underlying ads.Client so a clean retry can be attempted
// after a failed or partially-failed Connect. Must only be called when the
// previous client is no longer connected.
func (mc *ManagedClient) Reset() {
	settings := ads.ClientSettings{
		TargetNetID:          mc.cfg.TargetNetID,
		RouterHost:           mc.cfg.RouterAddr,
		RouterPort:           int(mc.cfg.RouterPort),
		StatePollingInterval: mc.cfg.StatePollingInterval,
		OnConnectionLost:     mc.onConnectionLost,
	}
	mc.inner = ads.NewClient(settings, mc.newSlogAdapter())
}

// Disconnect tears down the ADS connection and clears all subscriptions.
// Safe to call when the client was never fully connected.
func (mc *ManagedClient) Disconnect() {
	if mc.inner != nil {
		mc.inner.Disconnect()
	}
}

// Client returns the underlying *ads.Client for direct use.
func (mc *ManagedClient) Client() *ads.Client {
	return mc.inner
}

// onConnectionLost is registered as the ads.ClientSettings.OnConnectionLost
// callback. It blocks (in a goroutine spawned by ads-go) until TwinCAT returns
// to Run state, then calls the user-registered reconnectFn.
func (mc *ManagedClient) onConnectionLost(client *ads.Client, err error) {
	mc.zapLogger.Warn("ADS connection lost – waiting for TwinCAT Run state",
		zap.Error(err),
		zap.String("target_net_id", mc.cfg.TargetNetID),
	)

	tick := time.NewTicker(mc.cfg.StatePollingInterval)
	defer tick.Stop()

	for range tick.C {
		state := client.GetCurrentState()
		if state == nil {
			continue
		}
		// AdsState 5 == Run.
		if state.AdsState == 5 {
			mc.zapLogger.Info("TwinCAT returned to Run state – reconnecting",
				zap.String("target_net_id", mc.cfg.TargetNetID),
			)
			if mc.reconnectFn != nil {
				if rErr := mc.reconnectFn(client); rErr != nil {
					mc.zapLogger.Error("reconnect callback failed", zap.Error(rErr))
				}
			}
			return
		}
	}
}

// ---------------------------------------------------------------------------
// log/slog → *zap.Logger bridge
// ---------------------------------------------------------------------------

// zapSlogHandler adapts a *zap.Logger to the slog.Handler interface so that
// ads-go's internal slog messages are forwarded to OTel's zap logger.
type zapSlogHandler struct {
	logger *zap.Logger
	attrs  []slog.Attr
	group  string
}

func (mc *ManagedClient) newSlogAdapter() *slog.Logger {
	return slog.New(&zapSlogHandler{logger: mc.zapLogger})
}

func (h *zapSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	var zapLevel zapcore.Level
	switch {
	case level >= slog.LevelError:
		zapLevel = zap.ErrorLevel
	case level >= slog.LevelWarn:
		zapLevel = zap.WarnLevel
	case level >= slog.LevelInfo:
		zapLevel = zap.InfoLevel
	default:
		zapLevel = zap.DebugLevel
	}
	return h.logger.Core().Enabled(zapLevel)
}

func (h *zapSlogHandler) Handle(_ context.Context, r slog.Record) error {
	var zapLevel zapcore.Level
	switch {
	case r.Level >= slog.LevelError:
		zapLevel = zap.ErrorLevel
	case r.Level >= slog.LevelWarn:
		zapLevel = zap.WarnLevel
	case r.Level >= slog.LevelInfo:
		zapLevel = zap.InfoLevel
	default:
		zapLevel = zap.DebugLevel
	}

	fields := make([]zap.Field, 0, r.NumAttrs()+len(h.attrs))
	for _, a := range h.attrs {
		fields = append(fields, zap.String(a.Key, a.Value.String()))
	}
	r.Attrs(func(a slog.Attr) bool {
		fields = append(fields, zap.String(a.Key, a.Value.String()))
		return true
	})

	h.logger.Log(zapLevel, "[ads-go] "+r.Message, fields...)
	return nil
}

func (h *zapSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cloned := *h
	cloned.attrs = append(cloned.attrs, attrs...)
	return &cloned
}

func (h *zapSlogHandler) WithGroup(name string) slog.Handler {
	cloned := *h
	cloned.group = name
	return &cloned
}
