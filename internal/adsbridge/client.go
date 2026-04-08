// Package adsbridge provides a shared ADS client wrapper used by both
// the adslogsreceiver and adsmetricsreceiver packages.
package adsbridge

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
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
	// lostDone is closed by Reset() so orphaned onConnectionLost goroutines
	// started for a now-replaced client exit promptly instead of leaking.
	lostDone chan struct{}
}

// NewManagedClient creates a ManagedClient. The optional reconnectFn is invoked
// each time the TwinCAT runtime returns to Run state.
func NewManagedClient(cfg Config, logger *zap.Logger, reconnectFn ReconnectFunc) *ManagedClient {
	mc := &ManagedClient{
		cfg:         cfg,
		zapLogger:   logger,
		reconnectFn: reconnectFn,
		lostDone:    make(chan struct{}),
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
	// Signal any orphaned onConnectionLost goroutines from the old client to
	// exit instead of polling a dead connection forever.
	close(mc.lostDone)
	mc.lostDone = make(chan struct{})

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

// IsNetworkError reports whether err is a TCP/network-level failure as opposed
// to an ADS protocol error (e.g. "Symbol not found"). When true the caller
// should tear down and re-establish the ADS connection. When false the TCP
// connection is still alive and only the PLC-side resource is missing.
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "EOF") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "forcibly closed") ||
		strings.Contains(s, "use of closed network connection")
}

// onConnectionLost is registered as the ads.ClientSettings.OnConnectionLost
// callback. It runs in a goroutine spawned by ads-go, polling until TwinCAT
// returns to Run state and then calling reconnectFn in a retry loop until it
// succeeds (handles the case where symbols aren't deployed yet on restart).
func (mc *ManagedClient) onConnectionLost(client *ads.Client, err error) {
	mc.zapLogger.Warn("ADS connection lost – waiting for TwinCAT Run state",
		zap.Error(err),
		zap.String("target_net_id", mc.cfg.TargetNetID),
	)

	// Capture done so that if Reset() replaces this client, we exit cleanly
	// instead of leaking this goroutine.
	done := mc.lostDone

	tick := time.NewTicker(mc.cfg.StatePollingInterval)
	defer tick.Stop()

	for {
		select {
		case <-done:
			mc.zapLogger.Debug("onConnectionLost: client replaced, exiting",
				zap.String("target_net_id", mc.cfg.TargetNetID),
			)
			return
		case <-tick.C:
		}

		state := client.GetCurrentState()
		if state == nil || state.AdsState != 5 {
			continue
		}

		// TwinCAT is in Run state. Attempt reconnect.
		if mc.reconnectFn == nil {
			return
		}
		if rErr := mc.reconnectFn(client); rErr != nil {
			// Symbols may not be deployed yet – keep polling until they appear.
			mc.zapLogger.Info("Reconnect attempt failed, will retry on next poll",
				zap.Error(rErr),
				zap.String("target_net_id", mc.cfg.TargetNetID),
			)
			continue
		}
		mc.zapLogger.Info("Reconnected successfully after TwinCAT restart",
			zap.String("target_net_id", mc.cfg.TargetNetID),
		)
		return
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
