// Package adsbridge provides a shared ADS client wrapper used by the
// adsreceiver package's logs and metrics signals.
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
	adsstateinfo "github.com/jarmocluyse/ads-go/pkg/ads/ads-stateinfo"
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

// ManagedClientHooks holds optional lifecycle notification callbacks wired into
// the underlying ads.Client. All fields are optional; nil functions are ignored.
type ManagedClientHooks struct {
	// OnStateChange is invoked (from a background goroutine) whenever the
	// TwinCAT ADS state transitions. oldState is nil on the first successful
	// state read after connect.
	OnStateChange func(newState, oldState *adsstateinfo.SystemState)

	// OnConnectionLost is invoked when the ADS connection drops (TCP failure or
	// consecutive state-poll failure). It fires before the internal
	// reconnect-polling loop begins, giving callers a chance to emit an event.
	OnConnectionLost func(err error)
}

// ManagedClient wraps an ads.Client with automatic reconnect support and
// bridges ads-go's log/slog interface to OTel's *zap.Logger.
type ManagedClient struct {
	cfg         Config
	zapLogger   *zap.Logger
	inner       *ads.Client
	reconnectFn ReconnectFunc
	hooks       ManagedClientHooks
	mu          sync.RWMutex
	// lostDone is closed by Reset() so orphaned onConnectionLost goroutines
	// started for a now-replaced client exit promptly instead of leaking.
	lostDone chan struct{}
}

// NewManagedClient creates a ManagedClient. The optional reconnectFn is invoked
// each time the TwinCAT runtime returns to Run state. The hooks parameter allows
// callers to receive state-change and connection-lost notifications.
func NewManagedClient(cfg Config, logger *zap.Logger, reconnectFn ReconnectFunc, hooks ManagedClientHooks) *ManagedClient {
	mc := &ManagedClient{
		cfg:         cfg,
		zapLogger:   logger,
		reconnectFn: reconnectFn,
		hooks:       hooks,
		lostDone:    make(chan struct{}),
	}
	mc.inner = ads.NewClient(mc.buildSettings(), mc.newSlogAdapter())
	return mc
}

// buildSettings constructs the ads.ClientSettings, wiring in lifecycle hooks.
func (mc *ManagedClient) buildSettings() ads.ClientSettings {
	settings := ads.ClientSettings{
		TargetNetID:          mc.cfg.TargetNetID,
		RouterHost:           mc.cfg.RouterAddr,
		RouterPort:           int(mc.cfg.RouterPort),
		StatePollingInterval: mc.cfg.StatePollingInterval,
		OnConnectionLost:     mc.onConnectionLost,
	}
	if mc.hooks.OnStateChange != nil {
		fn := mc.hooks.OnStateChange
		settings.OnStateChange = func(_ *ads.Client, newState, oldState *adsstateinfo.SystemState) {
			fn(newState, oldState)
		}
	}
	return settings
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
	mc.inner = ads.NewClient(mc.buildSettings(), mc.newSlogAdapter())
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

	// Notify the caller (adsCore, fanning out to the logs signal) so it can emit an OTel system log.
	if mc.hooks.OnConnectionLost != nil {
		mc.hooks.OnConnectionLost(err)
	}

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

		// Actively read state from the router instead of using the cached value.
		// The cache was cleared to nil when this callback fired (restart-index or
		// consecutive-failure path), and the state poller that would refresh it has
		// been stopped. Using GetCurrentState() would loop forever on nil.
		state, sErr := client.ReadTcSystemState()
		if sErr != nil || state.AdsState != 5 {
			continue
		}

		// TwinCAT is in Run state. Attempt reconnect.
		if mc.reconnectFn == nil {
			client.RestartStatePoller()
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
		// Re-arm the state poller so the next PLC activation is also detected.
		client.RestartStatePoller()
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
