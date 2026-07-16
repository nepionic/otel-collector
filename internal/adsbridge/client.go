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
	// ConnectRetryInitialInterval is the first backoff interval used both for
	// the initial connect and when dialing a fresh connection to replace a
	// wedged one (default 1s).
	ConnectRetryInitialInterval time.Duration `mapstructure:"connect_retry_initial_interval"`
	// ConnectRetryMaxInterval caps that backoff, and also bounds how long
	// onConnectionLost tolerates consecutive ReadTcSystemState failures before
	// concluding the TCP session itself is wedged and forcing a fresh one
	// (default 30s).
	ConnectRetryMaxInterval time.Duration `mapstructure:"connect_retry_max_interval"`
}

// DefaultConfig returns a Config populated with sensible defaults.
func DefaultConfig() Config {
	return Config{
		RouterPort:                  48898,
		PLCPort:                     851,
		StatePollingInterval:        2 * time.Second,
		ConnectRetryInitialInterval: time.Second,
		ConnectRetryMaxInterval:     30 * time.Second,
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
//
// A TwinCAT restart-index bump doesn't necessarily drop the underlying TCP
// session to the router - the router itself can keep running across a PLC
// config activation/restart. If that session survives but stops actually
// responding (observed in practice: ReadTcSystemState timing out forever,
// never a clean EOF/reset IsNetworkError would catch), polling it would
// otherwise continue indefinitely with no path back to Run state. Once
// consecutive read failures exceed ConnectRetryMaxInterval, this gives up on
// the existing session and dials a fresh one via reconnectTCP.
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

	var firstReadFailureAt time.Time

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
		if sErr != nil {
			if firstReadFailureAt.IsZero() {
				firstReadFailureAt = time.Now()
			}
			if time.Since(firstReadFailureAt) < mc.cfg.ConnectRetryMaxInterval {
				continue
			}

			mc.zapLogger.Warn("ReadTcSystemState has failed continuously past the retry ceiling; the TCP session is likely wedged, dialing a fresh one",
				zap.Duration("stuck_for", time.Since(firstReadFailureAt)),
				zap.String("target_net_id", mc.cfg.TargetNetID),
				zap.Error(sErr),
			)
			client = mc.reconnectTCP(done)
			if client == nil {
				// done fired (client replaced/torn down elsewhere) while we were
				// dialing - another goroutine now owns reconnection, or the
				// receiver is shutting down.
				return
			}
			done = mc.lostDone
			firstReadFailureAt = time.Time{}
			continue
		}
		firstReadFailureAt = time.Time{}

		if state.AdsState != 5 {
			continue
		}

		// TwinCAT is in Run state. Attempt reconnect.
		if mc.reconnectFn == nil {
			client.RestartStatePoller()
			return
		}
		if rErr := mc.reconnectFn(client); rErr != nil {
			// Symbols may not be deployed yet – keep polling until they appear.
			mc.zapLogger.Warn("Reconnect attempt failed, will retry on next poll",
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

// reconnectTCP tears down a wedged ads.Client (Reset) and dials a fresh one,
// retrying with the same exponential backoff used for the initial connect.
// Returns the new, connected client, or nil if prevDone fires while dialing
// (the client was replaced or the receiver is shutting down elsewhere, so the
// caller should stop rather than keep managing a client it no longer owns).
func (mc *ManagedClient) reconnectTCP(prevDone chan struct{}) *ads.Client {
	select {
	case <-prevDone:
		return nil
	default:
	}
	mc.Reset()
	done := mc.lostDone

	backoff := mc.cfg.ConnectRetryInitialInterval
	if backoff <= 0 {
		backoff = time.Second
	}
	maxBackoff := mc.cfg.ConnectRetryMaxInterval
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	for {
		client := mc.Client()
		if cErr := client.Connect(); cErr != nil {
			mc.zapLogger.Warn("Fresh ADS connect failed, retrying",
				zap.Duration("retry_in", backoff),
				zap.String("target_net_id", mc.cfg.TargetNetID),
				zap.Error(cErr),
			)
			select {
			case <-done:
				return nil
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			mc.Reset()
			done = mc.lostDone
			continue
		}
		mc.zapLogger.Info("Dialed fresh ADS connection after wedged session",
			zap.String("target_net_id", mc.cfg.TargetNetID),
		)
		return client
	}
}

// ---------------------------------------------------------------------------
// log/slog → *zap.Logger bridge
// ---------------------------------------------------------------------------

// zapSlogHandler adapts a *zap.Logger to the slog.Handler interface so that
// dependencies using log/slog (ads-go, ads-logger) forward their internal
// diagnostics to OTel's zap logger.
type zapSlogHandler struct {
	logger *zap.Logger
	prefix string
	attrs  []slog.Attr
	group  string
}

// NewZapSlogAdapter returns a *slog.Logger backed by logger, so any dependency
// that accepts a *slog.Logger for its own internal diagnostics can be wired
// into the receiver's zap logger instead of writing to a separate target.
// prefix is prepended to every message (e.g. "[ads-go] ") to identify the source.
func NewZapSlogAdapter(logger *zap.Logger, prefix string) *slog.Logger {
	return slog.New(&zapSlogHandler{logger: logger, prefix: prefix})
}

func (mc *ManagedClient) newSlogAdapter() *slog.Logger {
	return NewZapSlogAdapter(mc.zapLogger, "[ads-go] ")
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

	h.logger.Log(zapLevel, h.prefix+r.Message, fields...)
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
