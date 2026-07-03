package adsreceiver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jarmocluyse/ads-go/pkg/ads"
	adsstateinfo "github.com/jarmocluyse/ads-go/pkg/ads/ads-stateinfo"
	"github.com/nepionic/otelcol-ads/internal/adsbridge"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// adsCore owns the single shared ADS connection for one receiver instance
// (component.ID). It is wrapped by sharedcomponent.Component so that no
// matter how many signals (logs, metrics) reference this receiver ID, exactly
// one adsbridge.ManagedClient / connect-retry loop is created, and each
// registered signal's subscribe/drain/reconnect logic is fanned out to from
// here.
type adsCore struct {
	id     component.ID
	cfg    *Config
	logger *zap.Logger

	client *adsbridge.ManagedClient

	mu      sync.Mutex // guards logs/metrics registration
	logs    *logsSignal
	metrics *metricsSignal

	// connected is set once the underlying TCP connection is established, and
	// gates whether Shutdown attempts to Disconnect. It intentionally does not
	// wait for full symbol subscription (unlike the per-signal "connected"
	// flags the old, separate receivers used) - with one shared connection, a
	// signal that can never resolve its symbols must not block the other
	// signal's clean disconnect on shutdown.
	connected atomic.Bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newCore(id component.ID, cfg *Config, logger *zap.Logger) *adsCore {
	return &adsCore{
		id:     id,
		cfg:    cfg,
		logger: logger,
		stopCh: make(chan struct{}),
	}
}

// registerLogs wires a logs pipeline's consumer into this core. Called
// synchronously from createLogsReceiver, before adsCore.Start ever runs.
func (c *adsCore) registerLogs(s *logsSignal) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.Logs == nil {
		return fmt.Errorf("ads receiver %q: used in a logs pipeline but no logs: block is configured", c.id)
	}
	c.logs = s
	return nil
}

// registerMetrics wires a metrics pipeline's consumer into this core. Called
// synchronously from createMetricsReceiver, before adsCore.Start ever runs.
func (c *adsCore) registerMetrics(s *metricsSignal) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.Metrics == nil {
		return fmt.Errorf("ads receiver %q: used in a metrics pipeline but no metrics: block is configured", c.id)
	}
	c.metrics = s
	return nil
}

// Start implements component.Component.
func (c *adsCore) Start(_ context.Context, _ component.Host) error {
	hooks := adsbridge.ManagedClientHooks{
		OnStateChange:    c.onStateChange,
		OnConnectionLost: c.onConnectionLost,
	}
	c.client = adsbridge.NewManagedClient(c.cfg.toBridgeConfig(), c.logger, c.reconnect, hooks)

	c.wg.Add(1)
	go c.connectLoop()

	c.logger.Info("ADS receiver starting",
		zap.String("target_net_id", c.cfg.TargetNetID),
		zap.Duration("connect_retry_max", c.cfg.ConnectRetryMaxInterval),
		zap.Duration("symbol_poll_interval", c.cfg.StatePollingInterval),
		zap.Bool("logs_enabled", c.logs != nil),
		zap.Bool("metrics_enabled", c.metrics != nil),
	)
	return nil
}

// Shutdown implements component.Component.
func (c *adsCore) Shutdown(_ context.Context) error {
	close(c.stopCh)
	c.wg.Wait()

	if c.logs != nil {
		c.logs.teardown()
	}
	if c.metrics != nil {
		c.metrics.teardown()
	}

	if c.connected.Load() {
		c.client.Disconnect()
	}

	c.logger.Info("ADS receiver stopped")
	return nil
}

// connectLoop connects to the ADS router with exponential backoff. Once the
// router is reached it calls subscribeLoop, which polls for PLC symbols
// without disconnecting. Only a network-level failure restarts the backoff
// and resets the client.
func (c *adsCore) connectLoop() {
	defer c.wg.Done()

	backoff := c.cfg.ConnectRetryInitialInterval

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if err := c.client.Connect(); err != nil {
			c.logger.Warn("ADS connect failed",
				zap.String("target_net_id", c.cfg.TargetNetID),
				zap.Duration("retry_in", backoff),
				zap.Error(err),
			)
			select {
			case <-c.stopCh:
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > c.cfg.ConnectRetryMaxInterval {
				backoff = c.cfg.ConnectRetryMaxInterval
			}
			c.client.Reset()
			continue
		}

		c.connected.Store(true)
		backoff = c.cfg.ConnectRetryInitialInterval
		c.logger.Info("Connected to ADS router", zap.String("target_net_id", c.cfg.TargetNetID))

		if c.logs != nil {
			c.logs.emitConnectionEstablished(c.cfg.RouterAddr)
		}

		if !c.subscribeLoop() {
			// TCP dropped while waiting for symbols; need a full reconnect.
			c.client.Reset()
			continue
		}
		return
	}
}

// subscribeLoop polls for PLC symbols on behalf of every registered signal
// while staying connected to the router. Returns true when every registered
// signal is subscribed (or stopCh fires), false when the TCP connection is
// lost and connectLoop must re-establish it.
func (c *adsCore) subscribeLoop() bool {
	logsDone := c.logs == nil
	metricsDone := c.metrics == nil

	first := true
	for {
		if !first {
			select {
			case <-c.stopCh:
				return true
			case <-time.After(c.cfg.StatePollingInterval):
			}
		}
		first = false

		select {
		case <-c.stopCh:
			return true
		default:
		}

		if !logsDone {
			netErr, done := c.logs.trySubscribe()
			if netErr != nil {
				c.logger.Warn("TCP connection dropped during logs setup", zap.String("target_net_id", c.cfg.TargetNetID), zap.Error(netErr))
				return false
			}
			logsDone = done
		}

		if !metricsDone {
			netErr, done := c.metrics.trySubscribe()
			if netErr != nil {
				c.logger.Warn("TCP connection dropped during metrics setup", zap.String("target_net_id", c.cfg.TargetNetID), zap.Error(netErr))
				return false
			}
			metricsDone = done
		}

		if logsDone && metricsDone {
			c.logger.Info("ADS receiver subscribed", zap.String("target_net_id", c.cfg.TargetNetID))
			return true
		}
	}
}

// reconnect is called by ManagedClient after TwinCAT returns to Run state. It
// fans out to whichever signals are registered, wrapping errors with which
// signal failed so retries are identifiable in logs. A signal that already
// succeeded is harmlessly re-run if a later signal fails and the whole
// reconnect is retried - each signal's reconnect unsubscribes-then-resubscribes
// idempotently.
func (c *adsCore) reconnect(client *ads.Client) error {
	if c.logs != nil {
		if err := c.logs.reconnect(client); err != nil {
			return fmt.Errorf("logs: %w", err)
		}
	}
	if c.metrics != nil {
		if err := c.metrics.reconnect(client); err != nil {
			return fmt.Errorf("metrics: %w", err)
		}
	}
	return nil
}

// onStateChange is registered as the ManagedClientHooks.OnStateChange
// callback. Only the logs signal emits system logs for state changes.
func (c *adsCore) onStateChange(newState, oldState *adsstateinfo.SystemState) {
	if c.logs != nil {
		c.logs.onStateChange(newState, oldState)
	}
}

// onConnectionLost is registered as the ManagedClientHooks.OnConnectionLost
// callback. Only the logs signal emits a system log for connection loss.
func (c *adsCore) onConnectionLost(err error) {
	if c.logs != nil {
		c.logs.onConnectionLost(err)
	}
}
