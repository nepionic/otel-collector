package adslogsreceiver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jarmocluyse/ads-go/pkg/ads"
	adsstateinfo "github.com/jarmocluyse/ads-go/pkg/ads/ads-stateinfo"
	adstypes "github.com/jarmocluyse/ads-go/pkg/ads/types"
	"github.com/nepionic/otelcol-ads/internal/adsbridge"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// logsReceiver subscribes to the TwinCAT OtelBridge log ring buffer and
// converts each log record to an OpenTelemetry plog.Logs batch.
type logsReceiver struct {
	cfg          *Config
	settings     receiver.Settings
	nextConsumer consumer.Logs
	logger       *zap.Logger

	client       *adsbridge.ManagedClient
	localRI      uint32 // consumer read index – accessed atomically
	ringGroup    uint32 // ADS IndexGroup of the log ring symbol
	ringOffset   uint32 // ADS IndexOffset of the log ring symbol
	ringSize     uint32 // total byte size of the log ring symbol
	subscription *ads.ActiveSubscription
	wmu          sync.Mutex // guards subscription and ring address fields

	connected atomic.Bool // true once connect+subscribe succeeded

	wiCh   chan uint32 // write_index notifications feed this channel
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newLogsReceiver(set receiver.Settings, cfg *Config, next consumer.Logs) *logsReceiver {
	return &logsReceiver{
		cfg:          cfg,
		settings:     set,
		nextConsumer: next,
		logger:       set.TelemetrySettings.Logger,
		wiCh:         make(chan uint32, 64),
		stopCh:       make(chan struct{}),
	}
}

// Start implements component.Component.
func (r *logsReceiver) Start(_ context.Context, _ component.Host) error {
	hooks := adsbridge.ManagedClientHooks{
		OnStateChange: r.onStateChange,
		OnConnectionLost: func(err error) {
			if r.cfg.SystemLogs.Enabled && r.cfg.SystemLogs.ConnectionEvents {
				r.emitSystemLog(
					plog.SeverityNumberWarn, "WARN",
					"ads.connection.lost",
					fmt.Sprintf("ADS connection lost: %v", err),
					nil,
				)
			}
		},
	}
	r.client = adsbridge.NewManagedClient(r.cfg.toBridgeConfig(), r.logger, r.reconnect, hooks)

	r.wg.Add(1)
	go r.connectLoop()

	r.logger.Info("ADS logs receiver starting",
		zap.String("target_net_id", r.cfg.TargetNetID),
		zap.String("ring_symbol", r.cfg.LogRingSymbol),
		zap.Duration("connect_retry_max", r.cfg.ConnectRetryMaxInterval),
		zap.Duration("symbol_poll_interval", r.cfg.StatePollingInterval),
	)
	return nil
}

// connectLoop connects to the ADS router with exponential backoff. Once the
// router is reached it calls subscribeLoop, which polls for PLC symbols without
// disconnecting. Only a network-level failure restarts the backoff and resets
// the client.
func (r *logsReceiver) connectLoop() {
	defer r.wg.Done()

	backoff := r.cfg.ConnectRetryInitialInterval

	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		if err := r.client.Connect(); err != nil {
			r.logger.Warn("ADS connect failed",
				zap.Duration("next_retry", backoff),
				zap.Error(err),
			)
			select {
			case <-r.stopCh:
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > r.cfg.ConnectRetryMaxInterval {
				backoff = r.cfg.ConnectRetryMaxInterval
			}
			r.client.Reset()
			continue
		}

		// Router reached – reset backoff for any future reconnect cycle.
		backoff = r.cfg.ConnectRetryInitialInterval
		r.logger.Info("Connected to ADS router",
			zap.String("target_net_id", r.cfg.TargetNetID),
		)

		if r.cfg.SystemLogs.Enabled && r.cfg.SystemLogs.ConnectionEvents {
			r.emitSystemLog(
				plog.SeverityNumberInfo, "INFO",
				"ads.connection.established",
				"Connected to ADS router",
				func(m pcommon.Map) {
					if r.cfg.RouterAddr != "" {
						m.PutStr("ads.router_addr", r.cfg.RouterAddr)
					}
				},
			)
		}

		if !r.subscribeLoop() {
			// TCP dropped while waiting for symbols; need a full reconnect.
			r.client.Reset()
			continue
		}
		return
	}
}

// subscribeLoop polls for the log ring symbol while staying connected to the
// router. ADS errors (e.g. "Symbol not found") cause a wait-and-retry without
// disconnecting. Returns true when subscribed (or stopCh fires), false when
// the TCP connection is lost and connectLoop must re-establish it.
func (r *logsReceiver) subscribeLoop() bool {
	first := true
	for {
		if !first {
			select {
			case <-r.stopCh:
				return true
			case <-time.After(r.cfg.StatePollingInterval):
			}
		}
		first = false

		select {
		case <-r.stopCh:
			return true
		default:
		}

		// If no ring symbol is configured, discover it by scanning the PLC
		// symbol table for a variable with {attribute 'otelcol_role' := 'log_ring'}.
		if r.cfg.LogRingSymbol == "" {
			sym, err := adsbridge.FindLogRingSymbol(r.client.Client(), r.cfg.PLCPort)
			if err != nil {
				if adsbridge.IsNetworkError(err) {
					r.logger.Warn("TCP connection dropped during symbol discovery", zap.Error(err))
					return false
				}
				r.logger.Info("Log ring symbol not yet discovered; waiting",
					zap.Error(err),
					zap.Duration("retry_in", r.cfg.StatePollingInterval),
				)
				continue
			}
			r.logger.Info("Discovered log ring symbol", zap.String("symbol", sym))
			r.cfg.LogRingSymbol = sym
		}

		if err := r.resolveRingSymbol(); err != nil {
			if adsbridge.IsNetworkError(err) {
				r.logger.Warn("TCP connection dropped during symbol lookup",
					zap.Error(err),
				)
				return false
			}
			r.logger.Info("Log ring symbol not yet available; waiting",
				zap.String("symbol", r.cfg.LogRingSymbol),
				zap.Duration("retry_in", r.cfg.StatePollingInterval),
			)
			continue
		}

		if err := r.subscribe(); err != nil {
			if adsbridge.IsNetworkError(err) {
				r.logger.Warn("TCP connection dropped during subscribe",
					zap.Error(err),
				)
				return false
			}
			r.logger.Warn("Subscribe failed; retrying", zap.Error(err))
			continue
		}

		r.connected.Store(true)
		r.logger.Info("ADS logs receiver subscribed",
			zap.String("target_net_id", r.cfg.TargetNetID),
			zap.String("ring_symbol", r.cfg.LogRingSymbol),
		)
		r.wg.Add(1)
		go r.drainLoop()
		return true
	}
}

// Shutdown implements component.Component.
func (r *logsReceiver) Shutdown(_ context.Context) error {
	close(r.stopCh)
	r.wg.Wait()

	if r.connected.Load() {
		r.wmu.Lock()
		if r.subscription != nil {
			_ = r.client.Client().Unsubscribe(r.subscription)
		}
		r.wmu.Unlock()
		r.client.Disconnect()
	}

	r.logger.Info("ADS logs receiver stopped")
	return nil
}

// resolveRingSymbol fetches the IndexGroup, IndexOffset and byte size of the
// log ring symbol so we can ReadRaw it efficiently.
func (r *logsReceiver) resolveRingSymbol() error {
	sym, err := r.client.Client().GetSymbol(r.cfg.PLCPort, r.cfg.LogRingSymbol)
	if err != nil {
		return fmt.Errorf("GetSymbol(%q): %w", r.cfg.LogRingSymbol, err)
	}
	r.wmu.Lock()
	r.ringGroup = sym.IndexGroup
	r.ringOffset = sym.IndexOffset
	r.ringSize = sym.Size
	r.wmu.Unlock()
	return nil
}

// subscribe registers an ADS on-change notification on the head field.
// head is the first UDINT in OTelLogRing, so its symbol path is
// "<LogRingSymbol>.head".
func (r *logsReceiver) subscribe() error {
	wiPath := r.cfg.LogRingSymbol + ".head"

	cb := func(data ads.SubscriptionData) {
		wi, ok := data.Value.(uint32)
		if !ok {
			return
		}
		select {
		case r.wiCh <- wi:
		default:
			// Channel full – drainLoop is lagging. Drop notification; the next
			// one will carry the updated index.
		}
	}

	sub, err := r.client.Client().SubscribeValue(
		r.cfg.PLCPort,
		wiPath,
		cb,
		ads.SubscriptionSettings{
			CycleTime:    r.cfg.SubscriptionCycleTime,
			SendOnChange: true,
		},
	)
	if err != nil {
		return fmt.Errorf("SubscribeValue(%q): %w", wiPath, err)
	}

	r.wmu.Lock()
	r.subscription = sub
	r.wmu.Unlock()
	return nil
}

// reconnect is called by the ManagedClient after TwinCAT returns to Run state.
func (r *logsReceiver) reconnect(client *ads.Client) error {
	r.logger.Info("Re-resolving ADS log ring symbol after TwinCAT restart")

	// Re-resolve symbol address (may change after PLC download).
	if err := r.resolveRingSymbol(); err != nil {
		return err
	}

	// Unsubscribe old, subscribe fresh.
	r.wmu.Lock()
	if r.subscription != nil {
		_ = client.Unsubscribe(r.subscription)
		r.subscription = nil
	}
	r.wmu.Unlock()

	// Reset read cursor so we don't try to replay stale slots.
	atomic.StoreUint32(&r.localRI, 0)

	if err := r.subscribe(); err != nil {
		return err
	}

	if r.cfg.SystemLogs.Enabled && r.cfg.SystemLogs.ConnectionEvents {
		r.emitSystemLog(
			plog.SeverityNumberInfo, "INFO",
			"ads.plc.reconnected",
			"Reconnected to TwinCAT after restart",
			nil,
		)
	}
	return nil
}

// drainLoop blocks on write_index notifications and drains new log slots.
func (r *logsReceiver) drainLoop() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		case newWI := <-r.wiCh:
			r.drain(newWI)
		}
	}
}

// drain reads all new slots from [localRI, newWI) and emits them as OTel logs.
func (r *logsReceiver) drain(newWI uint32) {
	lri := atomic.LoadUint32(&r.localRI)
	if newWI == lri {
		return
	}

	// Detect ring overflow before reading: if the PLC has written more than a
	// full ring's worth of entries since our last drain, some will be lost.
	if r.cfg.SystemLogs.Enabled && r.cfg.SystemLogs.RingOverflows {
		if pending := newWI - lri; pending > adsbridge.LogCapacity {
			lost := pending - adsbridge.LogCapacity/2
			r.emitSystemLog(
				plog.SeverityNumberWarn, "WARN",
				"ads.ring.overflow",
				fmt.Sprintf("Log ring buffer overflowed: ~%d entries lost", lost),
				func(m pcommon.Map) {
					m.PutInt("ads.ring.lost_count", int64(lost))
					m.PutInt("ads.ring.capacity", int64(adsbridge.LogCapacity))
				},
			)
		}
	}

	r.wmu.Lock()
	group := r.ringGroup
	offset := r.ringOffset
	size := r.ringSize
	r.wmu.Unlock()

	if size == 0 {
		return
	}

	rawRing, err := r.client.Client().ReadRaw(r.cfg.PLCPort, group, offset, size)
	if err != nil {
		r.logger.Warn("ReadRaw log ring failed", zap.Error(err))
		return
	}

	slots, newRI := adsbridge.DrainLogs(rawRing, lri, newWI)
	atomic.StoreUint32(&r.localRI, newRI)

	if len(slots) == 0 {
		return
	}

	logs := r.slotsToLogs(slots)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.nextConsumer.ConsumeLogs(ctx, logs); err != nil {
		r.logger.Error("ConsumeLogs failed", zap.Error(err))
	}
}

// slotsToLogs converts parsed log slots to a plog.Logs batch.
func (r *logsReceiver) slotsToLogs(slots []adsbridge.LogSlot) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	ra := rl.Resource().Attributes()
	ra.PutStr("service.name", r.settings.ID.Name())
	ra.PutStr("ads.net_id", r.cfg.TargetNetID)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("otelcol-ads/adslogsreceiver")

	for _, s := range slots {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(adsbridge.TimestampNsToTime(s.TimestampNs)))
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.SetSeverityNumber(adsSeverityToOtel(s.Severity))
		lr.SetSeverityText(adsSeverityText(s.Severity))
		lr.Body().SetStr(s.Message)

		la := lr.Attributes()
		if s.Source != "" {
			la.PutStr("ads.source", s.Source)
		}
		for _, a := range s.Attrs {
			if a.Key == "" {
				continue
			}
			switch a.AttrType {
			case adsbridge.LogAttrTypeStr:
				la.PutStr(a.Key, a.StrValue)
			case adsbridge.LogAttrTypeBoolean:
				la.PutBool(a.Key, a.BoolValue)
			case adsbridge.LogAttrTypeInt8, adsbridge.LogAttrTypeInt16, adsbridge.LogAttrTypeInt32, adsbridge.LogAttrTypeInt64,
				adsbridge.LogAttrTypeUInt8, adsbridge.LogAttrTypeUInt16, adsbridge.LogAttrTypeUInt32, adsbridge.LogAttrTypeUInt64:
				la.PutInt(a.Key, a.IntValue)
			case adsbridge.LogAttrTypeFloat32, adsbridge.LogAttrTypeFloat64:
				la.PutDouble(a.Key, a.DoubleValue)
			default:
				la.PutStr(a.Key, a.StrValue)
			}
		}
	}

	return logs
}

// adsSeverityToOtel maps TwinCAT EventLogger severity (0–4) to OTel SeverityNumber.
func adsSeverityToOtel(severity uint32) plog.SeverityNumber {
	switch severity {
	case 0:
		return plog.SeverityNumberDebug
	case 1:
		return plog.SeverityNumberInfo
	case 2:
		return plog.SeverityNumberWarn
	case 3:
		return plog.SeverityNumberError
	case 4:
		return plog.SeverityNumberFatal
	default:
		return plog.SeverityNumberUnspecified
	}
}

func adsSeverityText(severity uint32) string {
	switch severity {
	case 0:
		return "VERBOSE"
	case 1:
		return "INFO"
	case 2:
		return "WARNING"
	case 3:
		return "ERROR"
	case 4:
		return "FATAL"
	default:
		return ""
	}
}

// ---------------------------------------------------------------------------
// System log helpers – collector-generated events
// ---------------------------------------------------------------------------

// emitSystemLog creates and forwards a single collector-generated log record.
// setAttrs is an optional callback to add extra attributes; pass nil if none.
// It is safe to call concurrently from goroutines.
func (r *logsReceiver) emitSystemLog(
	severity plog.SeverityNumber,
	severityText, eventName, body string,
	setAttrs func(pcommon.Map),
) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	ra := rl.Resource().Attributes()
	ra.PutStr("service.name", r.settings.ID.Name())
	ra.PutStr("ads.net_id", r.cfg.TargetNetID)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("otelcol-ads/adslogsreceiver")

	now := pcommon.NewTimestampFromTime(time.Now())
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(now)
	lr.SetObservedTimestamp(now)
	lr.SetSeverityNumber(severity)
	lr.SetSeverityText(severityText)
	lr.Body().SetStr(body)
	lr.Attributes().PutStr("event.name", eventName)
	lr.Attributes().PutStr("ads.event.source", "otelcol")
	if setAttrs != nil {
		setAttrs(lr.Attributes())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.nextConsumer.ConsumeLogs(ctx, logs); err != nil {
		r.logger.Warn("Failed to emit system log",
			zap.String("event", eventName),
			zap.Error(err),
		)
	}
}

// onStateChange is registered as the ManagedClientHooks.OnStateChange callback.
// It fires on every TwinCAT ADS state transition and on the first state read.
func (r *logsReceiver) onStateChange(newState, oldState *adsstateinfo.SystemState) {
	if !r.cfg.SystemLogs.Enabled || !r.cfg.SystemLogs.PLCStateChanges {
		return
	}

	sev, sevText := plcStateChangeSeverity(newState.AdsState)
	var body string
	if oldState == nil {
		body = fmt.Sprintf("TwinCAT initial state: %s", newState.AdsState)
	} else {
		body = fmt.Sprintf("TwinCAT state changed: %s \u2192 %s", oldState.AdsState, newState.AdsState)
	}

	r.emitSystemLog(sev, sevText, "ads.plc.state_change", body, func(m pcommon.Map) {
		m.PutStr("ads.state.name", newState.AdsState.String())
		m.PutInt("ads.state.id", int64(newState.AdsState))
		if oldState != nil {
			m.PutStr("ads.state.previous", oldState.AdsState.String())
			m.PutInt("ads.state.previous_id", int64(oldState.AdsState))
		}
	})
}

// plcStateChangeSeverity maps a TwinCAT ADS state to an OTel severity level.
func plcStateChangeSeverity(state adstypes.ADSState) (plog.SeverityNumber, string) {
	switch state {
	case adstypes.ADSStateRun, adstypes.ADSStatePowerGood, adstypes.ADSStateResume:
		return plog.SeverityNumberInfo, "INFO"
	case adstypes.ADSStateStop, adstypes.ADSStateStopping,
		adstypes.ADSStateShutdown, adstypes.ADSStateSuspend:
		return plog.SeverityNumberWarn, "WARN"
	case adstypes.ADSStateError, adstypes.ADSStateException,
		adstypes.ADSStatePowerFailure, adstypes.ADSStateIncompatible,
		adstypes.ADSStateInvalid:
		return plog.SeverityNumberError, "ERROR"
	default:
		return plog.SeverityNumberInfo, "INFO"
	}
}
