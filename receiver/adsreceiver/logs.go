package adsreceiver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jarmocluyse/ads-go/pkg/ads"
	adsstateinfo "github.com/jarmocluyse/ads-go/pkg/ads/ads-stateinfo"
	adstypes "github.com/jarmocluyse/ads-go/pkg/ads/types"
	"github.com/nepionic/otelcol-ads/internal/adsbridge"
	adslogger "github.com/siyka-au/ads-logger/pkg/ads-logger"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// logsSignal subscribes to the TwinCAT OtelBridge log ring buffer and
// converts each log record to an OpenTelemetry plog.Logs batch. It shares its
// ADS connection with metricsSignal (if registered) via core.
type logsSignal struct {
	cfg    *LogsConfig
	core   *adsCore
	next   consumer.Logs
	set    receiver.Settings
	logger *zap.Logger

	localRI      uint32 // consumer read index – accessed atomically
	ringGroup    uint32 // ADS IndexGroup of the log ring symbol
	ringOffset   uint32 // ADS IndexOffset of the log ring symbol
	ringSize     uint32 // total byte size of the log ring symbol
	subscription *ads.ActiveSubscription
	wmu          sync.Mutex // guards subscription and ring address fields

	wiCh chan uint32 // write_index notifications feed this channel

	// TwinCAT system logger (port 100) subscription.
	loggerCancel context.CancelFunc
	loggerWg     sync.WaitGroup
}

func newLogsSignal(cfg *LogsConfig, core *adsCore, set receiver.Settings, next consumer.Logs) *logsSignal {
	return &logsSignal{
		cfg:    cfg,
		core:   core,
		set:    set,
		next:   next,
		logger: set.TelemetrySettings.Logger,
		wiCh:   make(chan uint32, 64),
	}
}

// trySubscribe attempts one round of: discover the log ring symbol if not
// configured, resolve its address, and subscribe to its write_index. Returns
// (err, false) on a network-level failure (caller must reconnect), (nil,
// false) when the PLC-side resource isn't ready yet (caller retries later),
// (nil, true) once fully subscribed and the drain loop is running.
//
// When push_ring.enabled is false, the ring path is skipped entirely — only
// the TwinCAT system logger (independent of the ring) is started, and this
// reports done immediately.
func (s *logsSignal) trySubscribe() (netErr error, done bool) {
	client := s.core.client.Client()
	plcPort := s.core.cfg.PLCPort

	if !s.cfg.PushRing.Enabled {
		s.startTCLogger(client)
		return nil, true
	}

	// If no ring symbol is configured, discover it by scanning the PLC symbol
	// table for a variable with {attribute 'otelcol_role' := 'log_ring'}.
	if s.cfg.PushRing.Symbol == "" {
		sym, err := adsbridge.FindLogRingSymbol(client, plcPort)
		if err != nil {
			if adsbridge.IsNetworkError(err) {
				s.logger.Warn("TCP connection dropped during symbol discovery", zap.Error(err))
				return err, false
			}
			s.logger.Info("Log ring symbol not yet discovered; waiting",
				zap.Error(err),
				zap.Duration("retry_in", s.core.cfg.StatePollingInterval),
			)
			return nil, false
		}
		s.logger.Info("Discovered log ring symbol", zap.String("symbol", sym))
		s.cfg.PushRing.Symbol = sym
	}

	if err := s.resolveRingSymbol(client, plcPort); err != nil {
		if adsbridge.IsNetworkError(err) {
			s.logger.Warn("TCP connection dropped during symbol lookup", zap.Error(err))
			return err, false
		}
		s.logger.Info("Log ring symbol not yet available; waiting",
			zap.String("symbol", s.cfg.PushRing.Symbol),
			zap.Duration("retry_in", s.core.cfg.StatePollingInterval),
		)
		return nil, false
	}

	if err := s.subscribe(client, plcPort); err != nil {
		if adsbridge.IsNetworkError(err) {
			s.logger.Warn("TCP connection dropped during subscribe", zap.Error(err))
			return err, false
		}
		s.logger.Warn("Subscribe failed; retrying", zap.Error(err))
		return nil, false
	}

	s.logger.Info("ADS logs signal subscribed",
		zap.String("target_net_id", s.core.cfg.TargetNetID),
		zap.String("ring_symbol", s.cfg.PushRing.Symbol),
	)
	s.startTCLogger(client)
	s.core.wg.Add(1)
	go s.drainLoop()
	return nil, true
}

// teardown unsubscribes and stops the TwinCAT logger subscription. Safe to
// call even if subscribe never succeeded.
func (s *logsSignal) teardown() {
	if s.loggerCancel != nil {
		s.loggerCancel()
		s.loggerWg.Wait()
	}

	s.wmu.Lock()
	sub := s.subscription
	s.wmu.Unlock()
	if sub != nil {
		_ = s.core.client.Client().Unsubscribe(sub)
	}
}

// resolveRingSymbol fetches the IndexGroup, IndexOffset and byte size of the
// log ring symbol so we can ReadRaw it efficiently.
func (s *logsSignal) resolveRingSymbol(client *ads.Client, plcPort uint16) error {
	sym, err := client.GetSymbol(plcPort, s.cfg.PushRing.Symbol)
	if err != nil {
		return fmt.Errorf("GetSymbol(%q): %w", s.cfg.PushRing.Symbol, err)
	}
	s.wmu.Lock()
	s.ringGroup = sym.IndexGroup
	s.ringOffset = sym.IndexOffset
	s.ringSize = sym.Size
	s.wmu.Unlock()
	return nil
}

// subscribe registers an ADS on-change notification on the head field.
// head is the first UDINT in OTelLogRing, so its symbol path is
// "<PushRing.Symbol>.head".
func (s *logsSignal) subscribe(client *ads.Client, plcPort uint16) error {
	wiPath := s.cfg.PushRing.Symbol + ".head"

	cb := func(data ads.SubscriptionData) {
		wi, ok := data.Value.(uint32)
		if !ok {
			return
		}
		select {
		case s.wiCh <- wi:
		default:
			// Channel full – drainLoop is lagging. Drop notification; the next
			// one will carry the updated index.
		}
	}

	sub, err := client.SubscribeValue(
		plcPort,
		wiPath,
		cb,
		ads.SubscriptionSettings{
			CycleTime:    s.cfg.PushRing.SubscriptionCycleTime,
			SendOnChange: true,
		},
	)
	if err != nil {
		return fmt.Errorf("SubscribeValue(%q): %w", wiPath, err)
	}

	s.wmu.Lock()
	s.subscription = sub
	s.wmu.Unlock()
	return nil
}

// reconnect is called by adsCore.reconnect after TwinCAT returns to Run state.
func (s *logsSignal) reconnect(client *ads.Client) error {
	if s.cfg.PushRing.Enabled {
		s.logger.Info("Re-resolving ADS log ring symbol after TwinCAT restart")

		plcPort := s.core.cfg.PLCPort

		// Re-resolve symbol address (may change after PLC download).
		if err := s.resolveRingSymbol(client, plcPort); err != nil {
			return err
		}

		// Unsubscribe old, subscribe fresh.
		s.wmu.Lock()
		if s.subscription != nil {
			_ = client.Unsubscribe(s.subscription)
			s.subscription = nil
		}
		s.wmu.Unlock()

		// Reset read cursor so we don't try to replay stale slots.
		atomic.StoreUint32(&s.localRI, 0)

		if err := s.subscribe(client, plcPort); err != nil {
			return err
		}
	}

	s.startTCLogger(client)

	if s.cfg.ConnectionEvents {
		s.emitSystemLog(
			plog.SeverityNumberInfo, "INFO",
			"ads.plc.reconnected",
			"Reconnected to TwinCAT after restart",
			nil,
		)
	}
	return nil
}

// drainLoop blocks on write_index notifications and drains new log slots.
func (s *logsSignal) drainLoop() {
	defer s.core.wg.Done()

	for {
		select {
		case <-s.core.stopCh:
			return
		case newWI := <-s.wiCh:
			s.drain(newWI)
		}
	}
}

// drain reads all new slots from [localRI, newWI) and emits them as OTel logs.
func (s *logsSignal) drain(newWI uint32) {
	lri := atomic.LoadUint32(&s.localRI)
	if newWI == lri {
		return
	}

	// Detect ring overflow before reading: if the PLC has written more than a
	// full ring's worth of entries since our last drain, some will be lost.
	if s.cfg.PushRing.RingOverflows {
		if pending := newWI - lri; pending > adsbridge.LogCapacity {
			lost := pending - adsbridge.LogCapacity/2
			s.emitSystemLog(
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

	s.wmu.Lock()
	group := s.ringGroup
	offset := s.ringOffset
	size := s.ringSize
	s.wmu.Unlock()

	if size == 0 {
		return
	}

	rawRing, err := s.core.client.Client().ReadRaw(s.core.cfg.PLCPort, group, offset, size)
	if err != nil {
		s.logger.Warn("ReadRaw log ring failed", zap.Error(err))
		return
	}

	slots, newRI := adsbridge.DrainLogs(rawRing, lri, newWI)
	atomic.StoreUint32(&s.localRI, newRI)

	if len(slots) == 0 {
		return
	}

	logs := s.slotsToLogs(slots)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.next.ConsumeLogs(ctx, logs); err != nil {
		s.logger.Error("ConsumeLogs failed", zap.Error(err))
	}
}

// slotsToLogs converts parsed log slots to a plog.Logs batch.
func (s *logsSignal) slotsToLogs(slots []adsbridge.LogSlot) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	ra := rl.Resource().Attributes()
	ra.PutStr("service.name", s.set.ID.Name())
	ra.PutStr("ads.net_id", s.core.cfg.TargetNetID)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("otelcol-ads/adsreceiver/logs")

	for _, slot := range slots {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(adsbridge.TimestampNsToTime(slot.TimestampNs)))
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.SetSeverityNumber(adsSeverityToOtel(slot.Severity))
		lr.SetSeverityText(adsSeverityText(slot.Severity))
		lr.Body().SetStr(slot.Message)

		la := lr.Attributes()
		if slot.Source != "" {
			la.PutStr("ads.source", slot.Source)
		}
		for _, a := range slot.Attrs {
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
func (s *logsSignal) emitSystemLog(
	severity plog.SeverityNumber,
	severityText, eventName, body string,
	setAttrs func(pcommon.Map),
) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	ra := rl.Resource().Attributes()
	ra.PutStr("service.name", s.set.ID.Name())
	ra.PutStr("ads.net_id", s.core.cfg.TargetNetID)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("otelcol-ads/adsreceiver/logs")

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
	if err := s.next.ConsumeLogs(ctx, logs); err != nil {
		s.logger.Warn("Failed to emit system log",
			zap.String("event", eventName),
			zap.Error(err),
		)
	}
}

// emitConnectionEstablished is called by adsCore.connectLoop after a
// successful TCP connect.
func (s *logsSignal) emitConnectionEstablished(routerAddr string) {
	if !s.cfg.ConnectionEvents {
		return
	}
	s.emitSystemLog(
		plog.SeverityNumberInfo, "INFO",
		"ads.connection.established",
		"Connected to ADS router",
		func(m pcommon.Map) {
			if routerAddr != "" {
				m.PutStr("ads.router_addr", routerAddr)
			}
		},
	)
}

// onConnectionLost is registered (via core) as the ManagedClientHooks.OnConnectionLost callback.
func (s *logsSignal) onConnectionLost(err error) {
	if s.cfg.ConnectionEvents {
		s.emitSystemLog(
			plog.SeverityNumberWarn, "WARN",
			"ads.connection.lost",
			fmt.Sprintf("ADS connection lost: %v", err),
			nil,
		)
	}
}

// onStateChange is registered (via core) as the ManagedClientHooks.OnStateChange
// callback. It fires on every TwinCAT ADS state transition and on the first
// state read.
func (s *logsSignal) onStateChange(newState, oldState *adsstateinfo.SystemState) {
	if !s.cfg.PLCStateChanges {
		return
	}

	sev, sevText := plcStateChangeSeverity(newState.AdsState)
	var body string
	if oldState == nil {
		body = fmt.Sprintf("TwinCAT initial state: %s", newState.AdsState)
	} else {
		body = fmt.Sprintf("TwinCAT state changed: %s → %s", oldState.AdsState, newState.AdsState)
	}

	s.emitSystemLog(sev, sevText, "ads.plc.state_change", body, func(m pcommon.Map) {
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

// ---------------------------------------------------------------------------
// TwinCAT system logger (ADS port 100) integration
// ---------------------------------------------------------------------------

// startTCLogger starts a subscription to the TwinCAT system logger (ADS port 100).
// Safe to call multiple times — cancels the previous subscription first.
// No-ops when twincat_logger is false.
func (s *logsSignal) startTCLogger(client *ads.Client) {
	if !s.cfg.TCLogger {
		return
	}

	// Cancel and drain any previous subscription.
	if s.loggerCancel != nil {
		s.loggerCancel()
		s.loggerWg.Wait()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.loggerCancel = cancel

	ch, err := adslogger.Subscribe(ctx, client)
	if err != nil {
		s.logger.Warn("Failed to subscribe to TwinCAT system logger", zap.Error(err))
		cancel()
		s.loggerCancel = nil
		return
	}

	s.loggerWg.Add(1)
	go s.tcLoggerLoop(ch)
	s.logger.Info("Subscribed to TwinCAT system logger (ADS port 100)")
}

// tcLoggerLoop reads entries from the TwinCAT logger channel and emits them
// as OTel log records. Exits when the channel is closed (ctx cancelled) or
// the core stops.
func (s *logsSignal) tcLoggerLoop(ch <-chan adslogger.LogEntry) {
	defer s.loggerWg.Done()
	for {
		select {
		case entry, ok := <-ch:
			if !ok {
				return
			}
			s.emitTCLogEntry(entry)
		case <-s.core.stopCh:
			return
		}
	}
}

// emitTCLogEntry converts a TwinCAT logger entry to a structured OTel log record.
// The entry timestamp, sender, port, and type mask are all mapped to attributes.
func (s *logsSignal) emitTCLogEntry(entry adslogger.LogEntry) {
	sev, sevText := tcLoggerSeverity(entry.Types)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	ra := rl.Resource().Attributes()
	ra.PutStr("service.name", s.set.ID.Name())
	ra.PutStr("ads.net_id", s.core.cfg.TargetNetID)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("otelcol-ads/adsreceiver/logs/tclogger")

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(entry.Timestamp))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.SetSeverityNumber(sev)
	lr.SetSeverityText(sevText)
	lr.Body().SetStr(entry.Message)

	la := lr.Attributes()
	la.PutStr("event.name", "ads.twincat.logger")
	la.PutStr("ads.event.source", "twincat")
	la.PutStr("ads.logger.sender", entry.Sender)
	la.PutInt("ads.logger.sender_port", int64(entry.SenderPort))
	if len(entry.Types) > 0 {
		la.PutStr("ads.logger.types", strings.Join(entry.Types, "|"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.next.ConsumeLogs(ctx, logs); err != nil {
		s.logger.Warn("Failed to emit TwinCAT logger entry",
			zap.String("sender", entry.Sender),
			zap.Error(err),
		)
	}
}

// tcLoggerSeverity maps TwinCAT logger type strings to an OTel severity.
// The highest severity present in the types slice wins.
func tcLoggerSeverity(types []string) (plog.SeverityNumber, string) {
	for _, t := range types {
		if t == "error" {
			return plog.SeverityNumberError, "ERROR"
		}
	}
	for _, t := range types {
		if t == "warning" {
			return plog.SeverityNumberWarn, "WARN"
		}
	}
	return plog.SeverityNumberInfo, "INFO"
}
