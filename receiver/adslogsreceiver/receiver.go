package adslogsreceiver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jarmocluyse/ads-go/pkg/ads"
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
	subOnce      sync.Once
	subscription *ads.ActiveSubscription
	wmu          sync.Mutex // guards subscription and ring address fields

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
	r.client = adsbridge.NewManagedClient(r.cfg.toBridgeConfig(), r.logger, r.reconnect)

	if err := r.client.Connect(); err != nil {
		return fmt.Errorf("adslogsreceiver: ADS connect failed: %w", err)
	}

	if err := r.resolveRingSymbol(); err != nil {
		return fmt.Errorf("adslogsreceiver: ring symbol resolution failed: %w", err)
	}

	if err := r.subscribe(); err != nil {
		return fmt.Errorf("adslogsreceiver: initial subscription failed: %w", err)
	}

	r.wg.Add(1)
	go r.drainLoop()

	r.logger.Info("ADS logs receiver started",
		zap.String("target_net_id", r.cfg.TargetNetID),
		zap.String("ring_symbol", r.cfg.LogRingSymbol),
	)
	return nil
}

// Shutdown implements component.Component.
func (r *logsReceiver) Shutdown(_ context.Context) error {
	close(r.stopCh)
	r.wg.Wait()

	r.wmu.Lock()
	if r.subscription != nil {
		_ = r.client.Client().Unsubscribe(r.subscription)
	}
	r.wmu.Unlock()

	r.client.Disconnect()
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

// subscribe registers an ADS on-change notification on the write_index field.
// The write_index is the first UDINT in the header, so its symbol path is
// "<LogRingSymbol>.header.write_index".
func (r *logsReceiver) subscribe() error {
	wiPath := r.cfg.LogRingSymbol + ".header.write_index"

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

	return r.subscribe()
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
			if a.Key != "" {
				la.PutStr(a.Key, a.Value)
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
