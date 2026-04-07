package adsmetricsreceiver

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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// metricsReceiver manages both the pull subscription path and the push ring
// buffer path for ADS metric collection.
type metricsReceiver struct {
	cfg          *Config
	settings     receiver.Settings
	nextConsumer consumer.Metrics
	logger       *zap.Logger

	client *adsbridge.ManagedClient

	// Pull path fields
	pullSubs []*ads.ActiveSubscription
	pullMu   sync.Mutex

	// Push ring buffer path fields
	pushLocalRI    uint32 // consumer index – accessed atomically
	pushRingGroup  uint32
	pushRingOffset uint32
	pushRingSize   uint32
	pushRingMu     sync.Mutex
	pushSub        *ads.ActiveSubscription
	pushWICh       chan uint32

	connected atomic.Bool // true once connect+subscribe succeeded

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newMetricsReceiver(set receiver.Settings, cfg *Config, next consumer.Metrics) *metricsReceiver {
	return &metricsReceiver{
		cfg:          cfg,
		settings:     set,
		nextConsumer: next,
		logger:       set.TelemetrySettings.Logger,
		pushWICh:     make(chan uint32, 64),
		stopCh:       make(chan struct{}),
	}
}

// Start implements component.Component.
func (r *metricsReceiver) Start(_ context.Context, _ component.Host) error {
	r.client = adsbridge.NewManagedClient(r.cfg.toBridgeConfig(), r.logger, r.reconnect)

	r.wg.Add(1)
	go r.connectLoop()

	r.logger.Info("ADS metrics receiver starting – connecting in background",
		zap.String("target_net_id", r.cfg.TargetNetID),
		zap.Duration("retry_max_interval", r.cfg.ConnectRetryMaxInterval),
	)
	return nil
}

// connectLoop attempts the full connect+subscribe sequence, retrying with
// exponential backoff until it succeeds or stopCh is closed.
func (r *metricsReceiver) connectLoop() {
	defer r.wg.Done()

	backoff := r.cfg.ConnectRetryInitialInterval
	attempt := 0

	for {
		attempt++

		if attempt > 1 {
			r.logger.Info("Retrying ADS metrics connection",
				zap.Int("attempt", attempt),
				zap.Duration("backoff", backoff),
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
		}

		if err := r.client.Connect(); err != nil {
			r.logger.Warn("ADS connect failed",
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			continue
		}

		if len(r.cfg.Subscriptions) > 0 {
			if err := r.subscribePull(); err != nil {
				r.logger.Warn("ADS pull subscriptions failed",
					zap.Int("attempt", attempt),
					zap.Error(err),
				)
				r.client.Disconnect()
				continue
			}
		}

		if r.cfg.PushRing.Enabled {
			if err := r.initPushRing(); err != nil {
				r.logger.Warn("ADS push ring init failed",
					zap.Int("attempt", attempt),
					zap.Error(err),
				)
				r.pullMu.Lock()
				for _, s := range r.pullSubs {
					_ = r.client.Client().Unsubscribe(s)
				}
				r.pullSubs = nil
				r.pullMu.Unlock()
				r.client.Disconnect()
				continue
			}
			r.wg.Add(1)
			go r.pushDrainLoop()
		}

		r.connected.Store(true)
		r.logger.Info("ADS metrics receiver connected",
			zap.String("target_net_id", r.cfg.TargetNetID),
			zap.Int("pull_subscriptions", len(r.cfg.Subscriptions)),
			zap.Bool("push_ring_enabled", r.cfg.PushRing.Enabled),
			zap.Int("attempts", attempt),
		)
		return
	}
}

// Shutdown implements component.Component.
func (r *metricsReceiver) Shutdown(_ context.Context) error {
	close(r.stopCh)
	r.wg.Wait()

	if r.connected.Load() {
		// Unsubscribe pull.
		r.pullMu.Lock()
		for _, sub := range r.pullSubs {
			_ = r.client.Client().Unsubscribe(sub)
		}
		r.pullSubs = nil
		r.pullMu.Unlock()

		// Unsubscribe push.
		r.pushRingMu.Lock()
		if r.pushSub != nil {
			_ = r.client.Client().Unsubscribe(r.pushSub)
			r.pushSub = nil
		}
		r.pushRingMu.Unlock()

		r.client.Disconnect()
	}

	r.logger.Info("ADS metrics receiver stopped")
	return nil
}

// ---------------------------------------------------------------------------
// Pull path
// ---------------------------------------------------------------------------

// subscribePull creates ADS subscriptions for each configured symbol.
func (r *metricsReceiver) subscribePull() error {
	r.pullMu.Lock()
	defer r.pullMu.Unlock()

	for i := range r.cfg.Subscriptions {
		sub := &r.cfg.Subscriptions[i]
		if err := r.subscribePullOne(sub); err != nil {
			return err
		}
	}
	return nil
}

func (r *metricsReceiver) subscribePullOne(sub *SubscriptionConfig) error {
	cycleTime := sub.CycleTime
	if cycleTime == 0 {
		cycleTime = 500 * time.Millisecond
	}
	metricType := sub.Type
	if metricType == "" {
		metricType = MetricTypeGauge
	}

	// Capture loop variable safely.
	capturedSub := sub

	cb := func(data ads.SubscriptionData) {
		md := r.pullValueToMetric(capturedSub, data.Value, data.Timestamp)
		if md.DataPointCount() == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.nextConsumer.ConsumeMetrics(ctx, md); err != nil {
			r.logger.Error("ConsumeMetrics (pull) failed",
				zap.String("metric", capturedSub.Name),
				zap.Error(err),
			)
		}
	}

	active, err := r.client.Client().SubscribeValue(
		r.cfg.PLCPort,
		sub.Symbol,
		cb,
		ads.SubscriptionSettings{
			CycleTime:    cycleTime,
			SendOnChange: sub.SendOnChange,
		},
	)
	if err != nil {
		return fmt.Errorf("SubscribeValue(%q): %w", sub.Symbol, err)
	}

	r.pullSubs = append(r.pullSubs, active)
	r.logger.Debug("Subscribed to PLC variable",
		zap.String("symbol", sub.Symbol),
		zap.String("metric", sub.Name),
		zap.Duration("cycle_time", cycleTime),
	)
	return nil
}

// pullValueToMetric converts an ADS subscription notification to a single
// pmetric.Metrics value. Returns an empty Metrics if the value cannot be
// converted to float64.
func (r *metricsReceiver) pullValueToMetric(
	sub *SubscriptionConfig,
	value any,
	ts time.Time,
) pmetric.Metrics {
	f64, ok := anyToFloat64(value)
	if !ok {
		r.logger.Warn("Pull metric: unsupported value type",
			zap.String("symbol", sub.Symbol),
			zap.String("type", fmt.Sprintf("%T", value)),
		)
		return pmetric.NewMetrics()
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", r.settings.ID.Name())
	rm.Resource().Attributes().PutStr("ads.net_id", r.cfg.TargetNetID)

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol-ads/adsmetricsreceiver")

	m := sm.Metrics().AppendEmpty()
	m.SetName(sub.Name)
	m.SetUnit(sub.Unit)
	m.SetDescription(sub.Description)

	pTs := pcommon.NewTimestampFromTime(ts)

	switch sub.Type {
	case MetricTypeCounter:
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := sum.DataPoints().AppendEmpty()
		dp.SetDoubleValue(f64)
		dp.SetTimestamp(pTs)
	case MetricTypeUpDownCounter:
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(false)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := sum.DataPoints().AppendEmpty()
		dp.SetDoubleValue(f64)
		dp.SetTimestamp(pTs)
	default: // gauge
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(f64)
		dp.SetTimestamp(pTs)
	}

	return md
}

// ---------------------------------------------------------------------------
// Push ring buffer path
// ---------------------------------------------------------------------------

// initPushRing resolves the ring symbol address and registers the write_index
// subscription.
func (r *metricsReceiver) initPushRing() error {
	if err := r.resolvePushRingSymbol(); err != nil {
		return err
	}
	return r.subscribePushRing()
}

func (r *metricsReceiver) resolvePushRingSymbol() error {
	sym, err := r.client.Client().GetSymbol(r.cfg.PLCPort, r.cfg.PushRing.Symbol)
	if err != nil {
		return fmt.Errorf("GetSymbol(%q): %w", r.cfg.PushRing.Symbol, err)
	}
	r.pushRingMu.Lock()
	r.pushRingGroup = sym.IndexGroup
	r.pushRingOffset = sym.IndexOffset
	r.pushRingSize = sym.Size
	r.pushRingMu.Unlock()
	return nil
}

func (r *metricsReceiver) subscribePushRing() error {
	wiPath := r.cfg.PushRing.Symbol + ".header.write_index"

	cb := func(data ads.SubscriptionData) {
		wi, ok := data.Value.(uint32)
		if !ok {
			return
		}
		select {
		case r.pushWICh <- wi:
		default:
		}
	}

	cycleTime := r.cfg.PushRing.SubscriptionCycleTime
	if cycleTime == 0 {
		cycleTime = 100 * time.Millisecond
	}

	sub, err := r.client.Client().SubscribeValue(
		r.cfg.PLCPort,
		wiPath,
		cb,
		ads.SubscriptionSettings{
			CycleTime:    cycleTime,
			SendOnChange: true,
		},
	)
	if err != nil {
		return fmt.Errorf("SubscribeValue(%q): %w", wiPath, err)
	}

	r.pushRingMu.Lock()
	r.pushSub = sub
	r.pushRingMu.Unlock()
	return nil
}

// pushDrainLoop processes write_index notifications and drains ring slots.
func (r *metricsReceiver) pushDrainLoop() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		case newWI := <-r.pushWICh:
			r.drainPushRing(newWI)
		}
	}
}

// drainPushRing reads all new metric slots from the ring and emits them.
func (r *metricsReceiver) drainPushRing(newWI uint32) {
	lri := atomic.LoadUint32(&r.pushLocalRI)
	if newWI == lri {
		return
	}

	r.pushRingMu.Lock()
	group := r.pushRingGroup
	offset := r.pushRingOffset
	size := r.pushRingSize
	r.pushRingMu.Unlock()

	if size == 0 {
		return
	}

	rawRing, err := r.client.Client().ReadRaw(r.cfg.PLCPort, group, offset, size)
	if err != nil {
		r.logger.Warn("ReadRaw metric ring failed", zap.Error(err))
		return
	}

	result, newRI := adsbridge.DrainMetrics(rawRing, lri, newWI)
	atomic.StoreUint32(&r.pushLocalRI, newRI)

	if result.TornReads > 0 {
		r.logger.Debug("Metric ring: torn reads (mid-write during ADS read)",
			zap.Uint32("count", result.TornReads),
		)
	}
	if result.Overflows > 0 {
		r.logger.Warn("Metric ring: consumer fell behind, slots lost",
			zap.Uint32("lost_slots", result.Overflows),
		)
	}

	if len(result.Slots) == 0 {
		return
	}

	md := r.pushSlotsToMetrics(result.Slots, result.OverflowCnt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.nextConsumer.ConsumeMetrics(ctx, md); err != nil {
		r.logger.Error("ConsumeMetrics (push) failed", zap.Error(err))
	}
}

// pushSlotsToMetrics converts a batch of push ring slots to pmetric.Metrics.
func (r *metricsReceiver) pushSlotsToMetrics(slots []adsbridge.MetricSlot, overflowCnt uint32) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("service.name", r.settings.ID.Name())
	ra.PutStr("ads.net_id", r.cfg.TargetNetID)

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol-ads/adsmetricsreceiver/push")

	for _, slot := range slots {
		m := sm.Metrics().AppendEmpty()
		m.SetName(slot.Name)
		m.SetUnit(slot.Unit)

		ts := pcommon.NewTimestampFromTime(adsbridge.TimestampNsToTime(slot.TimestampNs))

		var dp pmetric.NumberDataPoint
		switch slot.Kind {
		case adsbridge.MetricKindCounter:
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			dp = sum.DataPoints().AppendEmpty()
		case adsbridge.MetricKindUpDownCounter:
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(false)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			dp = sum.DataPoints().AppendEmpty()
		default: // gauge
			dp = m.SetEmptyGauge().DataPoints().AppendEmpty()
		}

		dp.SetDoubleValue(slot.ValueF64)
		dp.SetTimestamp(ts)
		for _, a := range slot.Attrs {
			if a.Key != "" {
				dp.Attributes().PutStr(a.Key, a.Value)
			}
		}
	}

	// Emit the buffer overflow counter as a self-observability metric.
	if overflowCnt > 0 {
		om := sm.Metrics().AppendEmpty()
		om.SetName("otelcol_ads.push_ring.overflow_total")
		om.SetDescription("Total number of metric ring buffer overflow events reported by the PLC")
		g := om.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetIntValue(int64(overflowCnt))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	}

	return md
}

// ---------------------------------------------------------------------------
// Reconnect callback (called by ManagedClient after TwinCAT restart)
// ---------------------------------------------------------------------------

func (r *metricsReceiver) reconnect(client *ads.Client) error {
	r.logger.Info("Re-establishing ADS metric subscriptions after TwinCAT restart")

	// Re-subscribe pull variables.
	if len(r.cfg.Subscriptions) > 0 {
		r.pullMu.Lock()
		for _, sub := range r.pullSubs {
			_ = client.Unsubscribe(sub)
		}
		r.pullSubs = nil
		r.pullMu.Unlock()

		if err := r.subscribePull(); err != nil {
			r.logger.Error("Failed to re-subscribe pull metrics", zap.Error(err))
		}
	}

	// Re-initialise push ring.
	if r.cfg.PushRing.Enabled {
		r.pushRingMu.Lock()
		if r.pushSub != nil {
			_ = client.Unsubscribe(r.pushSub)
			r.pushSub = nil
		}
		r.pushRingMu.Unlock()

		atomic.StoreUint32(&r.pushLocalRI, 0)

		if err := r.resolvePushRingSymbol(); err != nil {
			r.logger.Error("Failed to re-resolve push ring symbol", zap.Error(err))
		} else if err := r.subscribePushRing(); err != nil {
			r.logger.Error("Failed to re-subscribe push ring", zap.Error(err))
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// anyToFloat64 converts the Go types that ads-go returns for numeric PLC types
// to float64. Returns (0, false) for non-numeric types (e.g. string, bool).
func anyToFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int64:
		return float64(t), true
	case int32:
		return float64(t), true
	case int16:
		return float64(t), true
	case int8:
		return float64(t), true
	case uint64:
		return float64(t), true
	case uint32:
		return float64(t), true
	case uint16:
		return float64(t), true
	case uint8:
		return float64(t), true
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}
