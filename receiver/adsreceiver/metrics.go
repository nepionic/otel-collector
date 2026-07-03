package adsreceiver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jarmocluyse/ads-go/pkg/ads"
	"github.com/nepionic/otelcol-ads/internal/adsbridge"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// metricsSignal manages both the pull subscription path and the push ring
// buffer path for ADS metric collection. It shares its ADS connection with
// logsSignal (if registered) via core.
type metricsSignal struct {
	cfg    *MetricsConfig
	core   *adsCore
	next   consumer.Metrics
	set    receiver.Settings
	logger *zap.Logger

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
}

func newMetricsSignal(cfg *MetricsConfig, core *adsCore, set receiver.Settings, next consumer.Metrics) *metricsSignal {
	return &metricsSignal{
		cfg:      cfg,
		core:     core,
		set:      set,
		next:     next,
		logger:   set.TelemetrySettings.Logger,
		pushWICh: make(chan uint32, 64),
	}
}

// trySubscribe tries to set up all pull and push subscriptions atomically.
// Returns (nil, true) on success, (nil, false) on an ADS-level error (retry
// without reconnect), (err, false) on a network error (needs reconnect).
// On any failure it cleans up any partial subscriptions from this attempt.
func (s *metricsSignal) trySubscribe() (netErr error, done bool) {
	client := s.core.client.Client()

	// Clean up any leftover subscriptions from a previous failed attempt.
	s.pullMu.Lock()
	for _, sub := range s.pullSubs {
		_ = client.Unsubscribe(sub)
	}
	s.pullSubs = nil
	s.pullMu.Unlock()

	if len(s.cfg.Subscriptions) > 0 {
		if err := s.subscribePull(client); err != nil {
			s.pullMu.Lock()
			for _, sub := range s.pullSubs {
				_ = client.Unsubscribe(sub)
			}
			s.pullSubs = nil
			s.pullMu.Unlock()
			if adsbridge.IsNetworkError(err) {
				return err, false
			}
			s.logger.Debug("PLC symbols not yet available; waiting",
				zap.Int("configured_subscriptions", len(s.cfg.Subscriptions)),
				zap.Error(err),
				zap.Duration("retry_in", s.core.cfg.StatePollingInterval),
			)
			return nil, false
		}
	}

	if s.cfg.PushRing.Enabled {
		if err := s.initPushRing(client); err != nil {
			s.pullMu.Lock()
			for _, sub := range s.pullSubs {
				_ = client.Unsubscribe(sub)
			}
			s.pullSubs = nil
			s.pullMu.Unlock()
			if adsbridge.IsNetworkError(err) {
				return err, false
			}
			s.logger.Debug("Push ring symbol not yet available; waiting",
				zap.String("symbol", s.cfg.PushRing.Symbol),
				zap.Error(err),
				zap.Duration("retry_in", s.core.cfg.StatePollingInterval),
			)
			return nil, false
		}
		s.core.wg.Add(1)
		go s.pushDrainLoop()
	}

	s.logger.Info("ADS metrics signal subscribed",
		zap.String("target_net_id", s.core.cfg.TargetNetID),
		zap.Int("pull_subscriptions", len(s.cfg.Subscriptions)),
		zap.Bool("push_ring_enabled", s.cfg.PushRing.Enabled),
	)
	return nil, true
}

// teardown unsubscribes pull and push subscriptions. Safe to call even if
// subscribe never succeeded.
func (s *metricsSignal) teardown() {
	client := s.core.client.Client()

	s.pullMu.Lock()
	for _, sub := range s.pullSubs {
		_ = client.Unsubscribe(sub)
	}
	s.pullSubs = nil
	s.pullMu.Unlock()

	s.pushRingMu.Lock()
	if s.pushSub != nil {
		_ = client.Unsubscribe(s.pushSub)
		s.pushSub = nil
	}
	s.pushRingMu.Unlock()
}

// ---------------------------------------------------------------------------
// Pull path
// ---------------------------------------------------------------------------

// subscribePull creates ADS subscriptions for each configured symbol.
func (s *metricsSignal) subscribePull(client *ads.Client) error {
	s.pullMu.Lock()
	defer s.pullMu.Unlock()

	for i := range s.cfg.Subscriptions {
		sub := &s.cfg.Subscriptions[i]
		if err := s.subscribePullOne(client, sub); err != nil {
			return err
		}
	}
	return nil
}

func (s *metricsSignal) subscribePullOne(client *ads.Client, sub *SubscriptionConfig) error {
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
		md := s.pullValueToMetric(capturedSub, data.Value, data.Timestamp)
		if md.DataPointCount() == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.next.ConsumeMetrics(ctx, md); err != nil {
			s.logger.Error("ConsumeMetrics (pull) failed",
				zap.String("metric", capturedSub.Name),
				zap.Error(err),
			)
		}
	}

	active, err := client.SubscribeValue(
		s.core.cfg.PLCPort,
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

	s.pullSubs = append(s.pullSubs, active)
	s.logger.Debug("Subscribed to PLC variable",
		zap.String("symbol", sub.Symbol),
		zap.String("metric", sub.Name),
		zap.Duration("cycle_time", cycleTime),
	)
	return nil
}

// pullValueToMetric converts an ADS subscription notification to a single
// pmetric.Metrics value. Returns an empty Metrics if the value cannot be
// converted to float64.
func (s *metricsSignal) pullValueToMetric(
	sub *SubscriptionConfig,
	value any,
	ts time.Time,
) pmetric.Metrics {
	f64, ok := anyToFloat64(value)
	if !ok {
		s.logger.Warn("Pull metric: unsupported value type",
			zap.String("symbol", sub.Symbol),
			zap.String("type", fmt.Sprintf("%T", value)),
		)
		return pmetric.NewMetrics()
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", s.set.ID.Name())
	rm.Resource().Attributes().PutStr("ads.net_id", s.core.cfg.TargetNetID)

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol-ads/adsreceiver/metrics")

	m := sm.Metrics().AppendEmpty()
	m.SetName(sub.Name)
	m.SetUnit(sub.Unit)
	m.SetDescription(sub.Description)

	pTs := pcommon.NewTimestampFromTime(ts)

	var dp pmetric.NumberDataPoint
	switch sub.Type {
	case MetricTypeCounter:
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp = sum.DataPoints().AppendEmpty()
	case MetricTypeUpDownCounter:
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(false)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp = sum.DataPoints().AppendEmpty()
	default: // gauge
		dp = m.SetEmptyGauge().DataPoints().AppendEmpty()
	}
	dp.SetDoubleValue(f64)
	dp.SetTimestamp(pTs)
	for k, v := range sub.Attributes {
		dp.Attributes().PutStr(k, v)
	}

	return md
}

// ---------------------------------------------------------------------------
// Push ring buffer path
// ---------------------------------------------------------------------------

// initPushRing resolves the ring symbol address and registers the write_index
// subscription.
func (s *metricsSignal) initPushRing(client *ads.Client) error {
	// If no ring symbol is configured, discover it by scanning the PLC symbol
	// table for a variable with {attribute 'otelcol_role' := 'metric_ring'},
	// falling back to matching the OTelMetricRing type. There should only
	// ever be one metric ring on a PLC, so this is preferred over requiring
	// an explicit path.
	if s.cfg.PushRing.Symbol == "" {
		sym, err := adsbridge.FindMetricRingSymbol(client, s.core.cfg.PLCPort)
		if err != nil {
			return fmt.Errorf("FindMetricRingSymbol: %w", err)
		}
		s.logger.Info("Discovered metric ring symbol", zap.String("symbol", sym))
		s.cfg.PushRing.Symbol = sym
	}

	if err := s.resolvePushRingSymbol(client); err != nil {
		return err
	}
	return s.subscribePushRing(client)
}

func (s *metricsSignal) resolvePushRingSymbol(client *ads.Client) error {
	sym, err := client.GetSymbol(s.core.cfg.PLCPort, s.cfg.PushRing.Symbol)
	if err != nil {
		return fmt.Errorf("GetSymbol(%q): %w", s.cfg.PushRing.Symbol, err)
	}
	s.pushRingMu.Lock()
	s.pushRingGroup = sym.IndexGroup
	s.pushRingOffset = sym.IndexOffset
	s.pushRingSize = sym.Size
	s.pushRingMu.Unlock()
	return nil
}

func (s *metricsSignal) subscribePushRing(client *ads.Client) error {
	wiPath := s.cfg.PushRing.Symbol + ".head"

	cb := func(data ads.SubscriptionData) {
		wi, ok := data.Value.(uint32)
		if !ok {
			return
		}
		select {
		case s.pushWICh <- wi:
		default:
		}
	}

	cycleTime := s.cfg.PushRing.SubscriptionCycleTime
	if cycleTime == 0 {
		cycleTime = 100 * time.Millisecond
	}

	sub, err := client.SubscribeValue(
		s.core.cfg.PLCPort,
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

	s.pushRingMu.Lock()
	s.pushSub = sub
	s.pushRingMu.Unlock()
	return nil
}

// pushDrainLoop processes write_index notifications and drains ring slots.
func (s *metricsSignal) pushDrainLoop() {
	defer s.core.wg.Done()

	for {
		select {
		case <-s.core.stopCh:
			return
		case newWI := <-s.pushWICh:
			s.drainPushRing(newWI)
		}
	}
}

// drainPushRing reads all new metric slots from the ring and emits them.
func (s *metricsSignal) drainPushRing(newWI uint32) {
	lri := atomic.LoadUint32(&s.pushLocalRI)
	if newWI == lri {
		return
	}

	s.pushRingMu.Lock()
	group := s.pushRingGroup
	offset := s.pushRingOffset
	size := s.pushRingSize
	s.pushRingMu.Unlock()

	if size == 0 {
		return
	}

	rawRing, err := s.core.client.Client().ReadRaw(s.core.cfg.PLCPort, group, offset, size)
	if err != nil {
		s.logger.Warn("ReadRaw metric ring failed", zap.String("symbol", s.cfg.PushRing.Symbol), zap.Error(err))
		return
	}

	result, newRI := adsbridge.DrainMetrics(rawRing, lri, newWI)
	atomic.StoreUint32(&s.pushLocalRI, newRI)

	if result.TornReads > 0 {
		s.logger.Debug("Metric ring: torn reads (mid-write during ADS read)",
			zap.String("symbol", s.cfg.PushRing.Symbol),
			zap.Uint32("count", result.TornReads),
		)
	}
	if result.Overflows > 0 {
		s.logger.Warn("Metric ring: consumer fell behind, slots lost",
			zap.String("symbol", s.cfg.PushRing.Symbol),
			zap.Uint32("lost_slots", result.Overflows),
		)
	}

	if len(result.Slots) == 0 {
		return
	}

	md := s.pushSlotsToMetrics(result.Slots, result.OverflowCnt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.next.ConsumeMetrics(ctx, md); err != nil {
		s.logger.Error("ConsumeMetrics (push) failed", zap.String("symbol", s.cfg.PushRing.Symbol), zap.Error(err))
	}
}

// pushSlotsToMetrics converts a batch of push ring slots to pmetric.Metrics.
func (s *metricsSignal) pushSlotsToMetrics(slots []adsbridge.MetricSlot, overflowCnt uint32) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("service.name", s.set.ID.Name())
	ra.PutStr("ads.net_id", s.core.cfg.TargetNetID)

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("otelcol-ads/adsreceiver/metrics/push")

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
// Reconnect callback (called by core.reconnect after TwinCAT restart)
// ---------------------------------------------------------------------------

func (s *metricsSignal) reconnect(client *ads.Client) error {
	s.logger.Info("Re-establishing ADS metric subscriptions after TwinCAT restart")

	// Unsubscribe and re-subscribe pull variables.
	if len(s.cfg.Subscriptions) > 0 {
		s.pullMu.Lock()
		for _, sub := range s.pullSubs {
			_ = client.Unsubscribe(sub)
		}
		s.pullSubs = nil
		s.pullMu.Unlock()

		if err := s.subscribePull(client); err != nil {
			return fmt.Errorf("pull re-subscribe: %w", err)
		}
	}

	// Re-initialise push ring.
	if s.cfg.PushRing.Enabled {
		s.pushRingMu.Lock()
		if s.pushSub != nil {
			_ = client.Unsubscribe(s.pushSub)
			s.pushSub = nil
		}
		s.pushRingMu.Unlock()

		atomic.StoreUint32(&s.pushLocalRI, 0)

		if err := s.resolvePushRingSymbol(client); err != nil {
			return fmt.Errorf("push ring re-resolve: %w", err)
		}
		if err := s.subscribePushRing(client); err != nil {
			return fmt.Errorf("push ring re-subscribe: %w", err)
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
