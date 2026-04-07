// Package adsmetricsreceiver implements an OpenTelemetry Collector Metrics receiver
// that reads PLC variable data from a TwinCAT runtime via the ADS protocol.
//
// Two complementary data paths are supported:
//
//  1. Pull subscriptions – explicit symbol paths listed in the receiver config are
//     subscribed to via SubscribeValue. Each value change produces a metric data point.
//     Suitable for continuously monitored process variables (sensors, states, counters).
//
//  2. Push ring buffer – the PLC uses the Nepionic_Metrics library with an OtelMetricCore
//     backend (from the Nepionic_Metrics_OTel library). OtelMetricCore writes structured
//     metric entries directly into an OtelMetricRing variable, which is exposed as an ADS
//     symbol (e.g. "OtelBridge.MetricRing"). Each entry carries the metric name, unit,
//     value, kind (Gauge/Counter/UpDownCounter), and up to 5 structured key/value attributes.
//     The receiver subscribes to the ring's head counter via ADS notification and drains
//     new entries on each change.
//
// Both paths can be active simultaneously. Pull and push metric names must not
// overlap (they identify distinct OTel instruments).
package adsmetricsreceiver
