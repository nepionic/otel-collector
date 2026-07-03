// Package adsreceiver implements an OpenTelemetry Collector receiver that
// reads TwinCAT PLC data from a TwinCAT runtime over the ADS protocol,
// emitting logs, metrics, or both from a single receiver instance backed by
// one shared ADS connection.
//
// Logs signal:
//
// Reads structured log entries from a TwinCAT PLC ring buffer via the
// logs.push_ring path (the PLC side uses the Nepionic_Log library with an
// OtelLogCore backend, from the Nepionic_Log_OTel library, which writes log
// entries directly into an OtelLogRing variable exposed as an ADS symbol,
// e.g. "OtelBridge.LogRing"). Each entry carries a message body, severity,
// source (instance path), and up to 10 structured key/value attributes. The
// receiver subscribes to the ring's head counter via ADS notification,
// drains new entries on each change, and emits them as plog.Logs records.
// Set logs.push_ring.enabled to false to skip this path entirely.
//
// Independently of the ring, the receiver can also emit collector-generated
// system logs (TwinCAT state changes, connection events) and optionally
// subscribe to the TwinCAT system logger (ADS port 100) - see LogsConfig.
//
// Metrics signal:
//
// Reads PLC variable data via two complementary paths: pull subscriptions
// (explicit symbol paths subscribed to via SubscribeValue) and a push ring
// buffer (the PLC uses the Nepionic_Metrics library with an OtelMetricCore
// backend, writing structured metric entries into an OtelMetricRing variable).
// Both paths can be active simultaneously - see MetricsConfig.
//
// A receiver instance is configured with `logs:` and/or `metrics:` blocks;
// whichever are present determine which signal(s) that instance supports. If
// the same receiver ID is referenced from both a logs pipeline and a metrics
// pipeline, the underlying TCP connection, connect/reconnect loop, and
// TwinCAT-restart handling are shared rather than duplicated.
package adsreceiver
