// Package adslogsreceiver implements an OpenTelemetry Collector Logs receiver
// that reads structured log entries from a TwinCAT PLC ring buffer over ADS.
//
// The PLC side uses the Nepionic_Log library with an OtelLogCore backend
// (from the Nepionic_Log_OTel library). OtelLogCore writes log entries directly
// into an OtelLogRing variable, which is exposed as an ADS symbol
// (e.g. "OtelBridge.LogRing"). Each entry carries a message body, severity,
// source (instance path), and up to 10 structured key/value attributes.
//
// This receiver subscribes to the ring's head counter via ADS notification,
// drains new entries on each change, and emits them as plog.Logs records
// with attributes mapped to OTel log record attributes.
package adslogsreceiver
