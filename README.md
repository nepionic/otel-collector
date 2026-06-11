# OpenTelemetry Collector - Beckhoff TwinCAT ADS

A custom [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) distribution that bridges **Beckhoff TwinCAT ADS** telemetry to any OTLP-compatible backend (Grafana, Jaeger, Prometheus, etc.).

## Receivers

| Receiver | Type | Description |
|---|---|---|
| `adsevents` | Logs | Reads structured log entries from a TwinCAT PLC ring buffer over ADS (requires the *Nepionic_Log_OTel* PLC library). |
| `adsmetrics` | Metrics | Reads PLC variable data via ADS subscriptions (pull) and/or a TwinCAT ring buffer (push, requires the *Nepionic_Metrics_OTel* PLC library). |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| [Go](https://go.dev/dl/) | 1.25+ | Required to build from source. |
| [OTel Collector Builder (`ocb`)](https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder) | 0.149.0 | Used to assemble the collector binary. |
| [ads-go](https://github.com/jarmocluyse/ads-go) | local clone | ADS client library; pulled from a sibling workspace path (see below). |

---

## Clone & workspace setup

This repository uses a `go.work` file that references a local clone of **ads-go** as a sibling directory.

```
parent-folder/
├── nepionic/otel/          ← this repo
└── siyka/ads-go/           ← ads-go dependency (local)
```

1. Clone both repositories:

   ```sh
   # ads-go dependency
   git clone https://github.com/jarmocluyse/ads-go path/to/siyka/ads-go

   # this repo
   git clone <otelcol-ads-repo-url> path/to/nepionic/otel
   ```

2. The `go.work` file already contains the local replace directive:

   ```
   use (
       .
       ./dist
       ../siyka/ads-go   # ← adjust if your directory layout differs
   )
   ```

   If your `ads-go` clone lives elsewhere, update the path in both `go.work` **and** the `replace` directive in `go.mod`.

---

## Install the OTel Collector Builder

```sh
go install go.opentelemetry.io/collector/cmd/builder@v0.149.0
```

Verify it is on your `PATH`:

```sh
builder --version
```

---

## Build

From the root of this repository:

```sh
builder --config builder-config.yaml
```

The output binary is written to:

| OS | Path |
|---|---|
| Windows | `.\dist\otelcol-ads.exe` |
| Linux / macOS | `./dist/otelcol-ads` |

---

## Run

Copy one of the provided example configs and edit it for your environment, then start the collector:

```sh
# Windows
.\dist\otelcol-ads.exe --config config.yaml

# Linux / macOS
./dist/otelcol-ads --config config.yaml
```

### Example configs

| File | Purpose |
|---|---|
| `config-example.yaml` | Full-featured example with OTLP and debug exporters. |
| `config-file-logging.yaml` | Writes telemetry to local log files (useful for edge deployments). |

### Minimal receiver config

```yaml
receivers:
  adsevents:
    target_net_id: "192.168.1.1.1.1"   # AMS Net ID of the TwinCAT system
    plc_port: 851
    router_port: 48898                  # omit router_addr to use a local TwinCAT router

  adsmetrics:
    target_net_id: "192.168.1.1.1.1"
    plc_port: 851
    router_port: 48898
    subscriptions:
      - symbol: "MAIN.temperature"
        name: "plc.reactor.temperature"
        unit: "Cel"
        type: gauge
        cycle_time: 500ms
```

For a direct TCP connection (no local TwinCAT router — typical on Linux), add:

```yaml
router_addr: "192.168.1.1"   # IP address of the PLC / router
```

---

## Connection options

| Option | Default | Description |
|---|---|---|
| `target_net_id` | *(required)* | AMS Net ID of the TwinCAT system. |
| `plc_port` | `851` | ADS port of the PLC runtime (TC3 runtime 1). |
| `router_addr` | *(empty)* | IP/hostname of the ADS router. Leave empty when a local TwinCAT router is installed. |
| `router_port` | `48898` | TCP port of the ADS router. |
| `state_polling_interval` | `2s` | How often TwinCAT state is polled for restart detection. |
| `connect_retry_initial_interval` | `1s` | First backoff delay after a failed connection attempt. |
| `connect_retry_max_interval` | `30s` | Maximum backoff ceiling (doubles on each attempt). |
