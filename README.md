# OpenTelemetry Collector - Beckhoff TwinCAT ADS

A custom [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) distribution that bridges **Beckhoff TwinCAT ADS** telemetry to any OTLP-compatible backend (Grafana, Jaeger, Prometheus, etc.).

## Receivers

| Receiver | Type | Description |
|---|---|---|
| `ads` | Logs and/or Metrics | Reads from a single shared ADS connection. Enable the `logs:` block for structured log entries from a TwinCAT PLC ring buffer (requires the *Nepionic_Log_OTel* PLC library), the `metrics:` block for PLC variable data via ADS subscriptions (pull) and/or a TwinCAT ring buffer (push, requires the *Nepionic_Metrics_OTel* PLC library), or both. |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| [Go](https://go.dev/dl/) | 1.25+ | Required to build from source. |
| [OTel Collector Builder (`ocb`)](https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder) | 0.149.0 | Used to assemble the collector binary. |
| [ads-go](https://github.com/jarmocluyse/ads-go) | local clone | ADS client library; pulled from a sibling workspace path (see below). |
| [ads-logger](https://github.com/siyka-au/ads-logger) | local clone | TwinCAT system logger (ADS port 100) client; pulled from a sibling workspace path (see below). |

---

## Clone & workspace setup

This repository uses a `go.work` file that references local clones of **ads-go** and **ads-logger** as sibling directories.

```
workspaces/
├── nepionic/otel/          ← this repo
└── siyka/
    ├── ads-go/             ← ads-go dependency (local)
    └── ads-logger/         ← ads-logger dependency (local)
```

1. Clone all three repositories:

   ```sh
   # ads-go dependency
   git clone https://github.com/jarmocluyse/ads-go path/to/siyka/ads-go

   # ads-logger dependency
   git clone https://github.com/siyka-au/ads-logger path/to/siyka/ads-logger

   # this repo
   git clone <otelcol-ads-repo-url> path/to/nepionic/otel
   ```

2. The `go.work` file already contains the local `use` directives:

   ```
   use (
       .
       ./dist
       ../../siyka/ads-go       # ← adjust if your directory layout differs
       ../../siyka/ads-logger   # ← adjust if your directory layout differs
   )
   ```

   If your clones live elsewhere, update the paths in `go.work`. `go.mod` itself has no `replace` directives — local resolution is handled entirely by `go.work`.

3. For building the standalone collector binary (via `ocb`, not `go build`), copy `builder-config.local.yaml.example` to `builder-config.local.yaml` and adjust the same paths there — see [Build](#build) below.

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

From the root of this repository. `ocb`'s `--config` flag only accepts one file (repeating it just overrides the previous value, it does not merge), so concatenate the base config with your local module-path overrides first:

```pwsh
Get-Content builder-config.yaml, builder-config.local.yaml | Set-Content merged.yaml
builder --config merged.yaml
```

```sh
# Linux / macOS
cat builder-config.yaml builder-config.local.yaml > merged.yaml
builder --config merged.yaml
```

`builder-config.local.yaml` is gitignored and holds the `replaces:` entries for your local `ads-go`/`ads-logger` clones — copy `builder-config.local.yaml.example` to get started (see [Clone & workspace setup](#clone--workspace-setup)).

The output binary is written to:

| OS            | Path                     |
|---------------|--------------------------|
| Windows       | `.\dist\otelcol-ads.exe` |
| Linux / macOS | `./dist/otelcol-ads`     |

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

| File                  | Purpose                                                |
|-----------------------|---------------------------------------------------------|
| `config-example.yaml` | Full-featured example with OTLP and debug exporters.   |

### Minimal receiver config

```yaml
receivers:
  ads:
    target_net_id: "192.168.1.1.1.1"   # AMS Net ID of the TwinCAT system
    plc_port: 851
    router_port: 48898                  # omit router_addr to use a local TwinCAT router

    logs: {}                            # enables the logs signal with its defaults

    metrics:
      subscriptions:
        - symbol: "MAIN.temperature"
          name: "plc.reactor.temperature"
          unit: "Cel"
          type: gauge
          cycle_time: 500ms
```

Referencing the same `ads` receiver ID from both a `logs:` and a `metrics:` pipeline shares one underlying ADS connection; referencing it from only one pipeline only requires that pipeline's block to be present.

For a direct TCP connection (no local TwinCAT router — typical on Linux), add:

```yaml
router_addr: "192.168.1.1"   # IP address of the PLC / router
```

---

## Connection options

| Option                           | Default | Description |
|----------------------------------|---|---|
| `target_net_id`                  | *(required)* | AMS Net ID of the TwinCAT system. |
| `plc_port`                       | `851` | ADS port of the PLC runtime (TC3 runtime 1). |
| `router_addr`                    | *(empty)* | IP/hostname of the ADS router. Leave empty when a local TwinCAT router is installed. |
| `router_port`                    | `48898` | TCP port of the ADS router. |
| `state_polling_interval`         | `2s`     | How often TwinCAT state is polled for restart detection. |
| `connect_retry_initial_interval` | `1s` | First backoff delay after a failed connection attempt. |
| `connect_retry_max_interval`     | `30s` | Maximum backoff ceiling (doubles on each attempt). |
