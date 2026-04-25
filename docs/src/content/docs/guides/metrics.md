---
title: Metrics and tracing
description: Export Surefire metrics and traces using OpenTelemetry.
---

Surefire emits metrics via `System.Diagnostics.Metrics` and traces via `System.Diagnostics.ActivitySource`. Both use the name `"surefire"` (lowercase — OTel names are case-sensitive). Use `SurefireDiagnostics.MeterName` and `SurefireDiagnostics.ActivitySourceName` if you'd rather not hardcode the string.

## Instruments

| Instrument | Type | Unit | Tags | Description |
|---|---|---|---|---|
| `surefire.runs.claimed` | Counter | | `surefire.job.name` | Runs claimed by workers |
| `surefire.runs.completed` | Counter | | `surefire.job.name` | Runs completed successfully |
| `surefire.runs.failed` | Counter | | `surefire.job.name`, `surefire.dead_letter.reason` | Runs that reached the Failed terminal state. Reason is one of `retries_exhausted`, `no_handler_registered`, `shutdown_interrupted`, `stale_recovery` |
| `surefire.runs.cancelled` | Counter | | `surefire.job.name` | Runs cancelled |
| `surefire.runs.duration.ms` | Histogram | `ms` | `surefire.job.name` | Time from claim to terminal transition |
| `surefire.scheduler.lag.ms` | Histogram | `ms` | `surefire.job.name` | Time between a run's `NotBefore` and when it was actually claimed; growing values mean the cluster is undersized |
| `surefire.store.operation.ms` | Histogram | `ms` | `surefire.store.operation` | Store operation duration |
| `surefire.store.operation.failed` | Counter | | `surefire.store.operation` | Failed store operations |
| `surefire.store.retries` | Counter | | `surefire.service` | Transient store failure retries |
| `surefire.log_entries.dropped` | Counter | | `surefire.drop.reason` | Log entries dropped before store flush |

## Traces

The `"surefire"` activity source creates `surefire.run.execute` spans with these tags:

| Tag | Description |
|---|---|
| `surefire.run.id` | The run ID |
| `surefire.run.job` | The job name |
| `surefire.run.attempt` | Attempt number |
| `surefire.run.parent` | Parent run ID (if any) |

Failed jobs set the span status to `Error` with the exception message.

## Setup

Install the OpenTelemetry packages:

```bash
dotnet add package OpenTelemetry.Extensions.Hosting
dotnet add package OpenTelemetry.Exporter.OpenTelemetryProtocol
dotnet add package OpenTelemetry.Exporter.Console           # optional, for local dev
```

Wire up the meter and activity source in your host builder:

```csharp
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics.AddMeter("surefire");
        metrics.AddOtlpExporter();
        // metrics.AddConsoleExporter(); // uncomment for local dev
    })
    .WithTracing(tracing =>
    {
        tracing.AddSource("surefire");
        tracing.AddOtlpExporter();
        // tracing.AddConsoleExporter(); // uncomment for local dev
    });

builder.Services.AddSurefire();
```

The OTLP exporter sends data to any OpenTelemetry-compatible backend (Jaeger, Grafana, Aspire Dashboard, etc.). By default it connects to `http://localhost:4317`. Configure via environment variables:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_SERVICE_NAME=my-app
```

### .NET Aspire

If you're using .NET Aspire, metrics and traces are collected automatically — just add the meter and source:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter("surefire"))
    .WithTracing(t => t.AddSource("surefire"));
```
