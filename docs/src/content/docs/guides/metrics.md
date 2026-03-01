---
title: Metrics and tracing
description: Export Surefire metrics and traces using OpenTelemetry.
---

Surefire emits metrics via `System.Diagnostics.Metrics` and traces via `System.Diagnostics.ActivitySource`. Both use the name `"Surefire"`.

## Instruments

| Instrument | Type | Unit | Description |
|---|---|---|---|
| `surefire.runs.claimed` | Counter | — | Runs claimed by workers |
| `surefire.runs.completed` | Counter | — | Runs completed successfully |
| `surefire.runs.failed` | Counter | — | Runs that reached dead letter |
| `surefire.runs.cancelled` | Counter | — | Runs cancelled |
| `surefire.runs.duration.ms` | Histogram | ms | Run execution duration |

Current counters and histograms are emitted without additional metric tags.

## Traces

The `"Surefire"` activity source creates `surefire.run.execute` spans with these tags:

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
        metrics.AddMeter("Surefire");
        metrics.AddOtlpExporter();
        // metrics.AddConsoleExporter(); // uncomment for local dev
    })
    .WithTracing(tracing =>
    {
        tracing.AddSource("Surefire");
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
    .WithMetrics(m => m.AddMeter("Surefire"))
    .WithTracing(t => t.AddSource("Surefire"));
```
