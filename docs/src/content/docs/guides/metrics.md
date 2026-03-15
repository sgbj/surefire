---
title: Metrics and tracing
description: Export Surefire metrics and traces using OpenTelemetry.
---

Surefire emits metrics via `System.Diagnostics.Metrics` and traces via `System.Diagnostics.ActivitySource`. Both use the name `"Surefire"`.

## Instruments

| Instrument | Type | Unit | Description |
|---|---|---|---|
| `surefire.jobs.executed` | Counter | — | Jobs completed successfully |
| `surefire.jobs.failed` | Counter | — | Job executions that failed |
| `surefire.jobs.retried` | Counter | — | Retry runs created |
| `surefire.jobs.dead_lettered` | Counter | — | Jobs that exhausted all retries |
| `surefire.jobs.cancelled` | Counter | — | Jobs cancelled by user |
| `surefire.jobs.duration` | Histogram | ms | Execution duration |
| `surefire.runs.active` | UpDownCounter | — | Currently executing runs |

All instruments include a `surefire.job.name` tag.

## Traces

The `"Surefire"` activity source creates a span for each job execution with these tags:

| Tag | Description |
|---|---|
| `surefire.job.name` | The job name |
| `surefire.run.id` | The run ID |
| `surefire.node.name` | The node executing the job |

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
