---
title: Metrics and tracing
description: Export Surefire metrics and traces using OpenTelemetry.
---

## Setup

Surefire emits metrics via `System.Diagnostics.Metrics` and traces via `System.Diagnostics.ActivitySource`. Wire them into OpenTelemetry using `SurefireDiagnostics.MeterName` and `SurefireDiagnostics.ActivitySourceName`.

Install the OpenTelemetry packages:

```bash
dotnet add package OpenTelemetry.Extensions.Hosting
dotnet add package OpenTelemetry.Exporter.OpenTelemetryProtocol
```

Wire up the meter and activity source in your host builder:

```csharp
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics.AddMeter(SurefireDiagnostics.MeterName);
        metrics.AddOtlpExporter();
    })
    .WithTracing(tracing =>
    {
        tracing.AddSource(SurefireDiagnostics.ActivitySourceName);
        tracing.AddOtlpExporter();
    });

builder.Services.AddSurefire();
```

## Instruments

| Instrument | Type | Unit | Tags | Description |
|---|---|---|---|---|
| `surefire.runs.claimed` | Counter | | `surefire.job.name` | Runs claimed by workers |
| `surefire.runs.completed` | Counter | | `surefire.job.name` | Runs completed successfully |
| `surefire.runs.failed` | Counter | | `surefire.job.name`, `surefire.dead_letter.reason` | Runs that reached the Failed terminal state. Reason is one of `retries_exhausted`, `no_handler_registered`, `shutdown_interrupted`, `stale_recovery` |
| `surefire.runs.cancelled` | Counter | | `surefire.job.name` | Runs cancelled |
| `surefire.runs.duration.ms` | Histogram | `ms` | `surefire.job.name` | Time from claim to terminal transition |
| `surefire.scheduler.lag.ms` | Histogram | `ms` | `surefire.job.name` | Time between a run's `NotBefore` and when it was actually claimed. Growing values mean the cluster is undersized |
| `surefire.store.operation.ms` | Histogram | `ms` | `surefire.store.operation` | Store operation duration |
| `surefire.store.operation.failed` | Counter | | `surefire.store.operation` | Failed store operations |
| `surefire.store.retries` | Counter | | `surefire.service` | Transient store failure retries |
| `surefire.loop.errors` | Counter | | `surefire.loop` | Background loop tick failures (executor, maintenance, retention, log pump) |
| `surefire.log_entries.dropped` | Counter | | `surefire.drop.reason` | Log entries dropped before store flush |

## Traces

The activity source creates `surefire.run.execute` spans with these tags:

| Tag | Description |
|---|---|
| `surefire.run.id` | The run ID |
| `surefire.run.job` | The job name |
| `surefire.run.attempt` | Attempt number |
| `surefire.run.parent` | Parent run ID (if any) |
| `surefire.job.timeout` | `true` when the attempt was cancelled by `WithTimeout` |

Failed runs set the span status to `Error` with the exception message.
