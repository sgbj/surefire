---
title: Getting started
description: Get up and running with Surefire in minutes.
---

## Installation

```bash
dotnet add package Surefire
dotnet add package Surefire.Dashboard
dotnet add package Surefire.PostgreSql    # optional
```

## Quick start

```csharp
using Surefire;
using Surefire.Dashboard;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSurefire();
var app = builder.Build();

app.AddJob("Hello", () => "Hello, World!");

app.MapSurefireDashboard();
app.Run();
```

Run the app and go to `/surefire` to see the dashboard.

## Registering jobs

Call `AddJob` with a name and a delegate. Parameters are resolved from DI, job arguments, or special types like `JobContext` and `CancellationToken`:

```csharp
// Simple job with parameters
app.AddJob("Add", (int a, int b) => a + b);

// Async job with DI services
app.AddJob("Import", async (ILogger<Program> logger, CancellationToken ct) =>
{
    logger.LogInformation("Starting import");
    await Task.Delay(5000, ct);
});

// Progress reporting (0.0 to 1.0)
app.AddJob("DataImport", async (JobContext ctx, CancellationToken ct) =>
{
    for (var i = 1; i <= 10; i++)
    {
        await ctx.ReportProgressAsync(i / 10.0);
        await Task.Delay(1000, ct);
    }
});
```

Default parameter values work too — if `count` isn't provided when triggering, it defaults to `100`:

```csharp
app.AddJob("Process", async (JobContext ctx, CancellationToken ct, int count = 100) =>
{
    for (var i = 0; i < count; i++)
    {
        await ctx.ReportProgressAsync((double)i / count);
        await Task.Delay(500, ct);
    }
});
```

## Configuring jobs

Chain configuration methods after `AddJob`:

```csharp
app.AddJob("Cleanup", async () => { /* ... */ })
    .WithCron("0 * * * *")              // Every hour
    .WithDescription("Hourly cleanup")
    .WithTags("maintenance")
    .WithTimeout(TimeSpan.FromMinutes(5))
    .WithMaxConcurrency(1)              // Only one at a time
    .WithPriority(5)                    // Higher = claimed first (default 0)
    .WithRetry(3);                      // Up to 3 attempts
```

For more control over retries:

```csharp
app.AddJob("Flaky", async () => { /* ... */ })
    .WithRetry(policy =>
    {
        policy.MaxAttempts = 5;
        policy.BackoffType = BackoffType.Exponential;
        policy.InitialDelay = TimeSpan.FromSeconds(2);
        policy.MaxDelay = TimeSpan.FromMinutes(5);
    });
```

## Continuous jobs

A continuous job auto-restarts whenever it completes or fails (after retries). Use cases include queue consumers, stream processors, and background pollers.

```csharp
app.AddJob("QueueConsumer", async (CancellationToken ct) =>
{
    while (!ct.IsCancellationRequested)
    {
        var msg = await queue.DequeueAsync(ct);
        await ProcessAsync(msg, ct);
    }
})
.AsContinuous();
```

By default, continuous jobs have `MaxConcurrency` of 1 — only one instance runs across the cluster. Override this to run parallel workers:

```csharp
// 3 parallel workers
app.AddJob("QueueConsumer", async (CancellationToken ct) =>
{
    while (!ct.IsCancellationRequested)
    {
        var msg = await queue.DequeueAsync(ct);
        await ProcessAsync(msg, ct);
    }
})
.AsContinuous()
.WithMaxConcurrency(3);
```

| Scenario | Behavior |
|---|---|
| Job completes successfully | Restarts immediately |
| Job fails, retries remaining | Normal retry behavior |
| Job fails, retries exhausted | Restarts after cooldown delay |
| Job cancelled by user | No restart |
| Job disabled via dashboard | No restart |
| Job re-enabled via dashboard | New run created on next heartbeat |

If both `.WithCron()` and `.AsContinuous()` are set, continuous takes precedence — cron scheduling is skipped.

## Triggering jobs

Use `IJobClient` to trigger jobs from code. You can inject it anywhere — in other jobs, controllers, background services, etc.

```csharp
// Fire and forget
await client.TriggerAsync("Cleanup");

// With arguments
await client.TriggerAsync("Add", new { a = 1, b = 2 });

// Schedule for later
await client.TriggerAsync("Report", null, new RunOptions { NotBefore = DateTimeOffset.UtcNow.AddHours(1) });

// With priority (higher = claimed first)
await client.TriggerAsync("Urgent", null, new RunOptions { Priority = 10 });

// Run and wait for the result
var sum = await client.RunAsync<int>("Add", new { a = 1, b = 2 });

// Run and wait (no result)
await client.RunAsync("Cleanup");
```

Jobs can call other jobs too:

```csharp
app.AddJob("Workflow", async (IJobClient client, CancellationToken ct) =>
{
    var sum = await client.RunAsync<int>("Add", new { a = 100, b = 200 }, ct);
    return $"The sum is {sum}";
});
```

## Streaming

Jobs can return `IAsyncEnumerable<T>` to stream results incrementally. Other jobs can accept `IAsyncEnumerable<T>` parameters to consume those streams as items are produced.

A job that yields results over time:

```csharp
app.AddJob("FetchProducts", async (CancellationToken ct) =>
{
    return Fetch();

    async IAsyncEnumerable<string> Fetch()
    {
        await foreach (var line in ReadCsvLinesAsync(ct))
        {
            yield return line;
        }
    }
});
```

A job that consumes a stream and transforms it into a new one:

```csharp
app.AddJob("ConvertProducts", async (IAsyncEnumerable<string> lines, ILogger<Program> logger, CancellationToken ct) =>
{
    return Convert();

    async IAsyncEnumerable<Product> Convert()
    {
        await foreach (var line in lines)
        {
            var parts = line.Split(',');
            var product = new Product(parts[0], parts[1], decimal.Parse(parts[2]));
            logger.LogInformation("Converted: {Product}", product);
            yield return product;
        }
    }
});
```

Chain them with `RunAsync` to build a pipeline:

```csharp
app.AddJob("ProcessProducts", async (IJobClient client, CancellationToken ct) =>
{
    var lines = await client.RunAsync<IAsyncEnumerable<string>>("FetchProducts", ct);
    var products = await client.RunAsync<IAsyncEnumerable<Product>>("ConvertProducts", new { lines }, ct);

    var count = 0;

    await foreach (var product in products)
    {
        count++;
    }

    return $"Processed {count} products";
});
```

Each stage runs as its own job with independent logging, retries, and progress tracking. Data streams between them, so `FetchProducts` doesn't need to finish before `ConvertProducts` starts processing.

## Lifecycle callbacks

You can hook into job lifecycle events — per job or globally.

`OnSuccess` fires when a job completes successfully. `OnFailure` fires on every failure. `OnRetry` fires when a retry is about to happen. `OnDeadLetter` fires when all retries are exhausted.

```csharp
app.AddJob("Order", async (int orderId) => { /* ... */ })
    .WithRetry(3)
    .OnSuccess((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogInformation("Job {Name} run {RunId} completed on attempt {Attempt}",
            ctx.JobName, ctx.RunId, ctx.Attempt);
    })
    .OnFailure((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogWarning("Attempt {Attempt} failed: {Error}",
            ctx.Attempt, ctx.Exception?.Message);
    })
    .OnDeadLetter((JobContext ctx) =>
    {
        // All retries used up
    });
```

Global callbacks apply to every job:

```csharp
builder.Services.AddSurefire(options =>
{
    options.OnFailure((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogError(ctx.Exception, "Job {Name} failed", ctx.JobName);
    });
});
```

## Configuration

Pass options to `AddSurefire` to customize behavior:

```csharp
builder.Services.AddSurefire(options =>
{
    options.NodeName = "worker-1";
    options.PollingInterval = TimeSpan.FromSeconds(5);
    options.ShutdownTimeout = TimeSpan.FromSeconds(30);
    options.RetentionPeriod = TimeSpan.FromDays(14);
});
```

| Option | Default | Description |
|---|---|---|
| `NodeName` | Machine name | Identifies this node in the cluster |
| `PollingInterval` | 1s | How often to check for pending jobs |
| `HeartbeatInterval` | 30s | How often to send heartbeats |
| `StaleNodeThreshold` | 2min | When to consider a node dead |
| `RetentionPeriod` | 7 days | How long to keep completed runs (`null` = forever) |
| `RetentionCheckInterval` | 5min | How often to check for expired runs to purge |
| `ShutdownTimeout` | 15s | Grace period for running jobs on shutdown |
| `MaxNodeConcurrency` | `null` | Max concurrent jobs on this node (`null` = unlimited) |
| `AutoMigrate` | `true` | Automatically create/update the store schema on startup |

## Health check

Surefire ships with an ASP.NET Core health check that verifies store connectivity:

```csharp
builder.Services.AddHealthChecks()
    .AddSurefire();

app.MapHealthChecks("/health");
```

## PostgreSQL

By default Surefire uses an in-memory store. For persistent, multi-node deployments, use the PostgreSQL provider:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UsePostgreSql("Host=localhost;Database=myapp");
});
```

This sets up both the job store and the notification provider (LISTEN/NOTIFY). The schema defaults to `surefire` and is created automatically when `AutoMigrate` is enabled.

## Dashboard

```csharp
app.MapSurefireDashboard();         // at /surefire
app.MapSurefireDashboard("/");      // at the root
app.MapSurefireDashboard("/admin"); // at /admin
```

The dashboard is embedded in the NuGet package — no npm or extra files needed. See the [Dashboard guide](/surefire/guides/dashboard/) for details.
