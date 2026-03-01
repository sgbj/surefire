---
title: Getting started
description: Get up and running with Surefire in minutes.
---

## Installation

```bash
dotnet add package Surefire
dotnet add package Surefire.Dashboard
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
app.AddJob("SlowJob", async (JobContext ctx, CancellationToken ct) =>
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

## Triggering jobs

Use `IJobClient` to trigger jobs from code. You can inject it anywhere — in other jobs, controllers, background services, etc.

```csharp
// Fire and forget
await client.TriggerAsync("Cleanup");

// With arguments
await client.TriggerAsync("Add", new { a = 1, b = 2 });

// Schedule for later
await client.TriggerAsync("Report", notBefore: DateTimeOffset.UtcNow.AddHours(1));

// Run and wait for the result
var sum = await client.RunAsync<int>("Add", new { a = 1, b = 2 });
```

Jobs can call other jobs too:

```csharp
app.AddJob("Workflow", async (IJobClient client, CancellationToken ct) =>
{
    var sum = await client.RunAsync<int>("Add", new { a = 100, b = 200 }, ct);
    return $"The sum is {sum}";
});
```

## Lifecycle callbacks

You can hook into job lifecycle events — per job or globally.

`OnFailure` fires on every failure. `OnRetry` fires when a retry is about to happen. `OnDeadLetter` fires when all retries are exhausted.

```csharp
app.AddJob("Order", async (int orderId) => { /* ... */ })
    .WithRetry(3)
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
    options.PollingInterval = TimeSpan.FromSeconds(1);
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
| `ShutdownTimeout` | 15s | Grace period for running jobs on shutdown |

## Dashboard

```csharp
app.MapSurefireDashboard();         // at /surefire
app.MapSurefireDashboard("/");      // at the root
app.MapSurefireDashboard("/admin"); // at /admin
```

The dashboard is embedded in the NuGet package — no npm or extra files needed. See the [Dashboard guide](/guides/dashboard/) for details.
