---
title: Jobs
description: Register jobs, configure retries, timeouts, and lifecycle callbacks.
---

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

Default parameter values work too. If `count` isn't provided when triggering, it defaults to `100`:

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

## Parameter resolution

Parameters are resolved in this order:

1. **Special types** like `JobContext`, `CancellationToken`, `IJobClient`, and `IJobStore` are injected directly.
2. **DI services** are resolved from the scoped service provider.
3. **Job arguments** are deserialized from the JSON arguments passed when the job was triggered.

If a parameter has a default value and isn't provided in the arguments, the default is used.

## Configuration

Chain configuration methods after `AddJob`:

```csharp
app.AddJob("Cleanup", async () => { /* ... */ })
    .WithCron("0 * * * *", "America/New_York")
    .WithDescription("Hourly cleanup")
    .WithTags("maintenance")
    .WithTimeout(TimeSpan.FromMinutes(5))
    .WithMaxConcurrency(1)
    .WithPriority(5)
    .WithQueue("maintenance")
    .WithRateLimit("api-calls")
    .WithRetry(3);
```

| Method | Description |
|---|---|
| `WithCron(expr, tz?)` | Run on a cron schedule, optionally in a specific timezone |
| `WithDescription(text)` | Description shown in the dashboard |
| `WithTags(tags)` | Tags for filtering in the dashboard |
| `WithTimeout(duration)` | Cancel the job if it runs longer than this |
| `WithMaxConcurrency(n)` | Max simultaneous runs of this job across the cluster |
| `WithPriority(n)` | Higher priority runs are claimed first (default 0) |
| `WithQueue(name)` | Assign to a named queue |
| `WithRateLimit(name)` | Apply a named rate limit |
| `WithRetry(n)` | Max number of attempts |
| `WithRetry(configure)` | Fine-grained retry policy (backoff, delays, jitter) |
| `WithMisfirePolicy(policy)` | What to do when scheduled fires are missed |
| `Continuous()` | Auto-restart when the job completes or fails |
| `Internal()` | Hide from the dashboard job list |

## Lifecycle callbacks

Hook into job lifecycle events per job or globally.

```csharp
app.AddJob("Order", async (int orderId) => { /* ... */ })
    .WithRetry(3)
    .OnSuccess((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogInformation("Completed on attempt {Attempt}", ctx.Attempt);
    })
    .OnFailure((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogWarning("Attempt {Attempt} failed: {Error}", ctx.Attempt, ctx.Exception?.Message);
    })
    .OnDeadLetter((JobContext ctx) =>
    {
        // All retries exhausted
    });
```

- `OnSuccess` fires when a job completes successfully.
- `OnFailure` fires on every failure (before retry decision).
- `OnRetry` fires when a retry is about to be scheduled.
- `OnDeadLetter` fires when all retries are exhausted.

Global callbacks apply to every job:

```csharp
builder.Services.AddSurefire(surefire =>
{
    surefire.OnFailure((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogError(ctx.Exception, "Job {Name} failed", ctx.JobName);
    });
});
```
