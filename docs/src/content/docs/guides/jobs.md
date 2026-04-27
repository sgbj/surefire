---
title: Jobs
description: Register jobs, configure retries, timeouts, and lifecycle callbacks.
---

## Registering jobs

Register a job with `AddJob`, and pass a name and a delegate. The delegate's parameters can be `JobContext`, `CancellationToken`, services from DI, or arguments passed when triggering the run:

```csharp
// Simple job with parameters
app.AddJob("Add", (int a, int b) => a + b);

// Async job with DI services
app.AddJob("Import", async (ILogger<Program> logger, CancellationToken ct) =>
{
    logger.LogInformation("Starting import");
    await Task.Delay(5000, ct);
});
```

Use `JobContext.ReportProgressAsync` to report progress between 0.0 and 1.0:

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

Default parameter values are used when arguments aren't passed, so `count` falls back to `100`.

## Configuration

Chain configuration methods after `AddJob`:

```csharp
app.AddJob("Cleanup", async () => { /* ... */ })
    .WithCron("0 * * * *", "America/Chicago")
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
| `WithTimeout(duration)` | Fail the job if it runs longer than this |
| `WithMaxConcurrency(n)` | Max simultaneous runs of this job across the cluster |
| `WithPriority(n)` | Higher priority runs are claimed first (default 0) |
| `WithQueue(name)` | Assign to a named queue |
| `WithRateLimit(name)` | Apply a named rate limit |
| `WithRetry(n)` | Max number of retries (`n + 1` total attempts) |
| `WithRetry(configure)` | Fine-grained retry policy (backoff, delays, jitter) |
| `WithMisfirePolicy(policy, fireAllLimit?)` | Behavior for missed cron fires (with optional cap when policy is `FireAll`) |
| `Continuous()` | Restart after each run regardless of outcome |
| `OnSuccess(callback)` | Fires after a successful run |
| `OnRetry(callback)` | Fires after a failed attempt when a retry is scheduled |
| `OnDeadLetter(callback)` | Fires after retries are exhausted |
| `UseFilter<T>()` | Wrap execution with a filter (see [Filters](/surefire/guides/filters/)) |

## Lifecycle callbacks

Hook into job lifecycle events per job or globally.

```csharp
app.AddJob("Order", async (int orderId) => { /* ... */ })
    .WithRetry(3)
    .OnSuccess((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogInformation("Completed on attempt {Attempt}", ctx.Attempt);
    })
    .OnRetry((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogWarning("Retrying attempt {Attempt} for {JobName}", ctx.Attempt, ctx.JobName);
    })
    .OnDeadLetter((JobContext ctx) =>
    {
        // All retries exhausted
    });
```

Global callbacks apply to every job:

```csharp
builder.Services.AddSurefire(options =>
{
    options.OnDeadLetter((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogError(ctx.Exception, "Job {Name} reached dead letter", ctx.JobName);
    });
});
```

See [Filters](/surefire/guides/filters/) for cross-cutting behavior.

See [Triggering and running](/surefire/guides/triggering/) for how to invoke jobs from code, including running batches and observing existing runs.
