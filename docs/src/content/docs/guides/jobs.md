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

1. **Special types** — `JobContext` and `CancellationToken` are injected directly.
2. **DI services** — `IJobClient`, `ILogger<T>`, and any registered services are resolved from the scoped service provider.
3. **Job arguments** are deserialized from the JSON arguments passed when the job was triggered.

If a parameter has a default value and isn't provided in the arguments, the default is used.

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
| `WithTimeout(duration)` | Cancel the job if it runs longer than this |
| `WithMaxConcurrency(n)` | Max simultaneous runs of this job across the cluster |
| `WithPriority(n)` | Higher priority runs are claimed first (default 0) |
| `WithQueue(name)` | Assign to a named queue |
| `WithRateLimit(name)` | Apply a named rate limit |
| `WithRetry(n)` | Max number of retries (`n + 1` total attempts) |
| `WithRetry(configure)` | Fine-grained retry policy (backoff, delays, jitter) |
| `WithMisfirePolicy(policy, fireAllLimit?)` | What to do when scheduled fires are missed; for `FireAll`, optionally cap missed occurrences |
| `Continuous()` | Auto-restart when the job completes, fails, or is cancelled |

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

- `OnSuccess` fires when a job completes successfully.
- `OnRetry` fires when Surefire schedules another attempt after a failure.
- `OnDeadLetter` fires when all retries are exhausted.

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

## Batch and run APIs

Use batch APIs when you want to fan out a job over many inputs:

```csharp
var batch = await client.TriggerBatchAsync("RenderInvoice", [
    new { orderId = 101 },
    new { orderId = 102 },
    new { orderId = 103 }
]);

await foreach (var result in client.WaitEachAsync(batch.Id))
{
    Console.WriteLine($"{result.RunId}: {result.Status}");
}
```

- `TriggerBatchAsync` creates a batch and its child runs.
- `RunBatchAsync<T>` returns all typed results (throws aggregate failure if any child fails).
- `WaitEachAsync` streams child completion results one-by-one.
- `StreamEachAsync<T>` streams per-child output events.

If you need to resume a batch-wide event stream after reconnecting, use the batch event cursor overload:

```csharp
var cursor = new BatchRunEventCursor
{
    SinceEventId = lastSeenBatchEventId
};

await foreach (var @event in client.StreamBatchEventsAsync(batch.Id, cursor))
{
    Console.WriteLine($"{@event.RunId}: {@event.EventType}");
}
```

You can also interact with a single run after trigger:

```csharp
var runId = await client.TriggerAsync("Import", new { source = "s3://..." });

// Wait for terminal state
var runResult = await client.WaitAsync(runId);

// Stream typed outputs if the job emits output events
await foreach (var item in client.WaitStreamAsync<string>(runId))
{
    Console.WriteLine(item);
}

// Observe run + events together
await foreach (var observation in client.ObserveAsync(runId))
{
    Console.WriteLine($"{observation.Run.Status} / {observation.Event?.EventType}");
}

// Rerun the same arguments/input events
var rerunId = await client.RerunAsync(runId);
```

