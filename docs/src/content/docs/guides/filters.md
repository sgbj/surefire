---
title: Filters
description: Add cross-cutting behavior to jobs with the filter pipeline.
---

Filters are middleware for job execution. They wrap each handler invocation, so they're a good place for cross-cutting behavior: logging, metrics, authorization, error reporting.

## Writing a filter

Implement `IJobFilter`. Call `next(context)` to continue down the pipeline, or skip it to short-circuit.

```csharp
public class TimingFilter(ILogger<TimingFilter> logger) : IJobFilter
{
    public async Task InvokeAsync(JobContext context, JobFilterDelegate next)
    {
        var startedAt = Stopwatch.GetTimestamp();

        await next(context);

        logger.LogInformation("Job {Name} took {Elapsed}",
            context.JobName, Stopwatch.GetElapsedTime(startedAt));
    }
}
```

## Registering filters

### Global filters

Global filters run on every job. Register them with `UseFilter<T>` in the Surefire configuration:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseFilter<TimingFilter>();
});
```

Filters run in the order they're registered. The first filter registered is the outermost layer of the pipeline.

### Per-job filters

Per-job filters run only on a specific job. Add them with `UseFilter<T>` on the job builder:

```csharp
app.AddJob("CriticalImport", async () => { /* ... */ })
    .UseFilter<SlackNotificationFilter>();
```

Per-job filters are resolved from DI if registered, or created via `ActivatorUtilities` if not. Their constructor parameters are injected automatically. Global filters always wrap per-job filters, so a per-job filter runs inside any globals.

A common pattern is forwarding job failures somewhere observable. Here's a filter that posts a Slack message on failure and rethrows so the run still records the failure normally:

```csharp
public class SlackNotificationFilter(SlackClient slack) : IJobFilter
{
    public async Task InvokeAsync(JobContext context, JobFilterDelegate next)
    {
        try
        {
            await next(context);
        }
        catch (Exception ex)
        {
            await slack.PostAsync($"Job {context.JobName} failed: {ex.Message}");
            throw;
        }
    }
}
```

See [Lifecycle callbacks](/surefire/guides/jobs/#lifecycle-callbacks) for simpler hooks at specific events.
