---
title: Filters
description: Add cross-cutting behavior to jobs with the filter pipeline.
---

## Overview

Filters are middleware for job execution. They wrap the job handler and run before and after each execution, letting you add cross-cutting behavior like logging, metrics, authorization, or error handling.

## Writing a filter

Implement `IJobFilter`. Call `next(context)` to continue the pipeline, or skip it to short-circuit execution.

```csharp
public class LoggingFilter : IJobFilter
{
    public async Task InvokeAsync(JobContext context, JobFilterDelegate next)
    {
        var sw = Stopwatch.StartNew();
        Console.WriteLine($"Starting {context.JobName}");

        await next(context);

        Console.WriteLine($"Finished {context.JobName} in {sw.ElapsedMilliseconds}ms");
    }
}
```

## Registering filters

### Global filters

Global filters run on every job. Register them with `UseFilter<T>` in the Surefire configuration:

```csharp
builder.Services.AddSurefire(surefire =>
{
    surefire.UseFilter<LoggingFilter>();
    surefire.UseFilter<TenantFilter>();
});
```

Filters run in the order they're registered. The first filter registered is the outermost layer of the pipeline.

### Per-job filters

Per-job filters run only on a specific job. Add them with `UseFilter<T>` on the job builder:

```csharp
app.AddJob("Import", async () => { /* ... */ })
    .UseFilter<AuditFilter>();
```

Per-job filters are resolved from DI if registered, or created via `ActivatorUtilities` if not. Their constructor parameters are injected automatically.

## Pipeline order

The execution pipeline looks like this:

```
Global filter 1
  Global filter 2
    Per-job filter 1
      Per-job filter 2
        Job handler
```

Global filters wrap everything, per-job filters wrap only the job handler. Within each group, filters run in registration order.

## Examples

### Error notification filter

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

### Timing filter

```csharp
public class TimingFilter(ILogger<TimingFilter> logger) : IJobFilter
{
    public async Task InvokeAsync(JobContext context, JobFilterDelegate next)
    {
        var sw = Stopwatch.StartNew();

        await next(context);

        logger.LogInformation("Job {Name} took {Ms}ms",
            context.JobName, sw.ElapsedMilliseconds);
    }
}
```
