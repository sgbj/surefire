---
title: Cancellation
description: Cancel running jobs, propagate cancellation to child runs, and handle timeouts.
---

## Cancelling a run

Cancel a run from code using `IJobClient`:

```csharp
await client.CancelAsync(runId);
```

You can also cancel runs from the dashboard by clicking the cancel button on a running or pending job.

### What happens when you cancel

The behavior depends on the run's current status:

- **Pending** runs are immediately transitioned to `Cancelled`.
- **Running** runs receive a cancellation signal. The job's `CancellationToken` is triggered, giving the job a chance to stop gracefully.

If a running job doesn't respond to the cancellation token, it will continue until it finishes or the process shuts down. Always check `CancellationToken` in long-running jobs.

## Cascade cancellation

When you cancel a run, all of its child runs (triggered via `IJobClient` from within the job) are also cancelled. This walks the entire trace tree, so grandchildren and deeper descendants are cancelled too.

```csharp
app.AddJob("Parent", async (IJobClient client, CancellationToken ct) =>
{
    // If Parent is cancelled, this child is also cancelled
    await client.RunAsync("Child", ct);
});
```

## Timeouts

Set a timeout on a job to automatically cancel it if it runs too long:

```csharp
app.AddJob("Import", async (CancellationToken ct) =>
{
    await LongRunningWork(ct);
})
.WithTimeout(TimeSpan.FromMinutes(5));
```

When a job times out, it's treated as a failure (not a cancellation). If retries are configured, the job will be retried. The error message will indicate that the job timed out.

## Shutdown

When the application shuts down, Surefire cancels all running jobs and waits up to `ShutdownTimeout` (default 15 seconds) for them to finish. Jobs that don't finish in time are abandoned and will be recovered by another node.

```csharp
builder.Services.AddSurefire(surefire =>
{
    surefire.ShutdownTimeout = TimeSpan.FromSeconds(30);
});
```

Shutdown cancellation is treated as a failure, so jobs with retries configured will be retried on another node.

## Run expiration

Use `NotAfter` to set a deadline for a pending run. If the run hasn't been claimed by that time, it won't be picked up and will be cancelled automatically.

```csharp
await client.TriggerAsync("TimelyReport", args, new RunOptions
{
    NotAfter = DateTimeOffset.UtcNow.AddMinutes(30)
});
```

This is useful for time-sensitive work where a late execution is worse than no execution. Expired runs are cleaned up periodically by the scheduler.

## Handling cancellation in jobs

The `CancellationToken` passed to your job delegate is linked to both user cancellation and application shutdown. Use it for any async operations:

```csharp
app.AddJob("Export", async (CancellationToken ct) =>
{
    var data = await db.QueryAsync(ct);

    foreach (var batch in data.Chunk(100))
    {
        ct.ThrowIfCancellationRequested();
        await ProcessBatchAsync(batch, ct);
    }
});
```
