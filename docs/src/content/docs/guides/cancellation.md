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

Cancellation transitions the run directly to `Cancelled`. The running attempt receives a signal through its `CancellationToken` and stops at the next opportunity. If a running job never checks its token, it keeps executing until it finishes on its own or the process shuts down, even though the run is already marked `Cancelled`.

## Cascade cancellation

When you cancel a run, all of its child runs (triggered via `IJobClient` from within the job) are also cancelled. This walks the entire trace tree, so grandchildren and deeper descendants are cancelled too.

```csharp
app.AddJob("Parent", async (IJobClient client, CancellationToken ct) =>
{
    // If Parent is cancelled, this child is also cancelled.
    await client.RunAsync("Child", cancellationToken: ct);
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

A timeout cancels the running attempt's `CancellationToken` and records the attempt as failed (with a `TimeoutException`), not as cancelled. Normal retry policy applies, so a timeout that has retries remaining produces a fresh attempt.

## Shutdown

When the host shuts down, in-flight runs are cancelled and have up to `ShutdownTimeout` (default 15 seconds) to finish. Anything still running past the timeout is recorded as a failure and follows normal retry policy.

```csharp
builder.Services.AddSurefire(options =>
{
    options.ShutdownTimeout = TimeSpan.FromSeconds(30);
});
```

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

