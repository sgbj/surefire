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

Pending runs are cancelled immediately. Running runs first move into `Cancelling`, receive a cancellation signal via `CancellationToken`, and become `Cancelled` once the attempt has finished flushing its final observable events. If a running job doesn't check its token, it will continue until it finishes or the process shuts down.

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

Timeout is implemented via cancellation of the running attempt. In run history this appears as cancellation for that attempt.

## Shutdown

When the application shuts down, Surefire interrupts in-flight running attempts and waits up to `ShutdownTimeout` (default 15 seconds) for shutdown work.

```csharp
builder.Services.AddSurefire(options =>
{
    options.ShutdownTimeout = TimeSpan.FromSeconds(30);
});
```

Shutdown interruptions are recorded as run failures with an explicit shutdown error and then follow normal retry policy. If retries remain, the run is scheduled again; otherwise it moves to dead-letter.

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

