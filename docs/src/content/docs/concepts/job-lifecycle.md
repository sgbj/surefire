---
title: Job lifecycle
description: Statuses, retries, dead lettering, reruns, and deduplication.
---

## Statuses

Every run goes through a series of statuses:

| Status | Meaning |
|---|---|
| Pending | Waiting to be picked up by a node |
| Running | Currently executing |
| Completed | Finished successfully |
| Failed | Something went wrong (may be retried) |
| Cancelled | Cancelled by a user, during shutdown, or because it expired |
| Dead letter | Failed after all retry attempts were exhausted |
| Skipped | Intentionally not run (used by plans for conditional branching) |

## Retries

When a job fails and has `MaxAttempts > 1`:

1. The current run is marked `Failed`.
2. A new run is created with the next attempt number and a delay based on the backoff policy.
3. The retry links back to the original via `RetryOfRunId`, so you can follow the chain in the dashboard.
4. If all attempts are used up, the final run is marked `Dead letter` instead of `Failed`.

Configure retries on a per-job basis:

```csharp
app.AddJob("Flaky", async () => { /* ... */ })
    .WithRetry(policy =>
    {
        policy.MaxAttempts = 5;
        policy.BackoffType = BackoffType.Exponential;
        policy.InitialDelay = TimeSpan.FromSeconds(2);
        policy.MaxDelay = TimeSpan.FromMinutes(5);
        policy.Jitter = true; // adds randomness to prevent thundering herd
    });
```

The default backoff is fixed at 5 seconds with jitter enabled.

## Reruns

Reruns are different from retries. A retry is automatic and part of the same logical execution chain. A rerun is a manual action, typically triggered from the dashboard, that creates a completely independent run with the same job name and arguments.

Reruns don't inherit the parent's trace tree. They're linked via `RerunOfRunId` for reference, but they're otherwise standalone.

## Deduplication

You can prevent duplicate runs by setting a deduplication ID:

```csharp
await client.TriggerAsync("Import", args, new RunOptions
{
    DeduplicationId = $"import-{date:yyyy-MM-dd}"
});
```

If a run with the same deduplication ID already exists, the trigger will throw. Deduplication IDs are freed when runs are purged by the retention policy.

Cron jobs use automatic deduplication based on the job name and scheduled time, so you don't need to worry about double-firing after a node restart.

## Run expiration

Set `NotAfter` on a run to give it a deadline. If the run hasn't been picked up by that time, it won't be claimed and will eventually be cancelled automatically.

```csharp
await client.TriggerAsync("TimelyReport", args, new RunOptions
{
    NotBefore = DateTimeOffset.UtcNow,
    NotAfter = DateTimeOffset.UtcNow.AddMinutes(30)
});
```

This is useful for time-sensitive work where a late execution is worse than no execution.

## Node health

Nodes send heartbeats on an interval (default 30 seconds). If a node goes silent for longer than the `InactiveThreshold` (default 2 minutes), another node will recover its runs:

- Runs that were still executing on the inactive node get marked as `Failed` and retried if retries are configured.
- The recovery uses compare-and-swap to prevent races. If the original node comes back and tries to complete a recovered run, the update is rejected.

Inactive nodes are kept in the store but filtered from the dashboard. They're purged after the retention period expires.
