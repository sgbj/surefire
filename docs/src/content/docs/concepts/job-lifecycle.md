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
| Retrying | The latest attempt failed and another attempt is scheduled |
| Cancelled | Cancelled by a user, during shutdown, or because it expired |
| Dead letter | Failed after all retry attempts were exhausted |

## Retries

When a job fails and retries are configured:

1. The run transitions to `Retrying`.
2. After the backoff delay, the run transitions back to `Pending`.
3. If retries are exhausted, the run is marked `Dead letter`.

Configure retries on a per-job basis:

```csharp
app.AddJob("Flaky", async () => { /* ... */ })
    .WithRetry(policy =>
    {
        policy.MaxRetries = 5;
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

If a non-terminal run with the same deduplication ID already exists, the trigger is rejected. Deduplication IDs are released when the existing run reaches a terminal status (`Completed`, `Cancelled`, or `Dead letter`).

Cron jobs use automatic deduplication based on the job name and scheduled time, so you don't need to worry about double-firing after a node restart.

## Run expiration

Set `NotAfter` on a run to give it a deadline. If the run has not started by that time, it will eventually be cancelled automatically.

```csharp
await client.TriggerAsync("TimelyReport", args, new RunOptions
{
    NotBefore = DateTimeOffset.UtcNow,
    NotAfter = DateTimeOffset.UtcNow.AddMinutes(30)
});
```

This is useful for time-sensitive work where a late execution is worse than no execution.

## Node health

Nodes send heartbeats every 30 seconds (configurable). If a node goes silent for longer than `InactiveThreshold` (default 2 minutes), another node recovers its runs by transitioning them through retry flow. Inactive nodes are pruned from the store.
