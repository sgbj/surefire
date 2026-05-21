---
title: Job lifecycle
description: Run statuses, retries, and reruns.
---

## Statuses

A run moves through a small set of statuses:

| Status | Meaning |
|---|---|
| Pending | Waiting for a worker to pick it up |
| Running | Currently executing on a worker |
| Suspended | A durable orchestrator is waiting for child work to finish |
| Succeeded | Finished successfully |
| Canceled | Stopped by a user, shutdown, or expiration |
| Failed | Failed after retries were exhausted |

`Succeeded`, `Canceled`, and `Failed` are terminal. Once a run reaches one of those statuses, Surefire will not execute that run again. If you want to run the same job again, start a rerun. A rerun creates a new run.

`Suspended` is used by durable orchestrators. It means the orchestrator is waiting for child runs or batches to finish. A suspended run is not picked up by workers and does not use a concurrency slot. When the work it is waiting on finishes, Surefire moves the orchestrator back to `Pending` so it can continue.

`Attempt` records the execution attempt that produced the current status. It starts at 1. It only changes when a failed attempt is scheduled to retry.

Durable orchestrator replays are tracked separately from retries. `ReplayCount` counts durable replays. `FailureCount` counts failed execution attempts.

## Retries

When an attempt fails and retries remain, the run goes back to `Pending`. Surefire sets `NotBefore` to the next backoff time, increments `FailureCount`, and advances `Attempt` for the next execution. A worker can pick it up again after the delay.

When retries are exhausted, the run moves from `Running` to `Failed`. `FailureCount` is incremented, but `Attempt` is not advanced because there will not be another automatic attempt.

Configure retries per job:

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

By default, retries use a fixed 5 second backoff with jitter enabled.

## Reruns

Retries are automatic and continue the same run. Reruns are started by a user from the dashboard or with `IJobClient.RerunAsync`.

A rerun creates a new run with the same job name, arguments, and input events. The new run's `RerunOfRunId` points back to the original run so the dashboard can show the relationship.
