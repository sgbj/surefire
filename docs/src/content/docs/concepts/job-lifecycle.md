---
title: Job lifecycle
description: Run statuses, retries, and reruns.
---

## Statuses

A run has one of five statuses:

| Status | Meaning |
|---|---|
| Pending | Waiting to be claimed by a node |
| Running | Currently executing |
| Succeeded | Finished successfully |
| Canceled | Canceled by a user, during shutdown, or because it expired |
| Failed | Failed after all retry attempts were exhausted |

Succeeded, Canceled, and Failed are terminal. The `Attempt` field on the run records which attempt produced the terminal status.

## Retries

When an attempt fails and retries remain, the run transitions back to `Pending` with `NotBefore` set to the next backoff time and `Attempt` incremented. The next claim picks it up after the delay. When retries are exhausted, the run transitions from `Running` directly to `Failed`.

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

The default backoff is fixed at 5 seconds with jitter enabled.

## Reruns

Retries are automatic and continue the same run. Reruns are manual (from the dashboard or `IJobClient.RerunAsync`) and create a new run with the same job name, arguments, and input events. The new run's `RerunOfRunId` points back to the original so the dashboard can show the connection.
