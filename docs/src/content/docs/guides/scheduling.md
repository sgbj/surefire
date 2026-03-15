---
title: Scheduling
description: Cron schedules, continuous jobs, and misfire handling.
---

## Cron jobs

Use `WithCron` to run a job on a schedule. The expression follows standard cron syntax (5 fields: minute, hour, day-of-month, month, day-of-week):

```csharp
app.AddJob("DailyReport", async () => { /* ... */ })
    .WithCron("0 9 * * *", "America/New_York"); // 9am ET every day
```

The second argument is an optional timezone ID. Without it, the schedule runs in UTC.

Some common expressions:

| Expression | Schedule |
|---|---|
| `* * * * *` | Every minute |
| `0 * * * *` | Every hour |
| `0 9 * * *` | Daily at 9am |
| `0 9 * * 1-5` | Weekdays at 9am |
| `0 0 1 * *` | First of every month at midnight |

Each cron occurrence is automatically deduplicated, so if multiple nodes are running, the job fires exactly once per scheduled time.

## Misfire policy

If a node is down when a cron job is supposed to fire, the missed occurrences are called misfires. The misfire policy controls what happens when the node comes back up:

```csharp
app.AddJob("Cleanup", async () => { /* ... */ })
    .WithCron("0 * * * *")
    .WithMisfirePolicy(MisfirePolicy.FireOnce);
```

| Policy | Behavior |
|---|---|
| `Skip` | Ignore all missed fires and resume from the next future occurrence. This is the default. |
| `FireOnce` | Fire once to catch up, then resume the normal schedule. |
| `FireAll` | Fire every missed occurrence (capped at 10 to prevent flooding). |

`Skip` is the right choice for most jobs. If your server was down for 3 hours and you have a minutely job, you probably don't want 180 catch-up runs.

`FireOnce` is useful when the job does cumulative work (like processing everything since the last run) and you need it to run at least once after a gap.

`FireAll` is for cases where every single scheduled execution matters and skipping any would cause data loss.

## Continuous jobs

A continuous job auto-restarts whenever it completes or fails (after retries). This is useful for queue consumers, stream processors, and background pollers.

```csharp
app.AddJob("QueueConsumer", async (CancellationToken ct) =>
{
    while (!ct.IsCancellationRequested)
    {
        var msg = await queue.DequeueAsync(ct);
        await ProcessAsync(msg, ct);
    }
})
.Continuous();
```

By default, continuous jobs have `MaxConcurrency` of 1, meaning only one instance runs across the cluster. Override this to run parallel workers:

```csharp
app.AddJob("QueueConsumer", async (CancellationToken ct) =>
{
    while (!ct.IsCancellationRequested)
    {
        var msg = await queue.DequeueAsync(ct);
        await ProcessAsync(msg, ct);
    }
})
.Continuous()
.WithMaxConcurrency(3);
```

| Scenario | Behavior |
|---|---|
| Job completes | Restarts immediately |
| Job fails, retries remaining | Normal retry behavior |
| Job fails, retries exhausted | Restarts after a cooldown delay |
| Job cancelled by user | No restart |
| Job disabled via dashboard | No restart |
| Job re-enabled via dashboard | New run created on next heartbeat |

If both `WithCron()` and `Continuous()` are set, continuous takes precedence and cron scheduling is skipped.
