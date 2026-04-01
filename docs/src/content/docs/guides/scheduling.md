---
title: Scheduling
description: Cron schedules, continuous jobs, and misfire handling.
---

## Cron jobs

Use `WithCron` to run a job on a schedule. Surefire accepts 5-field or 6-field cron expressions:

```csharp
app.AddJob("DailyReport", async () => { /* ... */ })
    .WithCron("0 9 * * *", "America/Chicago"); // 9am Central every day
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

Each cron occurrence is deduplicated so overlapping scheduler activity does not create duplicate runs for the same fire time.

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
| `FireAll` | Fire missed occurrences, optionally bounded per scheduler tick with `fireAllLimit`. |

`Skip` is the right choice for most jobs. If your server was down for 3 hours and you have a minutely job, you probably don't want 180 catch-up runs.

`FireOnce` is useful when the job does cumulative work (like processing everything since the last run) and you need it to run at least once after a gap.

`FireAll` is for cases where every single scheduled execution matters and skipping any would cause data loss.

By default, `FireAll` is unbounded for the current scheduler tick:

```csharp
app.AddJob("Cleanup", async () => { /* ... */ })
    .WithCron("0 * * * *")
    .WithMisfirePolicy(MisfirePolicy.FireAll);
```

If you want to pace backlog replay after outages, set `fireAllLimit` to cap how many missed fires are enqueued per tick. Remaining missed fires are replayed on subsequent ticks:

```csharp
app.AddJob("Cleanup", async () => { /* ... */ })
    .WithCron("0 * * * *")
    .WithMisfirePolicy(MisfirePolicy.FireAll, fireAllLimit: 50);
```

## Continuous jobs

A continuous job auto-restarts whenever it completes, fails, or is cancelled. This is useful for queue consumers, stream processors, and background pollers.

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

Continuous successor creation is idempotent, so duplicate completion processing does not create duplicate successors.

On startup, Surefire seeds enabled continuous jobs up to their configured `MaxConcurrency` (default 1).

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
| Job cancelled by user | Restarts (unless job is disabled) |
| Job disabled via dashboard | No restart |
| Job re-enabled via dashboard | New run created on next heartbeat |

If both `WithCron()` and `Continuous()` are set, continuous takes precedence and cron scheduling is skipped.
