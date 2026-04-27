---
title: Scheduling
description: Cron schedules, continuous jobs, and misfire handling.
---

## Cron jobs

Use `WithCron` to run a job on a schedule:

```csharp
app.AddJob("DailyReport", async () => { /* ... */ })
    .WithCron("0 9 * * *", "America/Chicago"); // 9am Central every day
```

The second argument is an optional timezone ID. Without it, the schedule runs in UTC. Schedules respect the zone's daylight saving transitions, so `0 9 * * *` with `America/Chicago` always fires at 9:00 AM Central even when the equivalent UTC time shifts.

Each scheduled fire time produces at most one run, even with multiple nodes scheduling in parallel.

Some common expressions:

| Expression | Schedule |
|---|---|
| `* * * * *` | Every minute |
| `0 * * * *` | Every hour |
| `0 9 * * *` | Daily at 9am |
| `0 9 * * 1-5` | Weekdays at 9am |
| `0 0 1 * *` | First of every month at midnight |

## Misfire policy

If no node is available when a cron job is supposed to fire, the missed occurrences are called misfires. The misfire policy controls what happens when scheduling resumes:

```csharp
app.AddJob("Cleanup", async () => { /* ... */ })
    .WithCron("0 * * * *")
    .WithMisfirePolicy(MisfirePolicy.FireOnce);
```

| Policy | Behavior | When to use |
|---|---|---|
| `Skip` (default) | Ignore missed fires and resume from the next future occurrence. | Most jobs, where catch-up runs would pile up after an outage. |
| `FireOnce` | Fire once to catch up, then resume the normal schedule. | Cumulative work that should run at least once after a gap. |
| `FireAll` | Fire every missed occurrence, optionally capped with `fireAllLimit`. | Workloads where every scheduled execution matters and skipping any would cause data loss. |

To bound the catch-up after long outages, pass `fireAllLimit`. Surefire keeps the most recent N misses and skips the rest, so a 24-hour outage on an hourly job with `fireAllLimit: 10` fires the last 10 hours and resumes from now:

```csharp
app.AddJob("Cleanup", async () => { /* ... */ })
    .WithCron("0 * * * *")
    .WithMisfirePolicy(MisfirePolicy.FireAll, fireAllLimit: 50);
```

## Continuous jobs

A continuous job restarts after each run, regardless of whether it succeeded, failed, or was cancelled. Useful for queue consumers, stream processors, and background pollers that should always be running.

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

Continuous jobs default to `MaxConcurrency` of 1, meaning only one instance runs across the cluster. Override it to run parallel workers:

```csharp
app.AddJob("QueueConsumer", async (CancellationToken ct) => { /* ... */ })
    .Continuous()
    .WithMaxConcurrency(3);
```

Surefire seeds enabled continuous jobs up to their configured `MaxConcurrency` on startup.

| Scenario | Behavior |
|---|---|
| Job completes | Restarts |
| Job fails, retries remaining | Normal retry behavior |
| Job fails, retries exhausted | Restarts after a cooldown delay |
| Job cancelled | Restarts (unless job is disabled) |
| Job disabled via dashboard | No restart |
| Job re-enabled via dashboard | Restarts |
