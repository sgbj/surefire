---
title: Queues
description: Route jobs to specific queues with priority, concurrency, and rate limits.
---

## Basics

By default, all jobs go into the `default` queue. Use named queues to isolate different workloads:

```csharp
builder.Services.AddSurefire(options =>
{
    options.AddQueue("emails", queue =>
    {
        queue.Priority = 5;
        queue.MaxConcurrency = 10;
    });

    options.AddQueue("reports", queue =>
    {
        queue.Priority = 1;
    });
});
```

Assign jobs to specific queues with `WithQueue`:

```csharp
app.AddJob("SendWelcome", async () => { /* ... */ })
    .WithQueue("emails");

app.AddJob("MonthlyReport", async () => { /* ... */ })
    .WithQueue("reports");
```

## Priority

Queues have a priority (default 0, higher is more important). When multiple queues have pending runs, higher-priority queues are processed first.

Jobs also have their own priority, which is used as a tiebreaker within a queue.

## Concurrency

Set `MaxConcurrency` on a queue to limit how many runs from that queue can execute at the same time across the entire cluster:

```csharp
options.AddQueue("emails", queue =>
{
    queue.MaxConcurrency = 10;
});
```

This is separate from per-job concurrency (`WithMaxConcurrency` on the job) and per-node concurrency (`MaxNodeConcurrency` in the options). All three limits apply together.

## Rate limits

Queues can reference a named rate limit:

```csharp
options.AddSlidingWindowLimiter("smtp", maxPermits: 100, window: TimeSpan.FromMinutes(1));

options.AddQueue("emails", queue =>
{
    queue.RateLimitName = "smtp";
});
```

See the [rate limiting guide](/surefire/guides/rate-limiting/) for details.

## Pausing

Queues can be paused and resumed from the dashboard or the REST API. While a queue is paused, no new runs from it start. Runs already in progress are not affected.
