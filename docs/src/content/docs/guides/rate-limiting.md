---
title: Rate limiting
description: Control how fast jobs execute with fixed and sliding window rate limits.
---

## Defining rate limits

Register named rate limits in the Surefire configuration. Each rate limit has a name, a window type, and a maximum number of permits per window.

```csharp
builder.Services.AddSurefire(surefire =>
{
    surefire.AddRateLimit("smtp")
        .SlidingWindow(maxPermits: 100, TimeSpan.FromMinutes(1));

    surefire.AddRateLimit("api")
        .FixedWindow(maxPermits: 1000, TimeSpan.FromHours(1));
});
```

## Window types

**Fixed window** resets the permit count at the start of each window. If the window is 1 hour and you allow 1000 permits, the counter resets to zero every hour on the hour.

**Sliding window** spreads the limit more evenly over time. It uses a weighted count of the current and previous window to prevent bursts at window boundaries. For example, with 100 permits per minute, you can't send 100 at 0:59 and another 100 at 1:01.

## Applying rate limits

Rate limits can be applied at the job level or the queue level.

### Per job

```csharp
app.AddJob("SendEmail", async (string to, string body) => { /* ... */ })
    .WithRateLimit("smtp");
```

### Per queue

```csharp
surefire.AddQueue("emails")
    .WithRateLimit("smtp");
```

When a job has a rate limit and its queue also has a rate limit, both are checked during claim. If they reference the same named rate limit, it's only checked once to avoid double-counting.

## How it works

Rate limits are enforced at claim time, not at trigger time. When the scheduler tries to claim a pending run, it checks all applicable rate limits. If any limit has no available permits, the run is skipped and will be retried on the next polling cycle.

This means rate-limited runs stay in `Pending` status until a permit is available. They aren't failed or cancelled, just delayed.

For the in-memory store, rate limiting uses `System.Threading.RateLimiting` from the base class library. For database stores, the implementation uses approximate window-based counting in SQL to avoid distributed locking.
