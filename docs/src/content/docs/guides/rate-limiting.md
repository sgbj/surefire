---
title: Rate limiting
description: Control how fast jobs execute with fixed and sliding window rate limits.
---

## Defining rate limits

Register named rate limits in the Surefire configuration. Each rate limit has a name, a window type, and a maximum number of permits per window.

```csharp
builder.Services.AddSurefire(options =>
{
    options.AddSlidingWindowLimiter("smtp", maxPermits: 100, window: TimeSpan.FromMinutes(1));

    options.AddFixedWindowLimiter("api", maxPermits: 1000, window: TimeSpan.FromHours(1));
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
options.AddQueue("emails", queue =>
{
    queue.RateLimitName = "smtp";
});
```

When a job has a rate limit and its queue also has a rate limit, both limits apply.

Rate-limited runs stay in `Pending` status until a permit is available, so runs are delayed rather than failed or cancelled.

