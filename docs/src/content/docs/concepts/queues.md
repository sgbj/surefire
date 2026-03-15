---
title: Queues
description: Route jobs to specific queues with priority, concurrency, and rate limits.
---

## Basics

By default, all jobs go into a queue called `default`. You can create named queues to separate different types of work:

```csharp
builder.Services.AddSurefire(surefire =>
{
    surefire.AddQueue("emails").WithPriority(5).WithMaxConcurrency(10);
    surefire.AddQueue("reports").WithPriority(1);
});
```

Assign jobs to queues with `WithQueue`:

```csharp
app.AddJob("SendWelcome", async () => { /* ... */ })
    .WithQueue("emails");

app.AddJob("MonthlyReport", async () => { /* ... */ })
    .WithQueue("reports");
```

Jobs without an explicit queue go to `default`.

## Which queues a node processes

A node only processes queues that it explicitly registers. If you don't call `AddQueue` at all, the node processes only the `default` queue.

```csharp
// This node processes only "emails", NOT "default"
surefire.AddQueue("emails");

// This node processes both "default" and "emails"
surefire.AddQueue("default");
surefire.AddQueue("emails");
```

This lets you dedicate nodes to specific workloads. Run one set of nodes for high-priority email sending and another for heavy report generation.

## Priority

Queues have a priority (default 0, higher is more important). When multiple queues have pending runs, the scheduler claims from higher-priority queues first.

Jobs also have their own priority, which is used as a tiebreaker within a queue.

## Concurrency

Set `WithMaxConcurrency` on a queue to limit how many runs from that queue can execute at the same time across the entire cluster:

```csharp
surefire.AddQueue("emails").WithMaxConcurrency(10);
```

This is separate from per-job concurrency (`WithMaxConcurrency` on the job) and per-node concurrency (`MaxNodeConcurrency` in the options). All three are enforced during claim.

## Rate limits

Queues can reference a named rate limit:

```csharp
surefire.AddRateLimit("smtp")
    .SlidingWindow(maxPermits: 100, TimeSpan.FromMinutes(1));

surefire.AddQueue("emails")
    .WithRateLimit("smtp");
```

See the [rate limiting guide](/surefire/guides/rate-limiting/) for details.

## Pausing

Queues can be paused and resumed from the dashboard or the API. When a queue is paused, no runs from that queue will be claimed until it's resumed. Runs already in progress are not affected.
