---
title: Redis
description: Use Redis as the Surefire job store with real-time notifications.
---

## Installation

```bash
dotnet add package Surefire.Redis
```

## Setup

Register an `IConnectionMultiplexer` in DI (for example with `builder.AddRedisClient("surefire")` from Aspire, or `builder.Services.AddSingleton<IConnectionMultiplexer>(...)`), then:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseRedis();
});
```

This registers both the job store and the notification provider. Notifications use Redis Pub/Sub for real-time event delivery.

## Using a specific connection

Pass a factory to resolve a keyed or named multiplexer:

```csharp
options.UseRedis(sp => sp.GetRequiredKeyedService<IConnectionMultiplexer>("surefire"));
```

## When to use Redis

Redis is a good fit for:

- High-throughput workloads where claim latency matters
- High-throughput workloads where low scheduling latency matters
- Multi-node deployments that need real-time notifications without database-specific features (like PostgreSQL's LISTEN/NOTIFY)
- Teams already running Redis infrastructure

The Redis provider supports all Surefire features.

## Notification behavior

Redis Pub/Sub is the low-latency wakeup path, not the source of truth. The durable source of truth is still the Redis job store.

- **Normal case** — workers pick up new work quickly through Pub/Sub notifications.
- **Redis notification outage or disconnect** — wakeups can be delayed until the next polling interval, but workers still recover from the durable store.
- **Health checks** — Surefire degrades health when the notification transport is unhealthy, even if the store is still reachable.

If low wakeup latency matters during transient Redis outages, tune `PollingInterval` accordingly.

