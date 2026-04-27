---
title: Redis
description: Use Redis as the Surefire job store with real-time notifications.
---

## Installation

```bash
dotnet add package Surefire.Redis
```

## Setup

The Redis provider resolves an `IConnectionMultiplexer` from DI. Register one with `StackExchange.Redis` or the Aspire client extension, then call `UseRedis`:

```csharp
builder.Services.AddSurefire(options => options.UseRedis());
```

That call wires up both the store and notifications. Notifications use Redis Pub/Sub, so workers across the cluster wake up immediately when new runs are enqueued.

To resolve a keyed or named multiplexer, pass a factory:

```csharp
options.UseRedis(sp => sp.GetRequiredKeyedService<IConnectionMultiplexer>("surefire"));
```

## When to use Redis

Redis fits well for:

- High-throughput workloads where claim and scheduling latency matter.
- Multi-node deployments that want real-time notifications without depending on database-specific features.
- Teams already running Redis infrastructure.

## Notification behavior

Redis Pub/Sub is the low-latency wakeup path. The Redis store itself is always the source of truth.

- **Healthy**: workers pick up new runs immediately via Pub/Sub.
- **Pub/Sub disconnected**: wakeups fall back to `PollingInterval`. Workers still recover from the durable store.
- **Health checks**: Surefire reports degraded health when Pub/Sub is unhealthy, even if the store is reachable.
