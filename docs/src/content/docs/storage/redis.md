---
title: Redis
description: Use Redis as the Surefire job store with real-time notifications.
---

## Installation

```bash
dotnet add package Surefire.Redis
```

## Setup

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseRedis("localhost:6379");
});
```

This registers both the job store and the notification provider. Notifications use Redis Pub/Sub for real-time event delivery.

## Using an existing connection

If you already have an `IConnectionMultiplexer` registered in DI:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseRedis(connection);
});
```

## When to use Redis

Redis is a good fit for:

- High-throughput workloads where claim latency matters
- High-throughput workloads where low scheduling latency matters
- Multi-node deployments that need real-time notifications without database-specific features (like PostgreSQL's LISTEN/NOTIFY)
- Teams already running Redis infrastructure

The Redis provider supports all Surefire features.

