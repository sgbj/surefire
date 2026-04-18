---
title: PostgreSQL
description: Use PostgreSQL as the Surefire job store with real-time notifications.
---

## Installation

```bash
dotnet add package Surefire.PostgreSql
```

## Setup

```csharp
builder.Services.AddSurefire(options =>
{
    options.UsePostgreSql("Host=localhost;Database=myapp");
});
```

This registers both the job store and notification provider. Real-time notifications use PostgreSQL's `LISTEN`/`NOTIFY`, so workers pick up new jobs immediately instead of waiting for the next poll.

## Schema

With `AutoMigrate` enabled (the default), Surefire creates the required PostgreSQL tables automatically.
The current provider uses the built-in `surefire_*` table names.

## Store and notifications separately

In some setups you may want separate connections for the store and notification provider. For example, if you use PgBouncer in transaction mode for the store but need a direct connection for `LISTEN`/`NOTIFY`:

```csharp
options.UsePostgreSqlStore("Host=pgbouncer;Database=myapp");
options.UsePostgreSqlNotifications("Host=primary;Database=myapp");
```

The PostgreSQL provider supports all Surefire features.

## Notification behavior

`LISTEN`/`NOTIFY` is the low-latency wakeup path. PostgreSQL remains the durable store either way.

- **Normal case** — workers wake immediately from notifications.
- **Notification connection outage** — wakeups fall back to polling until notifications recover.
- **Health checks** — Surefire degrades health when notification delivery is unhealthy, even if the store itself is still reachable.

If you run through proxies or poolers, prefer a direct connection for notifications when you need predictable wakeup latency.

