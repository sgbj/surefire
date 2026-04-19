---
title: PostgreSQL
description: Use PostgreSQL as the Surefire job store with real-time notifications.
---

## Installation

```bash
dotnet add package Surefire.PostgreSql
```

## Setup

Register an `NpgsqlDataSource` in DI (for example with `builder.AddNpgsqlDataSource("surefire")` from Aspire, or `builder.Services.AddNpgsqlDataSource(connectionString)`), then:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UsePostgreSql();
});
```

This registers both the job store and notification provider. Real-time notifications use PostgreSQL's `LISTEN`/`NOTIFY`, so workers pick up new jobs immediately instead of waiting for the next poll.

Pass a factory to resolve the data source from a named registration or DI key:

```csharp
options.UsePostgreSql(sp => sp.GetRequiredKeyedService<NpgsqlDataSource>("surefire"));
```

## Schema

With `AutoMigrate` enabled (the default), Surefire creates the required PostgreSQL tables automatically.
The current provider uses the built-in `surefire_*` table names.

## Store and notifications separately

In some setups you may want separate connections for the store and notification provider. For example, if you use PgBouncer in transaction mode for the store but need a direct connection for `LISTEN`/`NOTIFY`, register two keyed data sources and wire each side:

```csharp
options.UsePostgreSqlStore(sp => sp.GetRequiredKeyedService<NpgsqlDataSource>("pgbouncer"));
options.UsePostgreSqlNotifications(sp => sp.GetRequiredKeyedService<NpgsqlDataSource>("primary"));
```

The PostgreSQL provider supports all Surefire features.

## Notification behavior

`LISTEN`/`NOTIFY` is the low-latency wakeup path. PostgreSQL remains the durable store either way.

- **Normal case** — workers wake immediately from notifications.
- **Notification connection outage** — wakeups fall back to polling until notifications recover.
- **Health checks** — Surefire degrades health when notification delivery is unhealthy, even if the store itself is still reachable.

If you run through proxies or poolers, prefer a direct connection for notifications when you need predictable wakeup latency.

