---
title: PostgreSQL
description: Use PostgreSQL as the Surefire job store with real-time notifications.
---

## Installation

```bash
dotnet add package Surefire.PostgreSql
```

## Setup

The PostgreSQL provider resolves an `NpgsqlDataSource` from DI. Register one with `Npgsql.DependencyInjection` or the Aspire client extension, then call `UsePostgreSql`:

```csharp
builder.Services.AddNpgsqlDataSource(builder.Configuration.GetConnectionString("Surefire")!);

builder.Services.AddSurefire(options => options.UsePostgreSql());
```

That call wires up both the store and notifications. Notifications use PostgreSQL's `LISTEN`/`NOTIFY`, so workers across the cluster wake up immediately when new runs are enqueued.

To resolve a keyed or named data source, pass a factory:

```csharp
options.UsePostgreSql(sp => sp.GetRequiredKeyedService<NpgsqlDataSource>("surefire"));
```

## Schema

With `AutoMigrate` enabled (the default), Surefire creates and migrates the required `surefire_*` tables on startup.

## Store and notifications on separate connections

Some setups need different connections for the store and the notification listener. A common case is running the store through PgBouncer in transaction mode while using a direct connection for `LISTEN`/`NOTIFY`. Register two keyed data sources and wire each side independently:

```csharp
options.UsePostgreSqlStore(sp => sp.GetRequiredKeyedService<NpgsqlDataSource>("store"));
options.UsePostgreSqlNotifications(sp => sp.GetRequiredKeyedService<NpgsqlDataSource>("notifications"));
```

## Notification behavior

`LISTEN`/`NOTIFY` is the low-latency wakeup path. PostgreSQL is always the source of truth.

- **Healthy**: workers wake immediately when notifications arrive.
- **Notification connection lost**: wakeups fall back to `PollingInterval` until the connection recovers.
- **Health checks**: Surefire reports degraded health when notifications are unhealthy, even if the store itself is reachable.
