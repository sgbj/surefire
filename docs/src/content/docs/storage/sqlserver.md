---
title: SQL Server
description: Use SQL Server as the Surefire job store.
---

## Installation

```bash
dotnet add package Surefire.SqlServer
```

## Setup

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("surefire")!);
});
```

Or pass the connection string directly:

```csharp
options.UseSqlServer("Server=localhost;Database=myapp;Trusted_Connection=true");
```

Point Surefire at a dedicated user database — not `master`, `tempdb`, `model`, or `msdb`. The principal used for migration needs permission to create tables; at runtime `db_datareader`/`db_datawriter` is enough.

For best performance, enable `READ_COMMITTED_SNAPSHOT` on the database. This prevents dashboard reads from blocking writers and reduces lock contention between claims, inserts, and maintenance under load:

```sql
ALTER DATABASE [YourDb] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
```

This requires `ALTER` on the database and exclusive access (hence `ROLLBACK IMMEDIATE`), so run it once during provisioning.

## Schema

With `AutoMigrate` enabled (the default), Surefire creates the required SQL Server tables automatically.
The current provider uses the built-in `dbo.surefire_*` table names.

## Notifications

The SQL Server provider does not include a real-time notification provider. Workers rely on polling to pick up new jobs. Set the `PollingInterval` to control how frequently the scheduler checks for pending runs:

```csharp
options.PollingInterval = TimeSpan.FromSeconds(1);
```

For lower latency, you can combine the SQL Server store with Redis as the notification provider. Register an `IConnectionMultiplexer` in DI (e.g. `builder.AddRedisClient("notifications")`) then:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseSqlServer("Server=localhost;Database=myapp;Trusted_Connection=true");
    options.UseRedisNotifications();
});
```

