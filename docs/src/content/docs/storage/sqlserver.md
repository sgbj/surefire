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
    options.UseSqlServer(builder.Configuration.GetConnectionString("Surefire")!);
});
```

## Database

With `AutoMigrate` enabled (the default), Surefire creates and migrates the required `dbo.surefire_*` tables on startup. The principal you use for migration needs permission to create tables. At runtime, `db_datareader` and `db_datawriter` are enough.

Surefire requires both `ALLOW_SNAPSHOT_ISOLATION` and `READ_COMMITTED_SNAPSHOT` to be ON on the target database. Snapshot isolation backs the multi-statement snapshot reads durable orchestrators take at claim time, and `READ_COMMITTED_SNAPSHOT` keeps dashboard reads from blocking writers and reduces lock contention between claims, inserts, and maintenance under load. Enable both once during provisioning:

```sql
ALTER DATABASE [YourDb] SET ALLOW_SNAPSHOT_ISOLATION ON;
ALTER DATABASE [YourDb] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
```

These require `ALTER` on the database and `READ_COMMITTED_SNAPSHOT` requires exclusive access (hence `ROLLBACK IMMEDIATE`).

## Notifications

The SQL Server provider has no built-in notification transport. Workers wake up on `PollingInterval` (default 5 seconds). Lower it for faster pickup of new runs:

```csharp
options.PollingInterval = TimeSpan.FromSeconds(2);
```

For real-time wakeups, pair the SQL Server store with the Redis notification provider. Register an `IConnectionMultiplexer` in DI, then:

```bash
dotnet add package Surefire.Redis
```

```csharp
builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(builder.Configuration.GetConnectionString("Redis")!));

builder.Services.AddSurefire(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("Surefire")!);
    options.UseRedisNotifications();
});
```
