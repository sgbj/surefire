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

Enable `READ_COMMITTED_SNAPSHOT` on the database to keep dashboard reads from blocking writers and reduce lock contention between claims, inserts, and maintenance under load:

```sql
ALTER DATABASE [YourDb] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
```

This requires `ALTER` on the database and exclusive access (hence `ROLLBACK IMMEDIATE`), so run it once during provisioning.

## Notifications

The SQL Server provider has no built-in notification transport. Workers wake up on `PollingInterval` (default 5 seconds). Lower it for faster pickup of new runs:

```csharp
options.PollingInterval = TimeSpan.FromSeconds(2);
```

For real-time wakeups, pair the SQL Server store with the Redis notification provider. Register an `IConnectionMultiplexer` in DI, then:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("Surefire")!);
    options.UseRedisNotifications();
});
```
