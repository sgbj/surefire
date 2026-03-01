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
    options.UseSqlServer("Server=localhost;Database=myapp;Trusted_Connection=true");
});
```

## Schema

With `AutoMigrate` enabled (the default), Surefire creates the required SQL Server tables automatically.
The current provider uses the built-in `dbo.surefire_*` table names.

## Notifications

The SQL Server provider does not include a real-time notification provider. Workers rely on polling to pick up new jobs. Set the `PollingInterval` to control how frequently the scheduler checks for pending runs:

```csharp
options.PollingInterval = TimeSpan.FromSeconds(1);
```

For lower latency, you can combine the SQL Server store with a separate notification provider like Redis:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseSqlServer("Server=localhost;Database=myapp;Trusted_Connection=true");
    options.UseRedisNotifications("localhost:6379");
});
```

