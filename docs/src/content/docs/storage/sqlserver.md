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
builder.Services.AddSurefire(surefire =>
{
    surefire.UseSqlServer("Server=localhost;Database=myapp;Trusted_Connection=true");
});
```

## Schema

By default, tables are created in a schema called `surefire`. You can change this:

```csharp
surefire.UseSqlServer("Server=localhost;Database=myapp", options =>
{
    options.Schema = "jobs";
});
```

The schema and all tables are created automatically when `AutoMigrate` is enabled (the default).

## Notifications

The SQL Server provider does not include a real-time notification provider. Workers rely on polling to pick up new jobs. Set the `PollingInterval` to control how frequently the scheduler checks for pending runs:

```csharp
surefire.PollingInterval = TimeSpan.FromSeconds(1);
```

For lower latency, you can combine the SQL Server store with a separate notification provider like Redis.
