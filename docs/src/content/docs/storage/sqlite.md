---
title: SQLite
description: Use SQLite as the Surefire job store for single-node deployments.
---

## Installation

```bash
dotnet add package Surefire.Sqlite
```

## Setup

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseSqlite("Data Source=surefire.db");
});
```

## When to use SQLite

SQLite is a good fit for:

- Development and testing
- Single-node deployments where you want persistence without running a database server
- Embedded applications

SQLite is not suitable for multi-node deployments since the database file is local to one machine.

## Notifications

The SQLite provider does not include a real-time notification provider. Workers rely on polling. Since SQLite is typically used in single-node setups, the default 5-second polling interval is usually fine. You can lower it if needed:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UseSqlite("Data Source=surefire.db");
    options.PollingInterval = TimeSpan.FromSeconds(1);
});
```

