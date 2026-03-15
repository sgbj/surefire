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
builder.Services.AddSurefire(surefire =>
{
    surefire.UseSqlite("Data Source=surefire.db");
});
```

## When to use SQLite

SQLite is a good fit for:

- Development and testing
- Single-node deployments where you want persistence without running a database server
- Embedded applications

It's not suitable for multi-node deployments since the database file is local to one machine.

## WAL mode

The SQLite provider enables WAL (write-ahead logging) mode automatically. This allows concurrent reads while a write is in progress, which is important for the scheduler and executor running simultaneously.

## Notifications

The SQLite provider does not include a real-time notification provider. Workers rely on polling. Since SQLite is typically used in single-node setups, the default 1-second polling interval is usually fine.
