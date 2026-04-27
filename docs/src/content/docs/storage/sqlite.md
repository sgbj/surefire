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

SQLite is a good fit for development, testing, embedded apps, and single-node deployments where you want persistence without running a database server.

## Schema

With `AutoMigrate` enabled (the default), Surefire creates and migrates the required `surefire_*` tables on startup.

## Notifications

The SQLite provider has no built-in notification transport. Workers wake up on `PollingInterval` (default 5 seconds). Lower it for faster pickup of new runs:

```csharp
options.PollingInterval = TimeSpan.FromSeconds(2);
```
