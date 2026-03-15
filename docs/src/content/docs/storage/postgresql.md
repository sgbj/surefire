---
title: PostgreSQL
description: Use PostgreSQL as the Surefire job store with real-time notifications.
---

## Installation

```bash
dotnet add package Surefire.PostgreSql
```

## Setup

```csharp
builder.Services.AddSurefire(surefire =>
{
    surefire.UsePostgreSql("Host=localhost;Database=myapp");
});
```

This registers both the job store and the notification provider. The notification provider uses PostgreSQL's `LISTEN`/`NOTIFY` for real-time event delivery, so workers pick up new jobs immediately instead of waiting for the next poll.

## Schema

By default, tables are created in a schema called `surefire`. You can change this:

```csharp
surefire.UsePostgreSql("Host=localhost;Database=myapp", options =>
{
    options.Schema = "jobs";
});
```

The schema and all tables are created automatically when `AutoMigrate` is enabled (the default). Tables include `jobs`, `runs`, and `run_events`.

## Store and notifications separately

In some setups you may want to register the store and notification provider independently. For example, if you're using a read replica for the store but need `LISTEN`/`NOTIFY` on the primary:

```csharp
surefire.UsePostgreSqlStore("Host=replica;Database=myapp");
surefire.UsePostgreSqlNotifications("Host=primary;Database=myapp");
```

## Features

The PostgreSQL provider supports all Surefire features:

- Atomic run claiming with `FOR UPDATE SKIP LOCKED`
- Rate limiting via approximate sliding window counts in SQL
- Deduplication via a unique partial index on `deduplication_id`
- Real-time notifications via `LISTEN`/`NOTIFY`
- Automatic schema migration
