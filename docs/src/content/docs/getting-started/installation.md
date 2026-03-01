---
title: Installation
description: Get up and running with Surefire in minutes.
---

## Install the packages

```bash
dotnet add package Surefire
dotnet add package Surefire.Dashboard

# Choose an optional storage provider:
dotnet add package Surefire.PostgreSql
dotnet add package Surefire.SqlServer
dotnet add package Surefire.Redis
dotnet add package Surefire.Sqlite
```

The core package includes an in-memory store that works out of the box for development.

## Quick start

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire();

var app = builder.Build();

app.AddJob("Hello", () => "Hello, World!");

app.MapSurefireDashboard();

app.Run();
```

Run the app and go to `/surefire` to see the dashboard.

## Next steps

- [Configuration](/surefire/getting-started/configuration/) covers all the options you can pass to `AddSurefire`.
- [Jobs](/surefire/guides/jobs/) explains how to register and configure jobs.
- Storage providers: [PostgreSQL](/surefire/storage/postgresql/), [SQL Server](/surefire/storage/sqlserver/), [Redis](/surefire/storage/redis/), and [SQLite](/surefire/storage/sqlite/).
