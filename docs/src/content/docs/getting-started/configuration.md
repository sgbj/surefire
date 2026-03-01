---
title: Configuration
description: Configure Surefire options, health checks, and the dashboard.
---

## Core options

Pass options to `AddSurefire` to tune runtime behavior:

```csharp
builder.Services.AddSurefire(options =>
{
    options.NodeName = "worker-1";
    options.PollingInterval = TimeSpan.FromSeconds(5);
    options.ShutdownTimeout = TimeSpan.FromSeconds(30);
    options.RetentionPeriod = TimeSpan.FromDays(14);
    options.MaxNodeConcurrency = 16;
});
```

| Option | Default | Description |
|---|---|---|
| `NodeName` | Machine name | Name reported in node views and logs |
| `PollingInterval` | 5s | Fallback polling interval when notifications are not available |
| `HeartbeatInterval` | 30s | Node heartbeat interval |
| `InactiveThreshold` | 2min | Inactivity window used for node/run recovery decisions |
| `RetentionPeriod` | 7 days | How long completed runs are kept. Set to `null` to disable purge |
| `RetentionCheckInterval` | 5min | How often retention cleanup runs |
| `ShutdownTimeout` | 15s | Time allowed for in-flight runs during host shutdown |
| `MaxNodeConcurrency` | `null` | Maximum concurrent executions on this node |
| `AutoMigrate` | `true` | Runs store migrations at startup |
| `SerializerOptions` | `JsonSerializerOptions.Web`-based | JSON options for arguments and results |

## Health checks

`AddSurefire` already registers a health check named `surefire`. To expose it:

```csharp
app.MapHealthChecks("/health");
```

## Dashboard mapping

```csharp
app.MapSurefireDashboard();          // /surefire
app.MapSurefireDashboard("/admin"); // /admin
```

The dashboard is embedded in the `Surefire.Dashboard` package. No separate frontend build step is required.

In production, secure the dashboard endpoints:

```csharp
app.MapSurefireDashboard()
    .RequireAuthorization("AdminPolicy");
```

