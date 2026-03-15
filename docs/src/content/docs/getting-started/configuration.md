---
title: Configuration
description: Configure Surefire options, health checks, and the dashboard.
---

## Options

Pass options to `AddSurefire` to customize behavior:

```csharp
builder.Services.AddSurefire(surefire =>
{
    surefire.NodeName = "worker-1";
    surefire.PollingInterval = TimeSpan.FromSeconds(5);
    surefire.ShutdownTimeout = TimeSpan.FromSeconds(30);
    surefire.RetentionPeriod = TimeSpan.FromDays(14);
});
```

| Option | Default | Description |
|---|---|---|
| `NodeName` | Machine name | Identifies this node in the cluster |
| `PollingInterval` | 1s | How often to check for pending jobs |
| `HeartbeatInterval` | 30s | How often to send heartbeats |
| `InactiveThreshold` | 2min | When to consider a node inactive |
| `RetentionPeriod` | 7 days | How long to keep completed runs. Set to `null` to keep them forever |
| `RetentionCheckInterval` | 5min | How often to check for expired runs to purge |
| `ShutdownTimeout` | 15s | Grace period for running jobs on shutdown |
| `MaxNodeConcurrency` | `null` | Max concurrent jobs on this node. `null` means unlimited |
| `AutoMigrate` | `true` | Automatically create/update the store schema on startup |
| `SerializerOptions` | `JsonSerializerOptions.Web` | JSON serializer options for job arguments and results |

## Client-only mode

If you have a service that only needs to trigger or monitor jobs (but not execute them), use `AddSurefireClient` instead of `AddSurefire`. This registers `IJobClient` and the store without starting the scheduler or executor.

```csharp
builder.Services.AddSurefireClient(surefire =>
{
    surefire.UsePostgreSql("Host=localhost;Database=myapp");
});
```

This is useful for API servers that trigger jobs, or monitoring services that query run status. The worker nodes use `AddSurefire` as normal.

## Health checks

Surefire ships with an ASP.NET Core health check that verifies store connectivity:

```csharp
builder.Services.AddHealthChecks()
    .AddSurefire();

app.MapHealthChecks("/health");
```

## Dashboard

```csharp
app.MapSurefireDashboard();         // at /surefire
app.MapSurefireDashboard("/");      // at the root
app.MapSurefireDashboard("/admin"); // at /admin
```

The dashboard is embedded in the `Surefire.Dashboard` NuGet package. No npm install or extra build steps needed. See the [dashboard guide](/surefire/guides/dashboard/) for a walkthrough.

In production, always add authorization. See the [dashboard guide](/surefire/guides/dashboard/#authorization) for details.
