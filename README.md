# Surefire

Distributed job scheduling for .NET with a minimal API style.

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire();

var app = builder.Build();

app.AddJob("Hello", () => "Hello, World!");

app.MapSurefireDashboard();

app.Run();
```

![Surefire dashboard](docs/src/assets/dashboard.png)

## Features

- **Distributed** — run across multiple nodes with coordinated claiming and retry handling.
- **Dashboard** — built-in real-time UI with live logs, progress tracking, and job management.
- **Batches** — trigger many runs at once and consume completion as a stream or aggregate.
- **Cron scheduling** — schedule jobs with cron expressions.
- **Retries** — configurable retry policies with fixed or exponential backoff.
- **Queues** — named queues with priority, concurrency limits, rate limiting, and pause/resume.
- **Timeouts & concurrency** — per-job timeout and max concurrency limits.
- **Streaming input & output** — stream values into and out of jobs with `IAsyncEnumerable<T>`.
- **Lifecycle callbacks** — hook into success, retry, and dead letter events.
- **OpenTelemetry** — traces and metrics out of the box.
- **Health checks** — integrates with ASP.NET Core health checks for store and node liveness.

## Getting started

```bash
# Core packages
dotnet add package Surefire
dotnet add package Surefire.Dashboard

# Optional provider packages (defaults to in-memory store + in-memory notifications)
dotnet add package Surefire.PostgreSql
dotnet add package Surefire.SqlServer
dotnet add package Surefire.Redis
dotnet add package Surefire.Sqlite
```

### Provider capabilities

| Package | Store | Notifications | Notes |
|---|---|---|---|
| `Surefire` | In-memory | In-memory | Best for local development and tests |
| `Surefire.PostgreSql` | Yes | Yes | Best all-around production backend |
| `Surefire.SqlServer` | Yes | No | Use when SQL Server is your system of record |
| `Surefire.Sqlite` | Yes | No | Good for single-node apps, local tools, and development |
| `Surefire.Redis` | Yes | Yes | Best when Redis is already a core dependency |

PostgreSQL and Redis provide cross-node notifications. SQL Server and SQLite currently provide durable storage only, so wakeups fall back to polling behavior.

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire();

var app = builder.Build();

// Simple job
app.AddJob("Add", (int a, int b) => a + b);

// Async job with DI, logging, and progress
app.AddJob("ImportData", async (JobContext ctx, ILogger<Program> logger, CancellationToken ct) =>
{
    for (var i = 1; i <= 10; i++)
    {
        logger.LogInformation("Step {I}/10", i);
        await ctx.ReportProgressAsync(i / 10.0);
        await Task.Delay(1000, ct);
    }
}).WithDescription("Imports data and reports progress");

// Scheduled job
app.AddJob("GenerateReport", async (IReportService reportService, CancellationToken ct) =>
{
    await reportService.GenerateReportAsync(ct);
}).WithCron("0 * * * *");

app.MapSurefireDashboard();

app.Run();
```

Provider packages expose strongly typed options, including command timeout configuration:

```csharp
builder.Services.AddSurefire(options =>
{
    options.UsePostgreSql(new PostgreSqlOptions
    {
        ConnectionString = builder.Configuration.GetConnectionString("surefire-postgres")!,
        CommandTimeout = TimeSpan.FromSeconds(30)
    });
});
```

## Observability and health

Surefire emits OpenTelemetry traces and metrics during runtime execution, and registers a built-in health check named `surefire` via `AddSurefire()`.

```csharp
builder.Services.AddHealthChecks();

app.MapHealthChecks("/health");
```

## Triggering jobs

Use `IJobClient` to trigger jobs from anywhere:

```csharp
// Fire and forget
await client.TriggerAsync("ImportData");

// With arguments
await client.TriggerAsync("Add", new { a = 1, b = 2 });

// Run and wait for the result
var sum = await client.RunAsync<int>("Add", new { a = 1, b = 2 });
```

## Job composition

Compose jobs by calling other jobs with `IJobClient`:

```csharp
app.AddJob("AddRandom", async (IJobClient client, CancellationToken ct) =>
{
    var a = Random.Shared.Next(1, 101);
    var b = Random.Shared.Next(1, 101);
    var sum = await client.RunAsync<int>("Add", new { a, b }, ct);
    return new { a, b, sum };
});
```

## Lifecycle callbacks

```csharp
app.AddJob("ProcessOrder", async (int orderId) => { /* ... */ })
    .WithRetry(3)
    .OnRetry((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogWarning("Attempt {Attempt} failed", ctx.Attempt);
    })
    .OnDeadLetter((JobContext ctx) =>
    {
        // All retries exhausted
    });
```

`OnSuccess` fires when a run completes. `OnRetry` fires when Surefire schedules another attempt after a failure. `OnDeadLetter` fires when no retries remain.

## Dashboard

```csharp
app.MapSurefireDashboard();    // at /surefire
app.MapSurefireDashboard("/"); // at the root
```

The dashboard is embedded in the package and includes:

- Live log streaming and progress bars
- Job management (trigger, enable/disable)
- Run history with filtering
- Node monitoring
- A REST API at `{prefix}/api/`

## Documentation

See the full documentation at [batary.dev/surefire](https://batary.dev/surefire).

## License

MIT
