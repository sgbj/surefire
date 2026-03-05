# Surefire

Distributed job scheduling for .NET with minimal API ergonomics.

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire();

var app = builder.Build();

app.AddJob("Hello", () => "Hello, World!");

app.MapSurefireDashboard();

app.Run();
```

## Features

- **Minimal API style** — register jobs with delegates. Parameters are resolved from DI automatically.
- **Distributed** — run multiple nodes. Each job runs exactly once.
- **Dashboard** — built-in real-time UI with live logs, progress tracking, and job management.
- **Cron scheduling** — schedule jobs with cron expressions.
- **Retries** — configurable retry policies with fixed or exponential backoff.
- **Timeouts & concurrency** — per-job timeout and max concurrency limits.
- **Streaming** — return `IAsyncEnumerable<T>` from jobs and pipe streams between them to build pipelines.
- **Lifecycle callbacks** — hook into success, failure, retry, and dead letter events.
- **OpenTelemetry** — traces and metrics out of the box.

## Getting started

```bash
dotnet add package Surefire
dotnet add package Surefire.Dashboard
dotnet add package Surefire.PostgreSql
```

```csharp
using Surefire;
using Surefire.Dashboard;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire();

var app = builder.Build();

// Simple job
app.AddJob("Add", (int a, int b) => a + b);

// Async job with DI, logging, and progress
app.AddJob("DataImport", async (JobContext ctx, ILogger<Program> logger, CancellationToken ct) =>
{
    for (var i = 1; i <= 10; i++)
    {
        logger.LogInformation("Step {I}/10", i);
        await ctx.ReportProgressAsync(i / 10.0);
        await Task.Delay(1000, ct);
    }
}).WithDescription("Imports data and reports progress");

// Scheduled job
app.AddJob("GenerateReport", (ILogger<Program> logger) =>
{
    logger.LogInformation("Generating report");
}).WithCron("0 * * * *");

// Job with retries
app.AddJob("Flaky", () =>
{
    if (Random.Shared.Next(3) != 0)
        throw new Exception("Bad luck");
}).WithRetry(3);

app.MapSurefireDashboard();

app.Run();
```

## Triggering jobs

Use `IJobClient` to trigger jobs from anywhere:

```csharp
// Fire and forget
await client.TriggerAsync("Cleanup");

// With arguments
await client.TriggerAsync("Add", new { a = 1, b = 2 });

// Run and wait for the result
var sum = await client.RunAsync<int>("Add", new { a = 1, b = 2 });
```

## Job composition

Jobs can call other jobs:

```csharp
app.AddJob("Workflow", async (IJobClient client, CancellationToken ct) =>
{
    var sum = await client.RunAsync<int>("Add", new { a = 100, b = 200 }, ct);
    return $"The sum is {sum}";
});
```

## Lifecycle callbacks

```csharp
app.AddJob("Order", async (int orderId) => { /* ... */ })
    .WithRetry(3)
    .OnFailure((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogWarning("Attempt {Attempt} failed", ctx.Attempt);
    })
    .OnDeadLetter((JobContext ctx) =>
    {
        // All retries exhausted
    });
```

`OnSuccess` fires when a job completes successfully. `OnFailure` fires on every failure. `OnRetry` fires when a retry is about to happen. `OnDeadLetter` fires when all retries are used up.

## Dashboard

```csharp
app.MapSurefireDashboard();         // at /surefire
app.MapSurefireDashboard("/");      // at the root
```

The dashboard is embedded in the NuGet package. It includes:

- Live log streaming and progress bars
- Job management (trigger, enable/disable)
- Run history with filtering
- Node monitoring
- A REST API at `{prefix}/api/`

## Documentation

See the [full documentation](https://batary.dev/surefire) for more details.

## License

MIT
