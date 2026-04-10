using System.Diagnostics;
using Surefire;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire(options =>
{
    options.UsePostgreSql("Host=localhost;Database=surefire;Username=postgres;Password=postgres");
    options.AddFixedWindowLimiter("Fibonacci", 10, TimeSpan.FromSeconds(10));
    options.UseFilter<StopwatchJobFilter>();
});

var app = builder.Build();

app.AddJob("Add", (int a, int b) => a + b)
    .WithDescription("Adds two numbers");

app.AddJob("AddRandom", async (IJobClient client, CancellationToken ct) =>
    {
        var a = Random.Shared.Next(1, 100);
        var b = Random.Shared.Next(1, 100);
        var sum = await client.RunAsync<int>("Add", new { a, b }, cancellationToken: ct);
        return new AddRandomResult(a, b, sum);
    })
    .WithDescription("Adds two random numbers by calling the Add job")
    .WithMaxConcurrency(50);

app.AddJob("DataImport", async (JobContext context, ILogger<Program> logger, CancellationToken ct, int count = 10) =>
    {
        for (var i = 1; i <= count; i++)
        {
            logger.LogInformation("Progress: {Percent:P}", i / (double)count);
            await context.ReportProgressAsync(i / (double)count);
            await Task.Delay(1000, ct);
        }
    })
    .WithDescription("Reports progress")
    .WithTags("progress", "max-concurrency")
    .WithMaxConcurrency(25);

app.AddJob("Flaky", () =>
    {
        if (Random.Shared.Next(4) != 0)
        {
            throw new InvalidOperationException("Flaky job failed.");
        }
    })
    .WithDescription("Demonstrates retries and dead-letter behavior")
    .WithTags("retry", "lifecycle-hooks")
    .WithRetry(2)
    .OnSuccess((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogInformation("{JobName} succeeded after attempt {Attempt}", ctx.JobName, ctx.Attempt);
    })
    .OnRetry((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogWarning("{JobName} retry scheduled after attempt {Attempt}", ctx.JobName, ctx.Attempt);
    })
    .OnDeadLetter((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogError("{JobName} dead-lettered after attempt {Attempt}", ctx.JobName, ctx.Attempt);
    });

app.AddJob("GenerateNumbers", async (CancellationToken ct, int count = 60) =>
    {
        return GenerateNumbers();

        async IAsyncEnumerable<int> GenerateNumbers()
        {
            for (var i = 1; i <= count; i++)
            {
                yield return i;
                await Task.Delay(1000, ct);
            }
        }
    })
    .WithDescription("Produces a stream of numbers")
    .WithTags("stream");

app.AddJob("DoubleNumbers", (IAsyncEnumerable<int> values) => values.Select(x => x * 2))
    .WithDescription("Takes a stream of numbers and doubles them")
    .WithTags("stream");

app.AddJob("SumNumbers", async (IJobClient client, CancellationToken ct) =>
    {
        var values = client.StreamAsync<int>("GenerateNumbers", cancellationToken: ct);
        var doubled = client.StreamAsync<int>("DoubleNumbers", new { values }, cancellationToken: ct);
        return await doubled.SumAsync(ct);
    })
    .WithDescription("Composes stream jobs")
    .WithTags("stream");

app.AddJob("Timeout", async (CancellationToken ct) => await Task.Delay(TimeSpan.FromSeconds(10), ct))
    .WithDescription("Timeout after 5 seconds")
    .WithTags("timeout")
    .WithTimeout(TimeSpan.FromSeconds(5));

app.AddJob("Fibonacci", async (IJobClient client, CancellationToken ct, int n = 5) =>
    {
        if (n <= 1)
        {
            return n;
        }

        return await client.RunAsync<int>("Fibonacci", new { n = n - 1 }, cancellationToken: ct) +
               await client.RunAsync<int>("Fibonacci", new { n = n - 2 }, cancellationToken: ct);
    })
    .WithDescription("Computes Fibonacci recursively")
    .WithRateLimit("Fibonacci")
    .WithTags("rate-limit");

app.AddJob("ScheduledDataImport", async (IJobClient client, ILogger<Program> logger) =>
    {
        var count = Random.Shared.Next(1, 5);
        logger.LogInformation("Triggering {Count} data import jobs", count);
        for (var i = 0; i < count; i++)
        {
            await client.TriggerAsync("DataImport", new { count = Random.Shared.Next(10, 20) });
        }
    })
    .WithDescription("Scheduled demo job")
    .WithCron("* * * * *")
    .WithMisfirePolicy(MisfirePolicy.FireOnce)
    .WithTags("cron");

app.AddJob("AlwaysRunning", async (ILogger<Program> logger, CancellationToken ct) =>
    {
        for (var i = 1; i <= 5; i++)
        {
            logger.LogInformation("Iteration {I}", i);
            await Task.Delay(TimeSpan.FromMinutes(1), ct);
        }
    })
    .Continuous()
    .WithTags("continuous");

app.AddJob("BigJob", async (IJobClient client, ILogger<Program> logger, CancellationToken ct) =>
    {
        for (var i = 1; i <= 10000; i++)
        {
            logger.LogInformation("Iteration: {I}", i);
        }

        var results = await client.RunBatchAsync<AddRandomResult>("AddRandom", new object?[10000], cancellationToken: ct);
        return results.Sum(r => (long)r.Sum);
    });

app.MapSurefireDashboard("/");

app.MapHealthChecks("/healthz");

app.Run();

record AddRandomResult(int A, int B, int Sum);

class StopwatchJobFilter(ILogger<StopwatchJobFilter> logger) : IJobFilter
{
    public async Task InvokeAsync(JobContext context, JobFilterDelegate next)
    {
        var timestamp = Stopwatch.GetTimestamp();
        await next(context);
        var elapsed = Stopwatch.GetElapsedTime(timestamp);
        logger.LogInformation("Stopwatch: {JobName} finished in {Elapsed}", context.JobName, elapsed);
    }
}