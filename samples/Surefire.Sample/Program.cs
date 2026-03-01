using System.Diagnostics;
using Surefire;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire(options =>
{
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
        var sum = await client.RunAsync<int>("Add", new { a, b }, ct);
        return new { a, b, sum };
    })
    .WithDescription("Adds two random numbers by calling the Add job");

app.AddJob("DataImport", async (JobContext context, ILogger<Program> logger, CancellationToken ct, int count = 10) =>
    {
        for (var i = 1; i <= count; i++)
        {
            logger.LogInformation("Progress: {Percent}", $"{i / (double)count:P}");
            await context.ReportProgressAsync(i / (double)count);
            await Task.Delay(1000, ct);
        }
    })
    .WithDescription("Reports progress")
    .WithTags("progress", "max-concurrency")
    .WithMaxConcurrency(2);

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
        var values = client.StreamAsync<int>("GenerateNumbers", ct);
        var doubled = client.StreamAsync<int>("DoubleNumbers", new { values }, ct);
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

        return await client.RunAsync<int>("Fibonacci", new { n = n - 1 }, ct) +
               await client.RunAsync<int>("Fibonacci", new { n = n - 2 }, ct);
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

app.MapSurefireDashboard("/");

app.Run();

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