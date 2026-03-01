using Surefire;
using Surefire.Dashboard;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire();

var app = builder.Build();

app.AddJob("Add", (int a, int b) => a + b);

app.AddJob("AddRandom", async (IJobClient client, ILogger<Program> logger, CancellationToken ct) =>
    {
        var a = Random.Shared.Next(100);
        var b = Random.Shared.Next(100);

        var sum = await client.RunAsync<int>("Add", new { a, b }, ct);

        logger.LogInformation("{A} + {B} = {Sum}", a, b, sum);
    })
    .WithDescription("Calls Add with two random numbers");

app.AddJob("SlowJob", async (JobContext jobContext, ILogger<Program> logger, CancellationToken ct, int count = 100) =>
    {
        logger.LogInformation("Starting slow job with count {Count}", count);

        for (var i = 1; i <= count && !ct.IsCancellationRequested; i++)
        {
            var percent = (double)i / count;
            
            logger.LogInformation("Progress: {Percent}%", Math.Round(percent * 100));
            
            await jobContext.ReportProgressAsync(percent);
            await Task.Delay(1000, ct);
        }

        logger.LogInformation("Finished slow job");
    })
    .WithDescription("A slow job that reports progress");

app.AddJob("EveryThirdTime", (JobContext jobContext) =>
    {
        if (jobContext.Attempt < 3)
        {
            throw new InvalidOperationException("Only succeeds on the third attempt");
        }
    })
    .WithRetry(3)
    .OnSuccess((JobContext jobContext, ILogger<Program> logger) =>
    {
        logger.LogInformation("Job {Name} succeeded on attempt {Attempt}", jobContext.JobName, jobContext.Attempt);
    })
    .OnFailure((JobContext jobContext, ILogger<Program> logger) =>
    {
        logger.LogWarning("Job {Name} failed on attempt {Attempt}", jobContext.JobName, jobContext.Attempt);
    });

app.AddJob("TriggerSlowJob", async (IJobClient client, ILogger<Program> logger, CancellationToken ct) =>
    {
        var count = Random.Shared.Next(1, 4);
        
        logger.LogInformation("Triggering {Count} jobs", count);

        for (var i = 0; i < count; i++)
        {
            await client.TriggerAsync("SlowJob", new { count = Random.Shared.Next(5, 15) }, cancellationToken: ct);
        }
    })
    .WithCron("* * * * *")
    .WithDescription("Triggers a random number of SlowJob every minute");

app.MapSurefireDashboard("/");

app.Run();