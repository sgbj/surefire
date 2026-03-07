using Surefire;
using Surefire.Dashboard;
using Surefire.PostgreSql;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire(options =>
{
    options.UsePostgreSql(builder.Configuration.GetConnectionString("Surefire")!);
});

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

app.AddJob("DataImport", async (JobContext ctx, ILogger<Program> logger, CancellationToken ct, int count = 100) =>
    {
        logger.LogInformation("Starting data import with count {Count}", count);

        for (var i = 1; i <= count && !ct.IsCancellationRequested; i++)
        {
            var percent = (double)i / count;

            logger.LogInformation("Progress: {Percent}%", Math.Round(percent * 100));

            await ctx.ReportProgressAsync(percent);
            await Task.Delay(1000, ct);
        }

        logger.LogInformation("Finished data import");
    })
    .WithDescription("Imports data and reports progress")
    .WithMaxConcurrency(2);

app.AddJob("EveryThirdTime", (JobContext ctx) =>
    {
        if (ctx.Attempt < 3)
        {
            throw new InvalidOperationException("Only succeeds on the third attempt");
        }
    })
    .WithRetry(3)
    .OnSuccess((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogInformation("Job {Name} succeeded on attempt {Attempt}", ctx.JobName, ctx.Attempt);
    })
    .OnFailure((JobContext ctx, ILogger<Program> logger) =>
    {
        logger.LogWarning("Job {Name} failed on attempt {Attempt}", ctx.JobName, ctx.Attempt);
    });

app.AddJob("ScheduledImport", async (IJobClient client, ILogger<Program> logger, CancellationToken ct) =>
    {
        var count = Random.Shared.Next(1, 5);

        logger.LogInformation("Triggering {Count} data imports", count);

        for (var i = 0; i < count; i++)
        {
            await client.TriggerAsync("DataImport", new { count = Random.Shared.Next(5, 15) }, cancellationToken: ct);
        }
    })
    .WithCron("* * * * *")
    .WithDescription("Triggers a random number of DataImport jobs every minute");

app.AddJob("FetchProducts", async (int count, CancellationToken ct) =>
    {
        return FetchProducts();

        async IAsyncEnumerable<string> FetchProducts()
        {
            string[] products =
            [
                "Widget,Electronics,69.99",
                "Gadget,Electronics,129.99",
                "Sprocket,Hardware,42.00",
                "Flanges,Hardware,88.72",
                "Doohickey,Toys,89.99",
                "Thingamajig,Toys,7.99",
                "Whatchamacallit,Kitchen,19.99",
                "Gizmo,Electronics,34.99",
            ];

            for (var i = 0; i < count && i < products.Length; i++)
            {
                yield return products[i];
                await Task.Delay(1000, ct);
            }
        }
    })
    .WithDescription("Streams raw product CSV strings");

app.AddJob("ConvertProducts", async (IAsyncEnumerable<string> products, ILogger<Program> logger, CancellationToken ct) =>
    {
        return ConvertProducts();

        async IAsyncEnumerable<Product> ConvertProducts()
        {
            await foreach (var line in products)
            {
                var parts = line.Split(',');
                var product = new Product(parts[0], parts[1], decimal.Parse(parts[2]));

                logger.LogInformation("Converted: {Product}", product);

                yield return product;
            }
        }
    })
    .WithDescription("Converts CSV strings into Product records");

app.AddJob("ProcessProducts", async (IJobClient client, ILogger<Program> logger, CancellationToken ct) =>
    {
        var csv = await client.RunAsync<IAsyncEnumerable<string>>("FetchProducts", new { count = 5 }, ct);
        var products = await client.RunAsync<IAsyncEnumerable<Product>>("ConvertProducts", new { products = csv }, ct);

        var total = 0m;
        var count = 0;

        await foreach (var product in products)
        {
            total += product.Price;
            count++;
        }

        logger.LogInformation("Processed {Count} products totalling {Total}", count, total);

        return new { count, total };
    })
    .WithDescription("Processes products by streaming data between jobs")
    .WithTags("streaming");

app.AddJob("AlwaysRunning", async (ILogger<Program> logger, CancellationToken ct) =>
    {
        for (var i = 1; i <= 10; i++)
        {
            logger.LogDebug("Starting iteration {Iteration}", i);
            await Task.Delay(TimeSpan.FromMinutes(1), ct);
        }
    })
    .AsContinuous();

app.AddJob("Fibonacci", async (IJobClient client, CancellationToken ct, int n = 5) =>
    {
        if (n <= 1)
        {
            return n;
        }

        return await client.RunAsync<int>("Fibonacci", new { n = n - 1 }, ct) +
               await client.RunAsync<int>("Fibonacci", new { n = n - 2 }, ct);
    });

app.MapSurefireDashboard();

app.Run();

record Product(string Name, string Category, decimal Price);
