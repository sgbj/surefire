using Surefire;
using Surefire.Dashboard;
using Surefire.PostgreSql;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSurefire(surefire =>
{
    surefire.UsePostgreSql(builder.Configuration.GetConnectionString("Surefire")!);
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

app.AddJob("FetchProducts", async (CancellationToken ct) =>
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
                "Doohickey,Toys,89.99"
            ];

            for (var i = 0; i < products.Length; i++)
            {
                yield return products[i];
                await Task.Delay(1000, ct);
            }
        }
    })
    .WithDescription("Streams raw product CSV strings");

app.AddJob("ConvertProducts", async (IAsyncEnumerable<string> lines, ILogger<Program> logger, CancellationToken ct) =>
    {
        return ConvertProducts();

        async IAsyncEnumerable<Product> ConvertProducts()
        {
            await foreach (var line in lines)
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
        var lines = await client.RunAsync<IAsyncEnumerable<string>>("FetchProducts", cancellationToken: ct);
        var products = await client.RunAsync<IAsyncEnumerable<Product>>("ConvertProducts", new { lines }, ct);

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
            logger.LogInformation("Starting iteration {Iteration}", i);
            await Task.Delay(TimeSpan.FromMinutes(1), ct);
        }
    })
    .Continuous();

app.AddJob("Fibonacci", async (IJobClient client, CancellationToken ct, int n = 5) =>
    {
        if (n <= 1)
        {
            return n;
        }

        return await client.RunAsync<int>("Fibonacci", new { n = n - 1 }, ct) +
               await client.RunAsync<int>("Fibonacci", new { n = n - 2 }, ct);
    });

// === Plan Examples ===

// Shared internal helpers used across multiple plans
app.AddJob("GetOrders", (string region) =>
    new[] { new Order(1, region, 99.99m), new Order(2, region, 149.50m), new Order(3, region, 29.99m) }).Internal();

app.AddJob("ValidateOrders", (Order[] orders) =>
    orders.Where(o => o.Total > 0).Select(o => new ValidatedOrder(o.Id, o.Region, o.Total, true)).ToArray()).Internal();

app.AddJob("GenerateSummary", (ValidatedOrder[] validated) =>
    new OrderSummary(validated.Length, validated.Sum(o => o.Total))).Internal();

app.AddJob("ProcessOrder", (Order order, ILogger<Program> logger) =>
    {
        logger.LogInformation("Processing order {OrderId} in {Region} for {Total}", order.Id, order.Region, order.Total);
        return new OrderResult(order.Id, $"Processed-{order.Id}");
    }).Internal();

// Registered plan — sequential pipeline
app.AddPlan("OrderPipeline", (PlanBuilder plan, Step<string?> region) =>
{
    // Default input: coalesce null to "US" via engine-native LINQ
    var region1 = region.Select(r => r ?? "US");

    var orders = plan.Run<Order[]>("GetOrders", new { region = region1 });
    var validated = plan.Run<ValidatedOrder[]>("ValidateOrders", new { orders });
    return plan.Run<OrderSummary>("GenerateSummary", new { validated });
});

// Registered plan — fan-out with ForEach
app.AddJob("ProcessAllOrders_SendConfirmation", (Order order, OrderResult result, ILogger<Program> logger) =>
    {
        logger.LogInformation("Sending confirmation for order {OrderId}: {TrackingId}", order.Id, result.TrackingId);
    }).Internal();

app.AddPlan("ProcessAllOrders", (PlanBuilder plan, Step<string> region) =>
{
    var orders = plan.Run<Order[]>("GetOrders", new { region });
    var results = orders.ForEach((p, order) =>
    {
        var result = p.Run<OrderResult>("ProcessOrder", new { order });
        p.Run("ProcessAllOrders_SendConfirmation", new { order, result });
        return result;
    });
    return results;
});

// Ad-hoc plan example (inside a job)
app.AddJob("RunAdHocPlan", async (IJobClient client, ILogger<Program> logger, CancellationToken ct) =>
    {
        var plan = new PlanBuilder();

        var a = plan.Run<int>("Add", new { a = 10, b = 20 }).WithName("first-add");
        var b = plan.Run<int>("Add", new { a = 30, b = 40 }).WithName("second-add");
        var both = plan.WhenAll(a, b);

        var sumOfSums = await client.RunAsync(plan, ct);

        var firstResult = await client.GetStepRunAsync(sumOfSums, "first-add", ct);
        var secondResult = await client.GetStepRunAsync(sumOfSums, "second-add", ct);

        logger.LogInformation("First: {First}, Second: {Second}",
            firstResult?.Result, secondResult?.Result);
    })
    .WithDescription("Demonstrates ad-hoc plans with WhenAll");

// === V2 Plan Features ===

// Plan with Switch branching — template-based, only winning branch runs
app.AddJob("ShippingPipeline_ClassifyRegion", (string region) => region switch
{
    "US" or "CA" => "domestic",
    "UK" or "DE" or "FR" => "europe",
    _ => "international"
}).Internal();

app.AddJob("ShippingPipeline_ApplyDomesticShipping", (Order[] orders) =>
    orders.Select(o => new ShippedOrder(o.Id, o.Region, o.Total, 5.99m)).ToArray()).Internal();

app.AddJob("ShippingPipeline_ApplyEuropeShipping", (Order[] orders) =>
    orders.Select(o => new ShippedOrder(o.Id, o.Region, o.Total, 14.99m)).ToArray()).Internal();

app.AddJob("ShippingPipeline_ApplyInternationalShipping", (Order[] orders) =>
    orders.Select(o => new ShippedOrder(o.Id, o.Region, o.Total, 29.99m)).ToArray()).Internal();

app.AddPlan("ShippingPipeline", (PlanBuilder plan, Step<string> region) =>
{
    var orders = plan.Run<Order[]>("GetOrders", new { region });
    var classification = plan.Run<string>("ShippingPipeline_ClassifyRegion", new { region });

    return plan.Switch<ShippedOrder[]>(classification)
        .Case("domestic", p => p.Run<ShippedOrder[]>("ShippingPipeline_ApplyDomesticShipping", new { orders }))
        .Case("europe", p => p.Run<ShippedOrder[]>("ShippingPipeline_ApplyEuropeShipping", new { orders }))
        .Default(p => p.Run<ShippedOrder[]>("ShippingPipeline_ApplyInternationalShipping", new { orders }));
});

// Plan with LINQ operators
app.AddPlan("OrderAnalytics", (PlanBuilder plan, Step<string> region) =>
{
    var orders = plan.Run<Order[]>("GetOrders", new { region });

    // Engine-native LINQ — no jobs needed for filtering/projection
    var highValue = orders.Where(o => o.Total > 50);
    var count = highValue.Count();
    var sorted = orders.OrderByDescending(o => o.Total);
    var topOrder = sorted.First();
    var topTotal = topOrder.Select(o => o.Total);

    return topTotal;
});

// Plan with Optional step and fallback
app.AddJob("ResilientPipeline_FallbackProcessor", (Order[] orders, ILogger<Program> logger) =>
{
    logger.LogWarning("Using fallback processor for {Count} orders", orders.Length);
    return orders.Select(o => new OrderResult(o.Id, $"FALLBACK-{o.Id}")).ToArray();
}).Internal();

app.AddPlan("ResilientPipeline", (PlanBuilder plan, Step<string> region) =>
{
    var orders = plan.Run<Order[]>("GetOrders", new { region });

    // ProcessOrder may fail — use a fallback
    var results = orders.ForEach((p, order) =>
    {
        return p.Run<OrderResult>("ProcessOrder", new { order })
            .WithFallback("ResilientPipeline_FallbackProcessor");
    });

    // Optional validation — failure doesn't block the pipeline
    var validated = plan.Run<ValidatedOrder[]>("ValidateOrders", new { orders }).Optional();

    return results;
});

// Plan with If/Else branching — template-based, only winning branch runs
app.AddJob("ComposedPipeline_IsLargeOrder", (int count) => count > 100).Internal();

app.AddPlan("ComposedPipeline", (PlanBuilder plan, Step<string> region) =>
{
    var orders = plan.Run<Order[]>("GetOrders", new { region });
    var orderCount = orders.Select(o => o.Length);

    var isLarge = plan.Run<bool>("ComposedPipeline_IsLargeOrder", new { count = orderCount });

    plan.If(isLarge, p => { p.Run<OrderResult[]>("ProcessAllOrders", new { region }); })
        .Else(p => { p.Run<OrderSummary>("GenerateSummary",
            new { validated = p.Run<ValidatedOrder[]>("ValidateOrders", new { orders }) }); });

    return orders;
});

// Plan with WaitForSignal
app.AddPlan("ApprovalWorkflow", (PlanBuilder plan, Step<string> region) =>
{
    var orders = plan.Run<Order[]>("GetOrders", new { region });
    var summary = plan.Run<OrderSummary>("GenerateSummary",
        new { validated = plan.Run<ValidatedOrder[]>("ValidateOrders", new { orders }) });

    // Wait for human approval before processing
    var approval = plan.WaitForSignal<bool>("manager-approval");

    // Process orders only after approval (approval step must complete first)
    var results = orders.ForEach((p, order) =>
    {
        return p.Run<OrderResult>("ProcessOrder", new { order }).DependsOn(approval);
    });

    return results;
});

app.MapSurefireDashboard();

app.Run();

record Product(string Name, string Category, decimal Price);
record Order(int Id, string Region, decimal Total);
record ValidatedOrder(int Id, string Region, decimal Total, bool IsValid);
record OrderSummary(int Count, decimal Total);
record OrderResult(int OrderId, string TrackingId);
record ShippedOrder(int OrderId, string Region, decimal OrderTotal, decimal ShippingCost);
