---
title: Streaming
description: Stream results between jobs using IAsyncEnumerable.
---

## Producing a stream

A job can return `IAsyncEnumerable<T>` to stream results incrementally instead of returning everything at once. Each yielded item is persisted as it's produced, so consumers can start processing before the producer finishes.

```csharp
app.AddJob("FetchProducts", async (CancellationToken ct) =>
{
    return Fetch();

    async IAsyncEnumerable<string> Fetch()
    {
        await foreach (var line in ReadCsvLinesAsync(ct))
        {
            yield return line;
        }
    }
});
```

## Consuming a stream

A job can accept an `IAsyncEnumerable<T>` parameter. When it runs as part of a pipeline, items arrive in real time as the upstream job produces them.

```csharp
app.AddJob("ConvertProducts", async (IAsyncEnumerable<string> lines, ILogger<Program> logger, CancellationToken ct) =>
{
    return Convert();

    async IAsyncEnumerable<Product> Convert()
    {
        await foreach (var line in lines)
        {
            var parts = line.Split(',');
            var product = new Product(parts[0], parts[1], decimal.Parse(parts[2]));
            logger.LogInformation("Converted: {Product}", product);
            yield return product;
        }
    }
});
```

## Building a pipeline

Use `StreamAsync<T>` or `RunAsync<T>` on `IJobClient` to chain streaming jobs together. Each stage runs as its own job with independent logging, retries, and progress tracking. Data flows between them without waiting for the upstream job to finish.

```csharp
app.AddJob("ProcessProducts", async (IJobClient client, CancellationToken ct) =>
{
    var lines = client.StreamAsync<string>("FetchProducts", ct);
    var products = client.StreamAsync<Product>("ConvertProducts", new { lines }, ct);

    var count = 0;
    await foreach (var product in products)
    {
        count++;
    }

    return $"Processed {count} products";
});
```

## How it works

Under the hood, streaming uses run events. Each yielded item is stored as an `Output` event on the run. An `OutputComplete` event signals the end of the stream (or an error). Consumers subscribe to these events in real time via the notification provider, with a polling fallback for reliability.

When a stream consumer is cancelled, the cancellation propagates to the producing job so it can stop work.

## Streaming in plans

Plans support streaming natively. Use `plan.Stream<T>()` to create a streaming step, and pass its output to downstream steps as an `IAsyncEnumerable<T>` parameter.

```csharp
var plan = new PlanBuilder();
var lines = plan.Stream<string>("FetchProducts");
var products = plan.Stream<Product>("ConvertProducts", new { lines });
var result = plan.Run<string>("ProcessProducts", new { products });

await client.RunAsync(plan);
```

You can also use `StreamForEach` to process each streamed item as a separate job, with results emitted as they complete:

```csharp
var items = plan.Stream<Order>("FetchOrders");
var processed = items.StreamForEach(item => plan.Run<Receipt>("ProcessOrder", new { order = item }));
```

See the [plans guide](/surefire/guides/plans/) for more on plan-based streaming.
