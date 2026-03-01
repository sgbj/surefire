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

Use `StreamAsync<T>` on `IJobClient` to chain streaming jobs together. Each stage runs as its own job with independent logging, retries, and progress tracking. Data flows between them without waiting for the upstream job to finish.

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

When a stream consumer is cancelled, the cancellation propagates to the producing job so it can stop work.

This propagation applies to owner-style calls (`StreamAsync*`).
Observer-style calls (`WaitStreamAsync*`) only stop local consumption and do not cancel the run.
