---
title: Streaming
description: Stream results between jobs using IAsyncEnumerable.
---

## Producing a stream

A job can return `IAsyncEnumerable<T>` to stream results incrementally instead of returning everything at once. Each yielded item is persisted as it's produced, so consumers can start processing before the producer finishes.

```csharp
app.AddJob("GenerateNumbers", (CancellationToken ct, int count = 60) =>
{
    return Generate();

    async IAsyncEnumerable<int> Generate()
    {
        for (var i = 1; i <= count; i++)
        {
            yield return i;
            await Task.Delay(1000, ct);
        }
    }
});
```

## Consuming a stream

A job can accept an `IAsyncEnumerable<T>` parameter. When it runs as part of a pipeline, items arrive in real time as the upstream job produces them.

```csharp
app.AddJob("DoubleNumbers", (IAsyncEnumerable<int> values) =>
    values.Select(value => value * 2));
```

## Building a pipeline

Use `StreamAsync<T>` on `IJobClient` to chain streaming jobs together. Each stage runs as its own job with independent logging, retries, and progress tracking. Data flows between stages without waiting for the upstream job to finish.

```csharp
app.AddJob("SumNumbers", async (IJobClient client, CancellationToken ct) =>
{
    var values = client.StreamAsync<int>("GenerateNumbers", cancellationToken: ct);
    var doubled = client.StreamAsync<int>("DoubleNumbers", new { values }, cancellationToken: ct);
    return await doubled.SumAsync(ct);
});
```

`StreamAsync` is an owner-style call: cancelling its `CancellationToken` cancels the run it triggered, so cancellation flows back through the pipeline to the producer. The observer-style equivalent, `WaitStreamAsync`, only stops your local consumption; it never cancels the run.
