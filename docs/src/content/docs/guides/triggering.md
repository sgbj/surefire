---
title: Triggering and running
description: Trigger jobs from code, wait for results, fan out batches, and observe existing runs.
---

Use `IJobClient` to run jobs from endpoints, other jobs, or background services.

The methods come in two flavors:

- **Trigger and own** (`Run*`, `Stream*`): start work and consume its outcome. Cancelling your `CancellationToken` cancels the run or batch you started.
- **Observe** (`Wait*`, `Observe*`): consume a run or batch that already exists. Cancelling your token only stops your observer; the run keeps going.

## Trigger a run

`TriggerAsync` enqueues a run and returns immediately. The returned `JobRun` carries the run ID and initial status.

```csharp
// Fire and forget
var run = await client.TriggerAsync("Cleanup");

// With arguments
await client.TriggerAsync("Add", new { a = 1, b = 2 });

// Schedule for later
await client.TriggerAsync("Report", null, new RunOptions
{
    NotBefore = DateTimeOffset.UtcNow.AddHours(1)
});

// With a higher priority (claimed before lower-priority pending runs)
await client.TriggerAsync("Urgent", null, new RunOptions { Priority = 10 });
```

Concurrency and rate limits are enforced when runs are claimed, not at trigger time. `TriggerAsync` always returns a `Pending` run, even if the queue is full.

## Run and wait

`RunAsync<T>` triggers a job and waits for its result.

```csharp
var sum = await client.RunAsync<int>("Add", new { a = 1, b = 2 });
```

For jobs without a result, use the non-generic overload. It throws `JobRunException` if the run terminates non-successfully.

```csharp
await client.RunAsync("Cleanup");
```

## Composing jobs

When a job triggers another, the child carries the parent's `ParentRunId` so the dashboard can render the trace tree. Cancelling the parent cascades to its descendants.

```csharp
app.AddJob("AddRandom", async (IJobClient client, CancellationToken ct) =>
{
    var a = Random.Shared.Next(1, 101);
    var b = Random.Shared.Next(1, 101);
    var sum = await client.RunAsync<int>("Add", new { a, b }, cancellationToken: ct);
    return new { a, b, sum };
});
```

## Stream a single run

If a job returns `IAsyncEnumerable<T>`, consume its output as a stream:

```csharp
await foreach (var line in client.StreamAsync<string>("FetchLines"))
{
    Console.WriteLine(line);
}
```

See [Streaming](/surefire/guides/streaming/) for more information.

## Run a batch

A batch fans out a single job over many inputs, or runs a mix of jobs together. Each input becomes a child run.

```csharp
// One job, many inputs
var results = await client.RunBatchAsync<Result>("RenderInvoice", new[]
{
    new { orderId = 101 },
    new { orderId = 102 },
    new { orderId = 103 }
});

// Different jobs in one batch
var mixed = await client.RunBatchAsync(new[]
{
    new BatchItem("RenderInvoice", new { orderId = 101 }),
    new BatchItem("EmailReceipt", new { orderId = 101 })
});
```

`RunBatchAsync<T>` waits for every child to terminate, then returns the results in commit order. If any child failed or was cancelled, it throws `AggregateException` at the end.

To consume results as each child finishes instead of waiting for the whole batch, use `StreamBatchAsync<T>`:

```csharp
await foreach (var result in client.StreamBatchAsync<Result>("RenderInvoice", inputs))
{
    Console.WriteLine($"Got {result}");
}
```

`StreamBatchAsync<T>` is fail-fast: it throws on the first non-success child, even though the rest of the batch keeps running.

## Observe an existing run or batch

The `Wait*` and `Observe*` methods observe a run or batch without owning it. Useful when you didn't trigger the work but still need to react to it: dashboards, monitoring, code waiting on a run someone else started.

```csharp
// Wait for a terminal status and return the final snapshot
var run = await client.WaitAsync(runId);

// Wait for a terminal status and deserialize the result
var sum = await client.WaitAsync<int>(runId);

// Stream output items from an existing run
await foreach (var item in client.WaitStreamAsync<string>(runId))
{
    Console.WriteLine(item);
}

// Yield each child of a batch as it terminates
await foreach (var child in client.WaitEachAsync(batchId))
{
    Console.WriteLine($"{child.Id}: {child.Status}");
}

// Stream raw events for a run (Output, Progress, Log, status events, etc.)
await foreach (var @event in client.ObserveRunEventsAsync(runId))
{
    Console.WriteLine($"{@event.EventType} #{@event.Id}");
}
```

Pass `sinceEventId` to resume an event stream where you left off.

## Run options

| Option | Description |
|---|---|
| `NotBefore` | Don't run before this time |
| `NotAfter` | Cancel the run if it hasn't started by this time |
| `Priority` | Higher-priority runs are claimed first |
| `DeduplicationId` | Prevents duplicate runs with the same ID |

## Deduplication

A deduplication ID makes sure only one run with a given ID is active at a time. While that run is non-terminal, any trigger using the same ID throws `RunConflictException`. The ID frees up once the run reaches a terminal status.

```csharp
await client.TriggerAsync("Import", args, new RunOptions
{
    DeduplicationId = $"import-{date:yyyy-MM-dd}"
});
```

## Run expiration

Set `NotAfter` to give a run a deadline. If it hasn't started by that time, Surefire cancels it automatically. Useful for time-sensitive work where a late execution is worse than no execution.

```csharp
await client.TriggerAsync("TimelyReport", args, new RunOptions
{
    NotAfter = DateTimeOffset.UtcNow.AddMinutes(30)
});
```

`NotAfter` only cancels runs that haven't started yet. To stop a run that's already executing, cancel it explicitly with `CancelAsync`.

## Cancelling and rerunning

```csharp
// Cancel a run (and any descendants it triggered)
await client.CancelAsync(runId);

// Cancel a whole batch
await client.CancelBatchAsync(batchId);

// Re-execute a finished run with the same arguments and inputs
var rerun = await client.RerunAsync(runId);
```

A rerun creates a new run with the same arguments and input events. See [Job lifecycle](/surefire/concepts/job-lifecycle/) for how reruns differ from retries.

## Querying runs

```csharp
// Single run
var run = await client.GetRunAsync(runId);

// Filtered query, paged automatically
await foreach (var item in client.GetRunsAsync(new RunFilter
{
    JobName = "DataImport",
    Status = JobStatus.Running
}))
{
    Console.WriteLine($"{item.Id} {item.Status}");
}
```
