---
title: Triggering and running
description: Trigger jobs from code, wait for results, and control scheduling.
---

## Triggering jobs

Use `IJobClient` to trigger jobs. You can inject it anywhere: controllers, other jobs, background services, etc.

```csharp
// Fire and forget
await client.TriggerAsync("Cleanup");

// With arguments
await client.TriggerAsync("Add", new { a = 1, b = 2 });

// Schedule for later
await client.TriggerAsync("Report", null, new RunOptions
{
    NotBefore = DateTimeOffset.UtcNow.AddHours(1)
});

// With priority (higher = claimed first)
await client.TriggerAsync("Urgent", null, new RunOptions { Priority = 10 });
```

`TriggerAsync` is enqueue-first: it creates pending runs and returns immediately. Job and queue max-concurrency limits are enforced when runs are picked up for execution, not when runs are triggered.

## Waiting for results

Use `RunAsync` to trigger a job and wait for its result:

```csharp
var sum = await client.RunAsync<int>("Add", new { a = 1, b = 2 });

// Wait for completion (returns RunResult)
var result = await client.RunAsync("Cleanup");
if (!result.IsSuccess)
{
    // inspect result.Error, result.Status, etc.
}
```

## Jobs calling other jobs

Jobs can trigger and wait on other jobs. The child run is linked to the parent via `ParentRunId`, which the dashboard uses to show the trace tree.

```csharp
app.AddJob("AddRandom", async (IJobClient client, CancellationToken ct) =>
{
    var a = Random.Shared.Next(1, 101);
    var b = Random.Shared.Next(1, 101);
    var sum = await client.RunAsync<int>("Add", new { a, b }, ct);
    return new { a, b, sum };
});
```

## Run options

| Option | Description |
|---|---|
| `NotBefore` | Don't run before this time |
| `NotAfter` | Cancel the run if it hasn't started by this time |
| `Priority` | Higher priority runs are claimed first |
| `DeduplicationId` | Prevents duplicate runs with the same ID |

## Deduplication

Set a deduplication ID to ensure only one run exists for a given key:

```csharp
await client.TriggerAsync("Import", args, new RunOptions
{
    DeduplicationId = $"import-{date:yyyy-MM-dd}"
});
```

If a non-terminal run with the same deduplication ID already exists, the call is rejected. Deduplication IDs are released when the existing run reaches a terminal status.

If run creation is rejected (for example because of deduplication), client calls throw `InvalidOperationException`.

## Querying runs

Use `GetRunAsync` to fetch a single run by ID, or `GetRunsAsync` to query runs with filters:

```csharp
// Get a specific run
var run = await client.GetRunAsync(runId);

// Query runs with filters
await foreach (var item in client.GetRunsAsync(new RunFilter
{
    JobName = "DataImport",
    Status = JobStatus.Running
}))
{
    Console.WriteLine($"{item.Id} {item.Status}");
}
```
