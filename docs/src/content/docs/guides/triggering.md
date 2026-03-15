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

## Waiting for results

Use `RunAsync` to trigger a job and wait for its result:

```csharp
var sum = await client.RunAsync<int>("Add", new { a = 1, b = 2 });

// Without a return value
await client.RunAsync("Cleanup");
```

## Jobs calling other jobs

Jobs can trigger and wait on other jobs. The child run is linked to the parent via `ParentRunId`, which the dashboard uses to show the trace tree.

```csharp
app.AddJob("Workflow", async (IJobClient client, CancellationToken ct) =>
{
    var sum = await client.RunAsync<int>("Add", new { a = 100, b = 200 }, ct);
    return $"The sum is {sum}";
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

If a run with the same deduplication ID already exists, the call throws. Deduplication IDs are freed when runs are purged by the retention policy.
