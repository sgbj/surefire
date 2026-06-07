---
title: Durable orchestration
description: Coordinate multi-step workflows that survive restarts by replaying from recorded history.
---

A durable job coordinates other jobs: it triggers child runs, waits for their results, and decides what to do next. Surefire records each step so the job can suspend while it waits and resume later, surviving restarts and crashes.

Mark a job durable with `.Durable()`:

```csharp
app.AddJob("Fibonacci", async (IJobClient client, CancellationToken ct, int n = 5) =>
{
    if (n <= 1)
    {
        return n;
    }

    var a = await client.RunAsync<int>("Fibonacci", new { n = n - 1 }, cancellationToken: ct);
    var b = await client.RunAsync<int>("Fibonacci", new { n = n - 2 }, cancellationToken: ct);
    
    return a + b;
})
.Durable();
```

## How replay works

A durable job doesn't hold a worker while it waits for child work. It triggers the child runs and **suspends**. When the children finish, Surefire re-runs the handler **from the top**.

On that second run, every `IJobClient` call that already completed returns its recorded result instead of running again. `client.RunAsync<int>("Fibonacci", ...)` doesn't start a new child run on replay; it returns the value the child produced the first time. The handler replays up to the point it left off, then continues with new work.

The recorded history lives in the store, so a job can suspend indefinitely and resume after the process restarts.

A suspended run shows as `Suspended` in the dashboard. It isn't claimed by workers and doesn't use a concurrency slot while it waits. See [Job lifecycle](/surefire/concepts/job-lifecycle/) for the full status list.

## Determinism

Because the handler runs again on every resume, it must take the same path each time. If a replay diverges from the recorded history, the run fails with a `DurableReplayMismatchException`.

This means you can't call anything non-deterministic directly in a durable handler: the current time, random numbers, new GUIDs, or external I/O such as HTTP and database calls. Each returns a different value on replay. Handle these in one of two ways.

### Record the value

For values like the current time or a random number, use the recorded helpers on `JobContext`. They run once, store the result, and return the same value on replay:

```csharp
app.AddJob("Reminder", async (IJobClient client, JobContext context, CancellationToken ct) =>
{
    var id = await context.NewGuidAsync();
    var scheduledAt = await context.GetUtcNowAsync();

    await client.RunAsync("SendReminder", new { id, scheduledAt }, cancellationToken: ct);
})
.Durable();
```

| Method | Replay-safe version of |
|---|---|
| `NewGuidAsync()` | `Guid.NewGuid()` |
| `NewGuidV7Async()` | `Guid.CreateVersion7()` |
| `GetUtcNowAsync()` | The current UTC time |
| `NextInt32Async()` | `Random.Shared.Next()` (overloads take bounds) |
| `NextDoubleAsync()` | `Random.Shared.NextDouble()` |

For anything else, `RecordAsync` runs a factory once and replays its stored result:

```csharp
app.AddJob("Quote", async (JobContext context, IRateService rates, CancellationToken ct) =>
{
    var rate = await context.RecordAsync("exchange-rate", async () =>
    {
        return await rates.GetExchangeRateAsync(ct);
    });

    // rate stays the same on every replay
})
.Durable();
```

### Move the work into a child job

`RecordAsync` runs its factory inline and records the result only *after* it returns, so a crash mid-call re-runs it on resume, with no automatic retry. That's fine for small, idempotent work like a quick HTTP read. But when running twice would be a bug (charging a card, writing a row, sending mail), give the work its own job and run it as a child. The side effect runs once, with its own retries and dashboard row, and replay returns the recorded result instead of repeating it:

```csharp
app.AddJob("ChargeCard", async (IPaymentGateway gateway, Payment payment) => await gateway.ChargeAsync(payment));

app.AddJob("Checkout", async (IJobClient client, CancellationToken ct, Order order) =>
{
    var receipt = await client.RunAsync<Receipt>("ChargeCard", new { order.Payment }, cancellationToken: ct);
    await client.RunAsync("EmailReceipt", new { order.CustomerId, receipt }, cancellationToken: ct);
})
.Durable();
```

On replay, the orchestrator gets the recorded receipt back, so the card is never charged twice.

## Batches

Everything in [Triggering and running](/surefire/guides/triggering/) works inside a durable job, including batches. The orchestrator suspends until the batch completes:

```csharp
app.AddJob("ProcessAll", async (IJobClient client, CancellationToken ct) =>
{
    var results = await client.RunBatchAsync<Result>("ProcessOrder", inputs, cancellationToken: ct);
    return results.Count;
})
.Durable();
```

Streaming a batch processes each result as it completes, suspending between results:

```csharp
app.AddJob("StreamAll", async (IJobClient client, CancellationToken ct) =>
{
    var total = 0L;
    await foreach (var result in client.StreamBatchAsync<Result>("ProcessOrder", inputs, cancellationToken: ct))
    {
        total += result.Amount;
    }
    return total;
})
.Durable();
```

## When a child fails

If a child run fails or is canceled, the `RunAsync<T>` call that started it throws a `JobRunException`, the same as outside a durable job. Catch it to handle the failure:

```csharp
try
{
    await client.RunAsync("RiskyStep", cancellationToken: ct);
}
catch (JobRunException)
{
    await client.RunAsync("Compensate", cancellationToken: ct);
}
```

An uncaught `JobRunException` fails the orchestrator. A replay would hit the same failed child, so Surefire dead-letters the run immediately instead of retrying it. Handle child failures you can recover from in the handler.

## Replay and observability

Because the handler re-runs from the top on every resume, anything that isn't a recorded operation runs again each time. Real side effects belong in a [child job](#move-the-work-into-a-child-job), which runs once and records its result so replay never repeats it. Surefire automatically ignores logger messages during a replay, so logs won't appear duplicated on the dashboard.

That leaves fire-and-forget observability that Surefire doesn't own, such as a metric counter or a trace span. Use `IsReplaying` to emit it only as the handler reaches new work, not each time it replays history:

```csharp
app.AddJob("Checkout", async (IJobClient client, JobContext context, CancellationToken ct, Order order) =>
{
    var receipt = await client.RunAsync<Receipt>("ChargeCard", new { order.Payment }, cancellationToken: ct);

    if (!context.IsReplaying)
    {
        CheckoutMetrics.Charged.Add(1);
    }

    await client.RunAsync("EmailReceipt", new { order.CustomerId, receipt }, cancellationToken: ct);
})
.Durable();
```

`IsReplaying` is `true` while the handler replays recorded history and `false` once it reaches new work. It's always `false` for non-durable jobs; `IsDurable` tells you whether the run is a durable orchestrator at all.
