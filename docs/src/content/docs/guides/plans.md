---
title: Plans
description: Compose jobs into multi-step execution graphs with dependencies, branching, and streaming.
---

## Overview

A plan is a directed graph of job steps with data dependencies between them. You define the graph using `PlanBuilder`, and the engine handles scheduling, data passing, failure handling, and parallelism.

```csharp
var plan = new PlanBuilder();
var a = plan.Run<int>("Add", new { a = 1, b = 2 });
var b = plan.Run<int>("Add", new { a = 3, b = 4 });
var sum = plan.Run<int>("Add", new { a, b });

var result = await client.RunAsync(plan, sum);
// result = 10
```

Steps run as regular jobs. The engine creates a pending run for each step when its dependencies are satisfied, and a worker picks it up like any other job. Retries, timeouts, concurrency limits, and all other job features work normally.

## Creating a plan

Use `PlanBuilder` to define steps. Each step returns a handle (`Step<T>` or `Step`) that you can pass as an argument to downstream steps, creating data dependencies.

```csharp
var plan = new PlanBuilder();

// These two steps run in parallel (no dependency between them)
var users = plan.Run<User[]>("FetchUsers");
var config = plan.Run<Config>("LoadConfig");

// This step depends on both
var report = plan.Run<string>("BuildReport", new { users, config });
```

## Running a plan

There are three ways to run a plan:

```csharp
// Fire and forget
await client.TriggerAsync(plan);

// Wait for completion (no result)
await client.RunAsync(plan);

// Wait for a specific step's result
var result = await client.RunAsync<string>(plan, report);
```

## Step types

### Run

Executes a registered job and returns its result.

```csharp
var result = plan.Run<int>("Add", new { a = 1, b = 2 });
var noResult = plan.Run("Cleanup");
```

### Stream

Executes a streaming job. The step handle can be passed to downstream steps as an `IAsyncEnumerable<T>` parameter, and items flow in real time.

```csharp
var lines = plan.Stream<string>("FetchLines");
var processed = plan.Run<int>("ProcessLines", new { lines });
```

### WhenAll

Waits for multiple steps of the same type and collects their results into an array.

```csharp
var a = plan.Run<int>("Add", new { a = 1, b = 2 });
var b = plan.Run<int>("Add", new { a = 3, b = 4 });
var results = plan.WhenAll(a, b); // Step<int[]> containing [3, 7]
```

### ForEach

Runs a job once per item in an array. The body lambda defines a template that gets instantiated for each item at runtime.

```csharp
var users = plan.Run<User[]>("FetchUsers");

// Run "SendEmail" for each user, collect results
var results = users.ForEach((p, user) =>
    p.Run<bool>("SendEmail", new { user }));
// results is Step<bool[]>
```

`ForEach` without a result type:

```csharp
users.ForEach((p, user) => p.Run("NotifyUser", new { user }));
```

### StreamForEach

Like `ForEach`, but emits results as they complete instead of waiting for all iterations. Returns a `StreamStep<T>`.

```csharp
var orders = plan.Stream<Order>("FetchOrders");
var receipts = orders.StreamForEach((p, order) =>
    p.Run<Receipt>("ProcessOrder", new { order }));
```

### If and Switch

Conditional branching based on a step's result. Each branch is a template — only the matching branch's steps are created and executed.

```csharp
var isLarge = plan.Run<bool>("CheckSize", new { order });
var result = plan.If(isLarge, p => p.Run<string>("ProcessLarge", new { order }))
    .Else(p => p.Run<string>("ProcessSmall", new { order }));
```

Chain multiple conditions with `ElseIf`:

```csharp
var result = plan.If(isUrgent, p => p.Run<string>("ExpressShip", new { order }))
    .ElseIf(isLarge, p => p.Run<string>("FreightShip", new { order }))
    .Else(p => p.Run<string>("StandardShip", new { order }));
```

If no `.Else()` is needed, just don't call it — the step is added to the graph immediately.

Switch on a string value:

```csharp
var region = plan.Run<string>("GetRegion", new { user });
var result = plan.Switch<string>(region)
    .Case("us", p => p.Run<string>("ProcessUS", new { user }))
    .Case("eu", p => p.Run<string>("ProcessEU", new { user }))
    .Default(p => p.Run<string>("ProcessOther", new { user }));
```

If no `.Default()` is needed, just don't call it — unmatched values produce a null result.

### Sub-plans

`Run` and `Stream` work transparently with plans. If the target job is a registered plan, the engine runs it as a sub-plan automatically — no special API needed:

```csharp
var result = plan.Run<string>("InvoicePlan", new { orderId });
```

The sub-plan runs independently and its result is copied to the parent step when it completes.

### WaitForSignal

Pause the plan until an external signal is delivered:

```csharp
var approval = plan.WaitForSignal<bool>("manager-approval");
var next = plan.Run<string>("ProcessApproval", new { approved = approval });
```

Deliver the signal from outside the plan:

```csharp
await client.SendSignalAsync(planRunId, "manager-approval", true);
```

## Data passing

When you pass a `Step<T>` handle as a property in the arguments object, the engine resolves it at runtime to the step's actual result. The property name maps to the job's parameter name (camelCased automatically).

```csharp
var count = plan.Run<int>("Count");
// "count" maps to the "count" parameter of "Process"
plan.Run("Process", new { count });
```

For streaming steps, the resolved value is a live `IAsyncEnumerable<T>` that the downstream job can iterate as items arrive.

## Step configuration

Steps support the same configuration as regular jobs, plus a few plan-specific options.

```csharp
var step = plan.Run<int>("Risky")
    .WithName("risky-step")           // Name for querying step runs
    .WithPriority(10)                 // Static priority override
    .WithPriority(priorityStep)       // Dynamic priority from another step
    .NotBefore(DateTimeOffset.UtcNow.AddHours(1))
    .WithDeduplicationId("unique-key")
    .WithFallback("FallbackJob")      // Try this job if the primary fails
    .Optional()                       // Don't fail the plan if this step fails
    .DependsOn(otherStep)             // Explicit dependency (in addition to data deps)
    .WithTriggerRule(TriggerRule.AllDone);
```

## Trigger rules

By default, a step runs when all of its dependencies complete successfully (`AllSuccess`). You can change this with `WithTriggerRule`:

| Rule | Behavior |
|---|---|
| `AllSuccess` | All dependencies completed successfully (default) |
| `AllDone` | All dependencies reached a terminal status, regardless of success or failure |
| `AllFailed` | All dependencies failed or dead-lettered |
| `OneSuccess` | At least one dependency completed successfully |
| `OneFailed` | At least one dependency failed |
| `NoneSkipped` | All dependencies are terminal and none were skipped |

## Failure policy

The plan's failure policy controls what happens when a step fails and has no retries left.

```csharp
var plan = new PlanBuilder();
plan.WithFailurePolicy(StepFailurePolicy.CancelAll);
```

| Policy | Behavior |
|---|---|
| `CancelDependents` | Cancel only steps that depend on the failed step (default) |
| `CancelAll` | Cancel all remaining steps in the plan |
| `ContinueAll` | Ignore the failure and let unrelated steps continue |

## Optional steps

Mark a step as optional so its failure doesn't cascade to dependents:

```csharp
var enrichment = plan.Run<Metadata>("EnrichData", new { order }).Optional();
var report = plan.Run<string>("BuildReport", new { order, enrichment });
```

If `EnrichData` fails, `BuildReport` still runs. The `enrichment` argument resolves to `null`.

## Fallbacks

Specify a fallback job to try if the primary job fails after all retries:

```csharp
var data = plan.Run<Data>("FetchFromPrimary")
    .WithFallback("FetchFromBackup");
```

If `FetchFromPrimary` fails, the engine creates a new run for `FetchFromBackup` with the same arguments. If the fallback also fails, the normal failure policy applies.

## Querying step runs

Use extension methods on `IJobClient` to inspect step runs within a plan:

```csharp
// Get the latest run for a named step
var stepRun = await client.GetStepRunAsync(planRunId, "risky-step");

// Get all runs for a step (useful for ForEach expansions)
var stepRuns = await client.GetStepRunsAsync(planRunId, "fetch-users");
```
