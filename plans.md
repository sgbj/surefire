# Plans — Unified API for Batches, Continuations, and DAGs

## Design Philosophy

Two dominant patterns exist in the industry:

1. **Procedural/replay** (Temporal, Durable Functions): Write normal async code, framework replays on recovery. Graph is runtime-only, invisible until execution.
2. **Declarative DAG** (Airflow, Hatchet): Declare steps and dependencies explicitly. Graph is static, inspectable before execution.

Both have tradeoffs. Procedural code is natural but ties up a concurrency slot (the orchestrator job sits idle while children run). Declarative DAGs are inspectable but require a separate DSL.

**Surefire's approach: deferred execution graphs built with natural C# syntax.** Steps are declared with `plan.Run<T>()` which returns a handle, not a result. Dependencies are inferred from data flow (passing one step's handle as another's input) or declared explicitly with `.DependsOn()`. This gives us:

- Natural C# code (no DSL, no YAML, no visual designer)
- Implicit parallelism from data dependencies (no explicit `Task.WhenAll`)
- Static inspectable graphs (the plan IS the graph)
- No wasted concurrency slots (the orchestrator doesn't hold a slot while steps run)
- Typed data flow between steps

### Key design constraints

- **Every step is a named job (or an engine-native operator).** No in-memory delegates or inline tasks. The plan graph is fully serializable and self-contained in the DB. Any node can advance any plan. Deployments don't corrupt in-flight plans. Engine-native operators (v2: `Where`, `Select`, etc.) are evaluated directly by the engine during advancement — no job registration needed.
- **Limited, controlled expression trees (v2 only).** Expression trees are used only for engine-native LINQ operators (`Where`, `Select`, etc.) where they're converted to a fixed operator model (member access, comparisons, constants) — not arbitrary code. For v1, all projections, predicates, and transforms are registered jobs. For complex logic beyond the operator set, use a job.
- **JSON graph, not relational.** The plan graph is a JSON document stored on the plan run. Step executions are regular job runs in the `runs` table. The graph is the plan; the runs are the execution.

---

## Core API

### Creating a plan

```csharp
app.AddJob("FulfillOrders", async (IJobClient client, FulfillmentRequest request, CancellationToken ct) =>
{
    var plan = new PlanBuilder();

    var orders = plan.Run<Order[]>("GetPendingOrders", new { request.Region });
    var validated = plan.Run<ValidationResult[]>("ValidateOrders", new { orders });
    var summary = plan.Run<FulfillmentSummary>("GenerateSummary", new { validated });

    return await client.RunAsync(plan, summary, ct);
});
```

- `plan.Run<T>("JobName", args)` returns a `Step<T>` — a lightweight handle, not a running task
- Passing a `Step<T>` in another step's arguments creates a dependency edge
- `client.RunAsync(plan, step, ct)` submits the graph and waits for the specified step to complete, returning its typed result
- Steps with no dependencies on each other run in parallel automatically
- `PlanBuilder` is a pure graph definition tool — zero async methods, no DI, no store access. `new PlanBuilder()` is all you need.

### Registering a plan

```csharp
app.AddPlan("FulfillOrders", (PlanBuilder plan, Step<string> region, Step<int> priority) =>
{
    var orders = plan.Run<Order[]>("GetPendingOrders", new { region, priority });
    var validated = plan.Run<ValidationResult[]>("ValidateOrders", new { orders });
    return plan.Run<FulfillmentSummary>("GenerateSummary", new { validated });
});
```

- Takes a `Delegate`, same pattern as `AddJob` — framework inspects params via `ParameterBinder`
- `PlanBuilder` is injected automatically
- `Step<T>` params become `InputStep` nodes in the graph, matched by name to trigger arguments
- The return value (`Step<T>`) marks which step's output becomes the plan's result. Return `void` or `Step` for no output.
- The lambda builds the graph but does NOT execute it — the engine drives execution
- **Static visibility**: the graph is computed at registration time for dashboard display
- Plans appear on the jobs page like any other job, but with a graph icon and a "View Plan" link
- Returns `JobBuilder` for `.WithCron()`, `.WithQueue()`, `.WithDescription()`, etc.

No input:

```csharp
app.AddPlan("DailyReport", (PlanBuilder plan) =>
{
    var data = plan.Run<Data>("FetchData");
    return plan.Run<Report>("GenerateReport", new { data });
});
```

No output:

```csharp
app.AddPlan("Cleanup", (PlanBuilder plan) =>
{
    var tmp = plan.Run<string>("CreateTmpFolder");
    var work = plan.Run("DoWork", new { tmp });
    plan.Run("DeleteTmpFolder", new { tmp }).DependsOn(work).WithTriggerRule(TriggerRule.AllDone);
});
```

Each `Step<T>` parameter creates an input node in the graph:

```json
{ "type": "input", "id": "input_region", "paramName": "region" }
```

At execution time, the engine resolves each input step from the trigger arguments by property name.

### Triggering and executing plans

```csharp
// Registered plan — fire and forget
await client.TriggerAsync("FulfillOrders", new { region = "US", priority = 5 }, ct);

// Registered plan — wait for single typed result
var summary = await client.RunAsync<FulfillmentSummary>("FulfillOrders", new { region = "US", priority = 5 }, ct);

// Registered plan — wait for completion, then query individual steps
var planRunId = await client.RunAsync("FulfillOrders", new { region = "US", priority = 5 }, ct);
var ordersRun = await client.GetStepRunAsync(planRunId, "get-orders", ct);
var orders = ordersRun!.Result.Deserialize<Order[]>();

// Ad-hoc plan — fire and forget
var plan = new PlanBuilder();
plan.Run("SendNotifications", new { users });
await client.TriggerAsync(plan, ct);

// Ad-hoc plan — wait for single result
var plan = new PlanBuilder();
var result = plan.Run<Summary>("Summarize", args);
var summary = await client.RunAsync(plan, result, ct);

// Ad-hoc plan — wait for completion, then query steps
var plan = new PlanBuilder();
var a = plan.Run<AResult>("JobA").WithName("a");
var b = plan.Run<BResult>("JobB", new { a }).WithName("b");
var planRunId = await client.RunAsync(plan, ct);
var aRun = await client.GetStepRunAsync(planRunId, "a", ct);
var bRun = await client.GetStepRunAsync(planRunId, "b", ct);
```

`new PlanBuilder()` works anywhere — inside job handlers, controllers, background services, startup code.

The builder constructs the graph. The client executes it. Execution is always through `IJobClient`:

- `client.TriggerAsync(plan, ct)` — fire and forget, returns plan run ID
- `client.RunAsync(plan, outputStep, ct)` — wait for single typed result
- `client.RunAsync(plan, ct)` — wait for all steps, returns plan run ID
- `client.TriggerAsync("Name", args, ct)` — fire and forget, returns run ID (same overload as jobs)
- `client.RunAsync<T>("Name", args, ct)` — wait for single typed result (same overload as jobs)
- `client.RunAsync("Name", args, ct)` — wait for completion, returns run ID (same overload as jobs)

Step results are queried via extension methods on `IJobClient`, backed by `RunFilter`:

- `client.GetStepRunAsync(planRunId, stepName, ct)` — single run (latest attempt)
- `client.GetStepRunsAsync(planRunId, stepName, ct)` — all attempts (retries)

These are convenience extension methods over `client.GetRunsAsync(RunFilter)` — no plan-specific methods on the `IJobClient` interface itself. See [Step result queries](#step-result-queries) for details.

**FailurePolicy** is set via `plan.WithFailurePolicy()` on the builder before execution.

---

## Step Types

### Run (execute a job, get typed result)

```csharp
var result = plan.Run<OrderResult>("ProcessOrder", new { orderId });
```

Creates a step that executes the named job and captures its typed result. Downstream steps that reference this handle won't execute until this step completes.

### Run (fire-and-forget within the plan)

```csharp
plan.Run("SendNotification", new { message });
```

No return type — the step runs but nothing depends on its result. The plan still waits for it to complete before the plan itself completes (all steps must finish).

`Step` (untyped) represents a step with no meaningful return value — conceptually `Step<void>`. It can be used with `DependsOn` for ordering, but cannot be passed as a data argument to another step.

### Ordering-only dependencies

```csharp
var migrated = plan.Run("MigrateSchema");
var seeded = plan.Run("SeedData").DependsOn(migrated);
var verified = plan.Run("Verify").DependsOn(migrated, seeded);
```

`.DependsOn()` creates an ordering edge without data flow. The downstream step doesn't receive the upstream step's result — it just waits for it to finish. Accepts one or more steps.

### Step configuration (fluent)

All step options are set via fluent methods on `Step<T>`, matching the `JobBuilder` pattern:

```csharp
var a = plan.Run<T>("SlowJob", args)
    .DependsOn(otherStep)
    .WithName("processing")
    .WithTriggerRule(TriggerRule.AllDone);
```

`Run` stays clean — just job name and args. Configuration is chainable and opt-in. Steps are mutable during graph construction but frozen once submitted to the client for execution.

### Stream (streaming step)

```csharp
var items = plan.Stream<Item>("FetchItems", new { source });
```

A streaming job that produces `IAsyncEnumerable<T>`. Returns `StreamStep<T>` — a subclass of `Step<T>` that marks the step as streaming. This type distinction lets the compiler resolve overloads between array and streaming sources.

**As a streaming parameter** — the downstream job receives `IAsyncEnumerable<T>` and processes items as they arrive. The downstream run is created when the stream step *starts*, not when it completes:

```csharp
var items = plan.Stream<Item>("FetchItems", args);
var summary = plan.Run<Summary>("Aggregate", new { items });
// Aggregate handler: async (IAsyncEnumerable<Item> items, CancellationToken ct) => ...
```

**As a ForEach/StreamForEach source** — each streamed item kicks off a new iteration:

```csharp
var items = plan.Stream<Item>("FetchItems", args);
var results = items.ForEach(item => plan.Run<Result>("Process", new { item }));
```

Both use Surefire's existing `IAsyncEnumerable` streaming infrastructure (Input/Output events, notification channels). The plan engine wires up the event channels between steps.

### ForEach (fan-out, collected results)

```csharp
var results = orders.ForEach(order =>
    plan.Run<ProcessResult>("ProcessOrder", new { order }));
```

Extension method on `Step<TItem[]>` and `StreamStep<TItem>`:

- **Array source** (`Step<TItem[]>`): all iterations start when the upstream step completes
- **Stream source** (`StreamStep<TItem>`): iterations spawn incrementally as items arrive
- The lambda receives a `Step<TItem>` representing each item
- Returns `Step<TResult[]>` — collected results from all iterations
- All iterations run in parallel (subject to the job's MaxConcurrency)
- Results are ordered to match the input order

**Action-only ForEach** (no collected results):

```csharp
orders.ForEach(order => plan.Run("SendEmail", new { order }));
```

Returns `Step` (untyped) — a completion signal you can `DependsOn` but has no result to extract. The body lambda returns `Step` instead of `Step<TResult>`.

### StreamForEach (fan-out, streamed results)

```csharp
var results = orders.StreamForEach(order =>
    plan.Run<ProcessResult>("ProcessItem", new { order }));
```

Same as `ForEach` but results stream out as iterations complete instead of being collected into an array. Returns `StreamStep<TResult>` — a downstream step can consume it as `IAsyncEnumerable<TResult>` or pipe it into another ForEach/StreamForEach.

Extension method on `Step<TItem[]>` and `StreamStep<TItem>` — same source flexibility as ForEach.

| | Array source (`Step<T[]>`) | Stream source (`StreamStep<T>`) |
|---|---|---|
| **ForEach** | All iterations start at once, collected into `Step<TResult[]>` | Iterations spawn incrementally, collected into `Step<TResult[]>` |
| **StreamForEach** | All iterations start at once, streamed as `StreamStep<TResult>` | Iterations spawn incrementally, streamed as `StreamStep<TResult>` |

### WhenAll (fan-in / collect)

```csharp
var a = plan.Run<Result>("JobA", args1);
var b = plan.Run<Result>("JobB", args2);
var c = plan.Run<Result>("JobC", args3);
var all = plan.WhenAll(a, b, c);  // Step<Result[]>
var summary = plan.Run<Summary>("Summarize", new { results = all });
```

Waits for all input steps and collects their results into an array. All inputs must be the same type. Useful for:

- **Bulk trigger**: run N independent jobs and collect results
- **Dynamic construction**: build a list of steps programmatically, then fan in

```csharp
// Dynamic — build steps in a loop
var steps = items.Select(item => plan.Run<Result>("Process", new { item })).ToList();
var collected = plan.WhenAll(steps);  // Step<Result[]>
```

**WhenAll vs ForEach**: ForEach declares one sub-graph template that's expanded at runtime. WhenAll collects already-declared steps. Use ForEach when all iterations run the same job(s); use WhenAll when steps are heterogeneous or dynamically constructed.

### ForEach with multi-step sub-graphs

```csharp
var shipments = orders.ForEach(order =>
{
    var inventory = plan.Run<InventoryHold>("ReserveInventory", new { order });
    var payment = plan.Run<PaymentResult>("ChargeCard", new { order });
    var shipment = plan.Run<Shipment>("CreateShipment", new { order, inventory, payment });
    plan.Run("SendConfirmation", new { order, shipment });
    return shipment;  // this step's result is collected
});
```

- The lambda body builds a sub-graph template (the builder enters "template mode" — steps created inside go into the template's `PlanGraph`, not the parent graph)
- `inventory` and `payment` run in parallel (no dependency between them)
- `shipment` waits for both (it references both handles)
- `return shipment` marks which step's result to collect into the output array
- `SendConfirmation` still runs (all steps must complete) but its result isn't collected

---

## Trigger Rules

By default, a step runs when all its dependencies succeed. `WithTriggerRule` changes this behavior — primarily for cleanup/finally patterns where a step must run regardless of upstream failures.

```csharp
public enum TriggerRule
{
    AllSuccess,   // default — all deps must succeed
    AllDone       // all deps must reach terminal state (success, failed, cancelled)
}
```

Future values (added as needed): `AllFailed`, `OneSuccess`, `OneFailed`, `NoneSkipped`.

### Dependency resolution with trigger rules

Dependencies are either **data dependencies** (step appears in `Arguments` as a `StepRefValue`) or **ordering dependencies** (step appears in `DependsOn` only).

- **Data dependencies always require success**, regardless of trigger rule. A failed step has no result value — there's nothing to pass.
- **Ordering dependencies** are governed by the trigger rule: `AllSuccess` requires the dep to succeed, `AllDone` only requires it to reach a terminal state.

This falls out naturally. The cleanup step gets its data from an early step that succeeded, and uses ordering + `AllDone` to wait for later steps that might fail:

```csharp
var tmpFolder = plan.Run<string>("CreateTmpFolder");
var pdf1 = plan.Run("GeneratePdf1", new { tmpFolder });
var pdf2 = plan.Run("GeneratePdf2", new { tmpFolder });
var upload = plan.Run("UploadPdfs", new { tmpFolder }).DependsOn(pdf1, pdf2);
var cleanup = plan.Run("DeleteTmpFolder", new { tmpFolder })
    .DependsOn(upload)
    .WithTriggerRule(TriggerRule.AllDone);
```

`cleanup` receives `tmpFolder` via data flow (succeeded — must have, or nothing else ran). It waits on `upload` via ordering. `AllDone` means it runs whether `upload` succeeded, failed, or was cancelled.

### Inspecting upstream outcomes at runtime

A step running with `AllDone` may want to know *why* it's running — did upstream succeed or fail? The job handler can inspect sibling step outcomes via `IJobClient` and `JobContext.PlanRunId`:

```csharp
app.AddJob("Cleanup", async (JobContext ctx, IJobClient client, string tmpFolder) =>
{
    var uploadRun = await client.GetStepRunAsync(ctx.PlanRunId!, "Upload");

    if (uploadRun?.Status == JobStatus.Failed)
        logger.Warn("Upload failed: {Error}, cleaning up anyway", uploadRun.Error);

    Directory.Delete(tmpFolder, recursive: true);
});
```

`JobContext.PlanRunId` is a nullable string — set when the job runs as a plan step, `null` otherwise. `GetStepRunAsync` is a convenience extension method over `RunFilter` — queries the store for a sibling step's run by name and returns the full `JobRun` with status, error, timing, result, etc. No plan-specific methods on `IJobClient`, no separate context type. This is where `WithName` pays off — lookup by human-readable name from inside a job handler.

---

## Data Flow

### How step references work

When you write `plan.Run<T>("Job", new { orders, region })`:

- `orders` is a `Step<Order[]>` — the engine records a dependency edge
- `region` is a plain `string` — passed through as a literal value

At execution time, when all upstream steps complete, the engine resolves step references to their actual results and constructs the argument object for the downstream job.

The job handler receives the resolved values:
```csharp
app.AddJob("ValidateOrders", (Order[] orders, string region) =>
{
    // orders is the actual Order[] from GetPendingOrders
    // region is the literal string from the plan
});
```

### Step reference detection

Arguments are inspected at plan construction time. Properties that are `Step<T>` instances create dependency edges. Properties that are plain values are serialized as literals. Combined with any explicit `.DependsOn()` edges, this produces the full dependency graph.

### Type safety

`Step<T>` carries the output type. When you pass a `Step<Order[]>` where a job expects `Order[]`, the types align at compile time.

For anonymous objects (`new { orders, region }`), the type checking happens at the job handler level — same as today with `TriggerAsync`. The plan doesn't add or remove type safety relative to the current model.

---

## Graph Model (C# types)

The plan graph is represented as typed C# classes that serialize to/from JSON using `System.Text.Json` polymorphic serialization.

### StepValue — the universal value type

All values in the plan graph — step arguments, per-step overrides, ForEach item references — use a single polymorphic type. This avoids magic marker keys (like `$step`) that could collide with user data.

```csharp
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(StepRefValue), "ref")]
[JsonDerivedType(typeof(StreamRefValue), "stream")]
[JsonDerivedType(typeof(ItemValue), "item")]
[JsonDerivedType(typeof(ConstantValue), "const")]
public abstract class StepValue { }

// Reference to another step's completed result
public sealed class StepRefValue : StepValue
{
    public required string StepId { get; set; }
}

// Reference to a streaming step (downstream run created on start, not completion)
public sealed class StreamRefValue : StepValue
{
    public required string StepId { get; set; }
}

// Reference to the current ForEach item
public sealed class ItemValue : StepValue { }

// A literal value, serialized inline
public sealed class ConstantValue : StepValue
{
    public required JsonElement Value { get; set; }
}
```

At execution time, the engine resolves each `StepValue`:
- `StepRefValue` → replaced with the referenced step's result JSON
- `StreamRefValue` → wires up event channels (downstream run starts when stream starts)
- `ItemValue` → replaced with the current ForEach iteration's element
- `ConstantValue` → used as-is

### Type hierarchy

```csharp
// The plan graph — contains all steps and metadata
public sealed class PlanGraph
{
    public string Version { get; set; } = "1";
    public required List<PlanStep> Steps { get; set; }
    public string? OutputStepId { get; set; }
    public StepFailurePolicy FailurePolicy { get; set; } = StepFailurePolicy.CancelDependents;
}

// Base step type with JSON polymorphism
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(InputStep), "input")]
[JsonDerivedType(typeof(RunStep), "run")]
[JsonDerivedType(typeof(StreamStep), "stream")]
[JsonDerivedType(typeof(ForEachStep), "forEach")]
[JsonDerivedType(typeof(WhenAllStep), "whenAll")]
public abstract class PlanStep
{
    public required string Id { get; set; }
    public string? Name { get; set; }  // human-readable, must be unique per plan
    public List<string> DependsOn { get; set; } = [];
    public TriggerRule TriggerRule { get; set; } = TriggerRule.AllSuccess;
}

public enum TriggerRule
{
    AllSuccess,
    AllDone
}

// A step that runs a named job
public sealed class RunStep : PlanStep
{
    public required string JobName { get; set; }
    public Dictionary<string, StepValue>? Arguments { get; set; }
}

// A step that runs a streaming job
public sealed class StreamStep : PlanStep
{
    public required string JobName { get; set; }
    public Dictionary<string, StepValue>? Arguments { get; set; }
}

// A plan input parameter (one per Step<T> param in the AddPlan delegate)
public sealed class InputStep : PlanStep
{
    public required string ParamName { get; set; }
}

// Collects multiple same-typed steps into an array
// NOT a job — the engine assembles results in-memory when all source steps complete
// Engine creates a synthetic run record to store the result (for step result access and dashboard visibility)
public sealed class WhenAllStep : PlanStep
{
    public required List<string> SourceStepIds { get; set; }
    // Builder automatically adds all SourceStepIds to DependsOn
}

// A step that fans out over a collection or stream
// No streaming flags — the engine derives behavior from context:
//   Source is a StreamStep in the graph? Subscribe to events instead of waiting for completion.
//   Consumer takes IAsyncEnumerable<T>? Stream results. Takes T[]? Buffer and pass.
public sealed class ForEachStep : PlanStep
{
    public required string SourceStepId { get; set; }
    public required PlanGraph Template { get; set; }  // sub-graph per item
    public string? CollectStepId { get; set; } // which template step's result to collect (null for action-only)
    // Builder automatically adds SourceStepId to DependsOn
}
```

### Step<T> and StreamStep<T> (construction-time handles)

```csharp
public class Step
{
    public virtual Step DependsOn(params Step[] steps) { ... return this; }
    public virtual Step WithName(string name) { ... return this; }
    public virtual Step WithTriggerRule(TriggerRule rule) { ... return this; }
}

public class Step<T> : Step
{
    public override Step<T> DependsOn(params Step[] steps) { ... return this; }
    public override Step<T> WithName(string name) { ... return this; }
    public override Step<T> WithTriggerRule(TriggerRule rule) { ... return this; }
}

public class StreamStep<T> : Step<T>
{
    public override StreamStep<T> DependsOn(params Step[] steps) { ... return this; }
    public override StreamStep<T> WithName(string name) { ... return this; }
    public override StreamStep<T> WithTriggerRule(TriggerRule rule) { ... return this; }
}
```

Uses covariant return types (C# 9+) — `override` with a more derived return type. No `new` hiding, so the correct return type is preserved regardless of how the reference is held. `Step` (untyped) is the base class — represents a step with no return value. `Step<T>` carries the output type for compile-time safety. `StreamStep<T>` marks a streaming output — lets the compiler resolve ForEach/StreamForEach overloads between array and streaming sources. `.DependsOn()` accepts `Step` (base) since it's ordering-only.

**Name vs Id**: `Id` is structural and auto-generated from position (`step_0`, `step_1`, `forEach_0_step_0`). Used internally for graph wiring, dependency resolution, and run correlation. `Name` is human-readable, set via `WithName()`, used for dashboard display and `client.GetStepRunAsync(planRunId, stepName)` lookup. Names must be unique per plan — validated at graph construction time. Across deployments, in-flight plans use the stored graph (immutable), so structural changes don't corrupt running plans.

**Name lookup scope**: `client.GetStepRunAsync(planRunId, "stepName")` resolves against top-level plan steps only. ForEach template steps have per-iteration IDs (e.g., `step_3[0].t_1`) — their template-level `WithName()` is for dashboard display, not for lookup. To inspect individual iteration results, query the ForEach step by name — its result is the collected array. Use `client.GetStepRunsAsync(planRunId, "forEachName")` to get all iteration runs.

### ForEach/StreamForEach extension methods

```csharp
// On Step<TItem[]> — array source
public static Step<TResult[]> ForEach<TItem, TResult>(this Step<TItem[]> source, Func<Step<TItem>, Step<TResult>> body);
public static Step ForEach<TItem>(this Step<TItem[]> source, Func<Step<TItem>, Step> body);
public static StreamStep<TResult> StreamForEach<TItem, TResult>(this Step<TItem[]> source, Func<Step<TItem>, Step<TResult>> body);

// On StreamStep<TItem> — streaming source
public static Step<TResult[]> ForEach<TItem, TResult>(this StreamStep<TItem> source, Func<Step<TItem>, Step<TResult>> body);
public static Step ForEach<TItem>(this StreamStep<TItem> source, Func<Step<TItem>, Step> body);
public static StreamStep<TResult> StreamForEach<TItem, TResult>(this StreamStep<TItem> source, Func<Step<TItem>, Step<TResult>> body);
```

No action-only `StreamForEach` — if there's no result, there's nothing to stream. Use the action-only `ForEach` instead.

### Serialized JSON examples

Run step with mixed arguments:

```json
{
  "type": "run",
  "id": "step_2",
  "name": "validation",
  "jobName": "ValidateOrders",
  "arguments": {
    "orders": { "type": "ref", "stepId": "step_1" },
    "region": { "type": "const", "value": "US" }
  },
  "dependsOn": ["step_1"],
  "triggerRule": "AllSuccess"
}
```

Cleanup step with AllDone trigger rule:

```json
{
  "type": "run",
  "id": "step_5",
  "name": "cleanup",
  "jobName": "DeleteTmpFolder",
  "arguments": {
    "tmpFolder": { "type": "ref", "stepId": "step_0" }
  },
  "dependsOn": ["step_0", "step_4"],
  "triggerRule": "AllDone"
}
```

Input steps (registered plan with `Step<string> region, Step<int> priority`):

```json
[
  { "type": "input", "id": "input_region", "paramName": "region" },
  { "type": "input", "id": "input_priority", "paramName": "priority" }
]
```

ForEach with sub-graph:

```json
{
  "type": "forEach",
  "id": "step_3",
  "sourceStepId": "step_2",
  "template": {
    "version": "1",
    "steps": [
      {
        "type": "run",
        "id": "t_1",
        "jobName": "ProcessOrder",
        "arguments": {
          "order": { "type": "item" }
        },
        "dependsOn": []
      },
      {
        "type": "run",
        "id": "t_2",
        "jobName": "SendConfirmation",
        "arguments": {
          "order": { "type": "item" },
          "result": { "type": "ref", "stepId": "t_1" }
        },
        "dependsOn": ["t_1"]
      }
    ],
    "outputStepId": "t_1"
  },
  "collectStepId": "t_1",
  "dependsOn": ["step_2"]
}
```

Future per-step override (dynamic priority from upstream step):

```json
{
  "type": "run",
  "id": "step_4",
  "jobName": "ProcessOrder",
  "arguments": {
    "order": { "type": "ref", "stepId": "step_3" }
  },
  "priority": { "type": "ref", "stepId": "step_2" },
  "dependsOn": ["step_2", "step_3"]
}
```

### Why this structure?

- **`StepValue` polymorphism**: Every value in the graph is explicitly typed via the `type` discriminator. No magic marker keys that could collide with user data. `System.Text.Json` handles serialization automatically.
- **`PlanGraph` is reused for ForEach templates**: A ForEach template is itself a graph (steps with dependencies). Recursive structure, single type.
- **`Dictionary<string, StepValue>` for arguments**: Each argument is explicitly a constant or a reference. The engine resolves all references at execution time to produce the final arguments for the job run.
- **`DependsOn` is always explicit**: Both data-flow dependencies (from `StepRefValue` in arguments) and ordering-only dependencies (from `.DependsOn()`) are merged into the `DependsOn` list. The engine doesn't distinguish — a dependency is a dependency. The distinction between data and ordering deps is derived from whether a step's `Arguments` contain a `StepRefValue` pointing to the dep.
- **Adding new step types is a new `JsonDerivedType`**: Conditionals, switch, sub-plans — each is a new subclass. No schema migration, no breaking changes to existing graphs.
- **Adding new value types is a new `JsonDerivedType` on `StepValue`**: Future needs add new subclasses without changing existing serialization.

### Engine processing

Finding the next steps to run:

```csharp
// After a step completes (or starts running), find newly unblocked steps
foreach (var step in graph.Steps.Where(s => !IsStarted(s.Id)))
{
    // Get arguments dictionary (only RunStep/StreamStep have arguments)
    var args = (step as RunStep)?.Arguments ?? (step as StreamStep)?.Arguments;

    bool unblocked = true;
    foreach (var depId in step.DependsOn)
    {
        var dep = GetStepRun(depId);
        bool isStreamDep = args?.Values.Any(v => v is StreamRefValue r && r.StepId == depId) == true;
        bool isDataDep = args?.Values.Any(v => v is StepRefValue r && r.StepId == depId) == true;

        if (isStreamDep)
        {
            // Stream deps require upstream to be running or completed
            // (Running = stream in progress, Completed = stream finished but events are replayable)
            if (dep.Status is not (JobStatus.Running or JobStatus.Completed)) { unblocked = false; break; }
        }
        else if (isDataDep)
        {
            // Data deps require upstream completion (need the result value)
            if (dep.Status != JobStatus.Completed) { unblocked = false; break; }
        }
        else
        {
            // Ordering deps governed by trigger rule
            if (step.TriggerRule == TriggerRule.AllDone)
            {
                if (!dep.Status.IsTerminal()) { unblocked = false; break; }
            }
            else
            {
                if (dep.Status != JobStatus.Completed) { unblocked = false; break; }
            }
        }
    }

    if (unblocked)
    {
        if (step is WhenAllStep whenAll)
        {
            // Engine-side aggregation — collect source results into array, no job execution
            var results = whenAll.SourceStepIds.Select(id => GetStepResult(id)).ToArray();
            CompleteStepWithResult(step.Id, results);
        }
        else
        {
            CreateStepRun(step);
        }
    }
}
```

For ForEach expansion, when the source step completes:

```csharp
if (step is ForEachStep forEach)
{
    var sourceResult = GetStepResult(forEach.SourceStepId);
    var items = sourceResult.Deserialize<JsonElement[]>();

    for (int i = 0; i < items.Length; i++)
    {
        // Stamp out a copy of the template with item-specific IDs
        // e.g., "t_1" becomes "step_3[0].t_1", "step_3[1].t_1", etc.
        var instanceSteps = InstantiateTemplate(forEach.Template, forEach.Id, i, items[i]);
        // Add to the runtime graph and create runs for root steps
    }
}
```

---

## Storage

### Schema

Plan columns on the `runs` table:

```sql
plan_run_id    TEXT    -- set on step runs, points to the plan run
plan_step_id   TEXT    -- internal graph ID (e.g., "step_2", "step_3[0].t_1")
plan_step_name TEXT    -- user-assigned name via WithName() (for GetRunAsync lookup)
plan_graph     JSONB   -- full graph definition (only set on plan runs, not step runs)

-- Step deduplication (reconciler idempotency)
UNIQUE INDEX ix_runs_plan_step ON runs (plan_run_id, plan_step_id) WHERE plan_run_id IS NOT NULL
```

Plan columns on the `jobs` table:

```sql
is_plan        BOOLEAN -- distinguishes plans from regular jobs
plan_graph     JSONB   -- static graph template for dashboard display (registered plans only)
```

No separate `plans` or `plan_steps` tables. A plan run IS a run. Step runs ARE runs. The relationships:

```
plan run (runs row, plan_graph set)
  ├── step run (plan_step_id = "step_1", plan_step_name = "validation")
  ├── step run (plan_step_id = "step_2", plan_step_name = "fulfillment")
  ├── step run (plan_step_id = "step_3[0].t_1", plan_step_name = null)
  ├── step run (plan_step_id = "step_3[1].t_1", plan_step_name = null)
  └── ...
```

### Registered plans

The graph template is stored on the job definition at registration time:

```
jobs table:
  name: "FulfillOrders"
  is_plan: true
  plan_graph: { "steps": [...], "outputStepId": "step_3" }
```

When triggered, the engine:
1. Loads the static graph template from the job definition
2. Creates a plan run (a row in `runs` with `plan_graph` = the template). The trigger arguments are stored as the plan run's `arguments` (same as any job run) — they are NOT embedded in the graph.
3. Creates step runs for root steps (no dependencies). Each `InputStep` resolves from the plan run's arguments by `ParamName`.
4. Advances on step completion

The graph is identical on the job definition and plan run — it's fully static, computed once at registration time. Since inputs are `Step<T>` handles (not real values), the graph structure doesn't depend on trigger arguments.

### Ad-hoc plans

No static graph — built dynamically at runtime. The `job_name` on the plan run is auto-derived:

- **Inside a job**: `"{ParentJobName}_plan_{runId}"` (derived from `JobContext.Current`, unique per plan)
- **Standalone** (controller, startup, etc.): `"_plan_{runId}"` (generated)

No job definition is created — ad-hoc plans don't appear in the jobs list, only in the runs list. Plan runs are identifiable by `plan_graph IS NOT NULL`.

```
runs table:
  id: "abc"
  job_name: "BatchProcess"        ← the parent job run

  id: "def"
  job_name: "BatchProcess_plan_def"   ← the plan run (auto-derived, unique per plan)
  parent_run_id: "abc"
  plan_graph: { ... }

  id: "ghi"
  job_name: "ProcessItem"         ← step run (actual job being executed)
  parent_run_id: "def"            ← trace tree: step → plan run
  plan_run_id: "def"              ← plan grouping
  plan_step_id: "step_1"
  plan_step_name: "validation"    ← from .WithName(), null if not set
```

Step runs have both `parent_run_id` and `plan_run_id` pointing to the plan run. `parent_run_id` gives us cascading cancellation and trace tree for free (cancel BatchProcess → cancels plan run → cancels step runs). `plan_run_id` is the explicit plan grouping column for plan-specific queries.

The plan run's `plan_graph` is the only place the graph exists. Constructed once at submission, persisted, immutable from there.

### Why JSON graph, not relational?

- **DAGs are document-shaped.** Modeling arbitrary graphs as rows + join tables is awkward, especially with nested ForEach templates (a sub-graph inside a graph).
- **Single read to get the full graph.** The engine loads it, finds unblocked steps, creates runs. No multi-table JOINs.
- **Schema-stable.** New step types (conditionals, switch, sub-plans) are new fields in the JSON — no migrations. Existing graphs remain valid.
- **Execution state is relational.** Step runs ARE runs — in the `runs` table with all existing infrastructure (heartbeats, claiming, retries, status tracking). The JSON is the plan; the runs are the execution.

### Concurrency control

When two steps complete simultaneously, the plan advancement logic must be serialized per plan to avoid double-creating steps:

- **PostgreSQL**: `pg_advisory_xact_lock(plan_run_id)` — same pattern as continuous job deduplication
- **In-memory**: `lock` per plan run ID

---

## Execution Model

### What happens when a plan is submitted

1. **Graph construction**: The plan builder has accumulated steps and dependencies into a `PlanGraph`
2. **Validation**: Cycle detection, missing job references, duplicate `WithName` values, type checks where possible
3. **Persistence**: The graph is serialized and stored as a plan run
4. **Root step creation**: Steps with no dependencies are created as pending job runs
5. **Wait** (`RunAsync` only): The caller awaits completion. `RunAsync<T>(plan, outputStep, ct)` returns as soon as the output step completes — the plan continues running in the background if other branches are still active. `RunAsync(plan, ct)` waits for all steps to reach terminal state.

### Step completion → plan advancement

When a step run completes, the plan engine advances the graph. This runs as part of the step completion callback in `JobExecutor`, not as a separate orchestrator:

1. Acquire advisory lock for the plan run
2. Load the plan graph from the plan run
3. Record the completed step's result
4. Find steps whose dependencies are now all satisfied (respecting trigger rules)
5. For `ForEachStep`: expand the template into N concrete step instances
6. For `StreamStep` consumers: the downstream run was already created at stream start — nothing to do
7. Create pending job runs for newly unblocked steps
8. If all steps are terminal: mark the plan run as completed (with the output step's result)

**No concurrency slot is held** for plan orchestration. The engine piggybacks on existing job completion infrastructure.

### Reconciler (durable recovery)

Event-driven advancement alone is insufficient. If a node dies after completing a step but before creating newly-unblocked steps, the plan stalls. A periodic reconciler scans active plan runs and recovers stalled plans:

1. Query plan runs in `Running` state
2. For each, load the graph and current step run statuses
3. Recompute which steps should be unblocked
4. Idempotently create any missing step runs (deduplicated by `plan_run_id` + `plan_step_id`)

This runs on the same schedule as other background maintenance (heartbeat checks, stale run recovery). It is not an optimization — it is a required correctness mechanism.

Engine-side steps (WhenAll, future LINQ operators) are evaluated inline during reconciliation — the reconciler computes their results directly rather than creating job runs for them.

### Streaming step execution

When a step depends on a streaming step via `StreamRefValue` (not `StepRefValue`):

1. The stream step is created as a pending run
2. When the stream step transitions to `Running`, the engine immediately creates the downstream run
3. The engine wires up event channels: stream step's Output events become the downstream run's Input events
4. The downstream run starts processing items via `IAsyncEnumerable<T>` parameter binding (existing infrastructure)
5. The downstream run completes independently of the stream step

For ForEach with a streaming source (source step is a `StreamStep` in the graph):
1. The engine subscribes to the stream step's Output events
2. Each emitted item stamps out a new template instance immediately
3. The ForEach completes when: the stream step is done AND all iterations are done

For StreamForEach (returns `StreamStep<TResult>`):
1. Results are emitted as Output events on the ForEach step's synthetic run as iterations complete
2. Downstream steps referencing the result via `StreamRefValue` receive results as `IAsyncEnumerable<TResult>`
3. Downstream steps referencing via `StepRefValue` get the buffered `TResult[]` after all iterations complete
4. Order is preserved via iteration index tagging

### Stream consumption parity

Streaming in plans uses the same parameter binding as regular jobs. The downstream handler's parameter type determines consumption:

- **`IAsyncEnumerable<T>`**: engine wires up event channels, items stream through as produced
- **`T[]`**: engine buffers the stream and passes the completed array

This works for both `StreamStep<T>` outputs (from `plan.Stream<T>()` or `StreamForEach`) and regular `Step<T[]>` outputs. The engine handles materialization transparently based on what the handler declares — same as `ParameterBinder` does for regular job-to-job streaming today.

### Result access

`Step<T>` is a pure construction handle — no result or status properties. Post-execution access uses convenience extension methods backed by `RunFilter`. No dedicated result type.

### Step result queries

Step results are queried via extension methods on `IJobClient`. These are thin wrappers over `client.GetRunsAsync(RunFilter)` — no plan-specific methods on the interface:

```csharp
public static class PlanExtensions
{
    public static async Task<JobRun?> GetStepRunAsync(
        this IJobClient client, string planRunId, string stepName, CancellationToken ct = default)
    {
        var result = await client.GetRunsAsync(new RunFilter
        {
            PlanRunId = planRunId,
            PlanStepName = stepName,
            Take = 1
        }, ct);
        return result.Items.FirstOrDefault();
    }

    public static async Task<IReadOnlyList<JobRun>> GetStepRunsAsync(
        this IJobClient client, string planRunId, string stepName, CancellationToken ct = default)
    {
        var result = await client.GetRunsAsync(new RunFilter
        {
            PlanRunId = planRunId,
            PlanStepName = stepName
        }, ct);
        return result.Items;
    }
}
```

`RunFilter` gains two nullable fields for plan queries (needed by the dashboard API anyway):

```csharp
public string? PlanRunId { get; set; }
public string? PlanStepName { get; set; }
```

Usage:

```csharp
// Wait for plan completion, get the plan run ID
var planRunId = await client.RunAsync(plan, ct);

// Query a step's run by name
var run = await client.GetStepRunAsync(planRunId, "validation", ct);
// run.Status, run.Error, run.StartedAt, run.CompletedAt, run.Result

// Deserialize the result
var orders = run!.Result.Deserialize<Order[]>();

// All retry attempts for a step
var runs = await client.GetStepRunsAsync(planRunId, "risky-step", ct);
// runs[0] — first try (failed)
// runs[1] — retry (completed)
```

`GetStepRunAsync` returns the full `JobRun` which already has `Status`, `Error`, `StartedAt`, `CompletedAt`, `Result`, etc. `GetStepRunsAsync` returns all attempts for a step (including retries), ordered by attempt.

**ForEach results**: The ForEach step's collected result is an array assembled by the engine and stored on the ForEach step's synthetic run:

```csharp
var items = orders.ForEach(order => plan.Run<Result>("Process", new { order })).WithName("processing");
var planRunId = await client.RunAsync(plan, ct);
var run = await client.GetStepRunAsync(planRunId, "processing", ct);
var allResults = run!.Result.Deserialize<Result[]>();
```

---

## Error Handling

### Default: CancelDependents

When a step fails (after exhausting its own retry policy), downstream steps that depend on it are cancelled. Steps on independent branches continue running. The plan fails when all steps reach terminal state and at least one has failed.

```
GetOrders ──→ ValidateOrders ──→ FulfillOrders
                                      ↑ (cancelled — upstream failed)
           ──→ NotifyOps (independent branch — continues running)
```

Steps with `TriggerRule.AllDone` are not cancelled — they run when their deps terminate regardless of outcome.

### Failure policies

Set on the plan builder:

```csharp
var plan = new PlanBuilder();
plan.WithFailurePolicy(StepFailurePolicy.CancelAll);

// Or inside a registered plan
app.AddPlan("MyPlan", (PlanBuilder plan, Step<string> region) =>
{
    plan.WithFailurePolicy(StepFailurePolicy.ContinueAll);
    // ...
});
```

- **CancelDependents**: Cancel steps that depend (directly or transitively) on the failed step. Others continue. Steps with `TriggerRule.AllDone` still run.
- **CancelAll**: Cancel all non-terminal steps immediately. Plan fails fast. Steps with `TriggerRule.AllDone` still run.
- **ContinueAll**: Let all steps run regardless of failures. Plan completes with mixed results. Useful for batch processing where partial success is acceptable.

### Retry

Individual steps retry per their job's retry policy (the existing mechanism). The plan doesn't add its own retry layer. If a step exhausts its retries, the failure policy determines what happens to the rest of the plan.

### Future: ForEach partial failure handling (v2)

In v1, ForEach returns `Step<TResult[]>` — if any iteration fails, the failure policy applies (default: cancel dependents).

In v2, a richer API could lift iteration results into run metadata, enabling filtering by outcome before collecting:

```csharp
// Hypothetical — lift each iteration into a Step<Run<T>> for outcome-aware processing
orders.ForEach(order => plan.Run<Receipt>("ProcessOrder", new { order }))
    .AsRuns()
    .Where(run => run.Status == RunStatus.Completed)
    .AsResults()
    .Select(r => r.Customer.EmailAddress)
    .ForEach(email => plan.Run("SendEmail", new { email }));
```

This composes with LINQ operators and avoids a special `ContinueAll` policy. Design TBD.

### Future: Per-step fallbacks (v2)

```csharp
var result = plan.Run<T>("Primary", args).WithFallback("FallbackJob");
```

If the primary step fails after all retries, the engine runs the fallback job instead and uses its result. Transparent to downstream steps.

---

## Future Extensions (v2+, no API breaks)

These are designed to fit within the existing graph model by adding new `PlanStep` subclasses and `JsonDerivedType` entries. No changes to the core engine loop.

### If (conditional branching)

```csharp
var isApproved = plan.Run<bool>("CheckApproval", new { order });

plan.If(isApproved, plan.Run("SendReceipt", new { order }));

var result = plan.If(isApproved,
    then: plan.Run<Receipt>("ProcessPayment", new { order }),
    @else: plan.Run<Receipt>("RejectOrder", new { order }));
```

The condition is a `Step<bool>` — a regular job that returns true/false. The engine reads the bool result and activates the matching branch. The other branch gets status `Skipped`.

### Switch (branch on string)

```csharp
var orderType = plan.Run<string>("ClassifyOrder", new { order });
var result = plan.Switch(orderType,
    ("standard", plan.Run<Receipt>("StandardProcess", args)),
    ("express",  plan.Run<Receipt>("ExpressProcess", args)),
    ("bulk",     plan.Run<Receipt>("BulkProcess", args)));
```

### Sub-plans (plan composition)

```csharp
var subResult = plan.RunPlan<T>("SubPlanName", new { input });
```

A step that triggers another registered plan and waits for its result.

### Manual approval / wait steps

```csharp
var approval = plan.WaitForSignal<ApprovalDecision>("manager-approval");
```

A step that pauses until an external signal is received (via dashboard or API).

### Per-step overrides

Step fluent methods accepting both static literals and `Step<T>` handles for dynamic, runtime-computed values:

```csharp
step.WithPriority(100);               // static
step.WithPriority(priorityStep);       // Step<int> — dynamic
step.NotBefore(scheduledTimeStep);     // Step<DateTimeOffset>
step.WithDeduplicationId(dedupStep);   // Step<string>
```

### Optional steps

```csharp
plan.Run<Result>("RiskyJob", args).Optional();
```

An optional step doesn't trigger the failure policy. Downstream steps that depend on it receive `null` for its result.

### Internal jobs

```csharp
app.AddJob("_IsLargeOrder", (int count) => count > 100)
    .Internal();
```

Jobs marked as internal are hidden from the dashboard jobs list. Useful for lightweight glue logic in plans. Dashboard `GET /jobs` filters them out by default, with `?includeInternal=true` for debugging.

### Engine-native LINQ operators

Extension methods on `Step<T[]>` that translate to engine-native step types. NOT jobs — the engine evaluates them directly during plan advancement, like how EF Core translates LINQ to SQL. No job registration, no scheduling overhead, no cross-node discovery.

```csharp
var orders = plan.Run<Order[]>("FetchOrders");
var validated = plan.Run<ValidatedOrder[]>("ValidateOrders", new { orders });

var totals = validated
    .Where(o => o.IsValid)
    .Select(o => o.Total);

var report = plan.Run<Report>("GenerateReport", new { totals });
```

Supported operators:

```csharp
Step<T[]>     step.Where(Expression<Func<T, bool>> predicate);
Step<TOut[]>  step.Select<TOut>(Expression<Func<T, TOut>> selector);
Step<int>     step.Count();
Step<TOut>    step.Sum/Min/Max<TOut>(Expression<Func<T, TOut>> selector);
Step<T>       step.First/FirstOrDefault(Expression<Func<T, bool>> predicate);
Step<bool>    step.Any/All(Expression<Func<T, bool>> predicate);
Step<T[]>     step.OrderBy/OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector);
```

Expression trees are converted to a serializable `OperatorExpression` model (member access, comparisons, logical operators, constants). The engine supports a fixed set of operators — complex logic should use a registered job.

---

## Dashboard Visualization

### Registered plans: static graph view

For plans registered with `app.AddPlan()`, the graph is known at registration time. The dashboard can render it without executing the plan:

- Job detail page shows a "Plan" tab with the step graph
- Nodes are steps (labeled with step name or job name)
- Edges show data dependencies and ordering dependencies
- ForEach nodes show a "dynamic fan-out" indicator
- Stream steps show a streaming indicator

### Plan run: runtime graph view

When a plan is executing (or has completed), the dashboard shows the actual execution:

- Steps colored by status (pending/running/completed/failed/skipped)
- ForEach nodes expanded to show actual N iterations (collapsible for large fan-outs)
- Timing info on each step (started at, duration)
- Click a step to navigate to its run detail page
- Failed steps highlighted with error info
- Streaming steps show item count and throughput
- Steps with `TriggerRule.AllDone` marked with a "finally" indicator

### Plan versioning during deployment

The dashboard's static graph view shows the current registered version. The runtime graph view shows what's actually executing (the graph frozen at submission time). If a plan's graph changes between deployments, in-flight plan runs use the old graph — their runtime view may differ from the static registered view. This is expected and correct: the plan run is immutable once submitted.

### Layout

Libraries like dagre (JavaScript) handle DAG layout well. The graph structure is simple enough for automatic layout:

- Linear chains: top to bottom or left to right
- Parallel branches: side by side
- Fan-out/fan-in: one node splitting into many, converging back
- ForEach sub-graphs: grouped or indented, collapsible

---

## Design Decisions

### Why "Plan" and not "Workflow"?

- "Workflow" is overloaded — every library uses it differently
- "Plan" conveys deferred execution (it's a plan, not yet running)
- `new PlanBuilder()` and `app.AddPlan()` read naturally
- Short, distinct, memorable

### Why deferred execution instead of procedural (Temporal-style)?

- **No wasted slots**: procedural orchestrators hold a concurrency slot while waiting for children
- **Static visibility**: the graph is inspectable before execution
- **No replay complexity**: Temporal requires deterministic code — plans have no such constraint
- **Tradeoff**: less flexible than arbitrary async code, but conditionals and ForEach cover the common patterns

### Why not separate batch and continuation APIs?

A batch is a plan with a single ForEach. A continuation is a plan with sequential steps. One API handles both, plus DAGs, fan-out/fan-in, and everything in between:

```csharp
// Batch
var plan = new PlanBuilder();
orders.ForEach(order => plan.Run("ProcessItem", new { order }));
await client.RunAsync(plan, ct);

// Continuation
var plan = new PlanBuilder();
var a = plan.Run<AResult>("StepA", args);
var b = plan.Run<BResult>("StepB", new { a });
await client.RunAsync(plan, b, ct);
```

### Why not in-memory tasks/delegates?

In a distributed durable system, in-memory delegates don't survive node failures or deployments. If node A builds a plan with delegates and crashes, node B picks up the plan but has no delegates. Named jobs are durable, distributed, independently testable, and survive deploys. The graph JSON is fully self-contained — any node can advance any plan.

### Why `new PlanBuilder()` instead of `client.CreatePlan()`?

`PlanBuilder` is a pure graph definition tool — zero async methods, no DI, no store access. It builds a data structure. There's no reason to couple it to `IJobClient`. `new PlanBuilder()` is honest about what it is: a POCO builder with no hidden dependencies. The client only matters at execution time.

### Why builder constructs, client executes?

The `PlanBuilder` is a pure graph definition tool with zero async methods. The `IJobClient` owns execution (`TriggerAsync`, `RunAsync`). Post-execution step queries use extension methods backed by `RunFilter` — no plan-specific methods on the `IJobClient` interface. This separation keeps concerns clean — the builder doesn't need access to the store or execution infrastructure. It also means plan construction is deterministic, testable, and serializable without any IO.

### Why TriggerRule enum instead of sugar methods?

`Always()`, `Never()`, `Sometimes()` etc. are not descriptive of what they're tied to. An enum (`TriggerRule.AllDone`) is explicit, extensible (new values don't require new methods), and matches industry terminology (Airflow's `trigger_rule`). One fluent method (`.WithTriggerRule()`) covers all current and future trigger conditions.

### Why ForEach/StreamForEach as step extension methods?

`orders.ForEach(order => ...)` reads better than `plan.ForEach(orders, order => ...)` — you're operating on the collection, not asking the plan to do it. Same logic applies to v2 LINQ operators: `step.Where(x => ...)` is better than `plan.Where(step, x => ...)`. Step methods for transforming/iterating, plan methods for constructing new steps.

### Why `StreamStep<T>` as a separate type?

`plan.Stream<T>()` returns `StreamStep<T>` instead of `Step<T>` so the compiler can distinguish streaming sources from array values. Without this, `Step<string>` from `plan.Run<string>()` and `Step<string>` from `plan.Stream<string>()` would be ambiguous for ForEach overloads. `StreamStep<T>` inherits from `Step<T>` so it works everywhere a `Step` does.

### Why ForEach vs StreamForEach naming?

The distinction is about **output**, not input. Both accept array or streaming sources. `ForEach` collects results into `Step<TResult[]>`. `StreamForEach` streams results as `StreamStep<TResult>`. The source type (array vs stream) determines when iterations spawn; the method name determines how results come out.

### Why engine-native LINQ operators instead of auto-registered inline jobs?

Turning lambdas into auto-registered internal jobs has a cross-node discovery problem: in a distributed system, a node can only execute a job it has registered. Inline delegates in job handlers are only registered when that handler runs — other nodes may not have the registration when they claim a step run.

Engine-native operators avoid this entirely. `Where`, `Select`, `Sum` etc. are evaluated directly by the plan engine during advancement — no job runs, no registration, no scheduling. The expression tree is serialized into the graph as an `OperatorExpression` (a fixed set of supported operations: member access, comparisons, logical operators, constants). Any node can evaluate any operator because the logic is in the engine, not in user-registered code.

This mirrors EF Core's approach: LINQ expressions are translated to SQL (an internal representation the engine knows how to execute), not delegated to user code. Complex operations that exceed the operator set use registered jobs — explicit, discoverable, distributed.

### Plan identity: plan as a job type

Named plans use `JobDefinition.IsPlan = true`. This reuses existing infrastructure — triggering, scheduling, cron, queue assignment, rate limiting, enable/disable — without duplicating it. Plans appear alongside jobs in the dashboard.

---

## Edge Cases

### Circular dependencies
Detected at plan construction time (before execution). Throws immediately.

### Empty ForEach (zero items)
The ForEach step completes with an empty array result. Downstream steps receive `[]`.

### Step referencing itself
Impossible by construction — you can't pass a step's own handle to itself because the handle doesn't exist yet.

### Plan with no steps
`client.RunAsync(plan, ct)` completes immediately and returns the plan run ID.

### Very large fan-out (10,000+ items)
ForEach creates N job runs, subject to the job's MaxConcurrency and queue rate limits. The engine creates runs in configurable batches, not all at once.

### Nested ForEach
A ForEach inside a ForEach lambda creates nested fan-out. Each outer item spawns an inner fan-out. Step IDs encode the nesting: `"step_3[0].step_4[2].t_1"`.

### Long-running plans (hours/days)
Plans are durable — they survive node restarts. Step runs are regular job runs with heartbeats and recovery. The plan's graph state is persisted in the store.

### Plan cancellation
Cancelling a plan (or the caller's `CancellationToken`) cancels all non-terminal step runs. Uses the existing cascading cancellation mechanism.

### ForEach ordering
Results are guaranteed to match input order. Each iteration is tagged with its index, and the collected result array is assembled in order.

### Node crash mid-advancement
Advisory lock (PostgreSQL) or in-memory lock serializes plan advancement per plan run. If a node crashes while advancing, the lock is released and another node picks it up via the reconciler. Idempotent step creation (deduplication by `plan_run_id` + `plan_step_id`) prevents duplicate runs.

### Ad-hoc plan run identity
Ad-hoc plan runs need a `job_name` (NOT NULL). The engine auto-derives it: `"{ParentJobName}_plan_{runId}"` inside a job (from `JobContext.Current`, unique per plan), or `"_plan_{runId}"` for standalone plans. No job definition is created — ad-hoc plans don't appear in the jobs list, only in the runs list. Plan runs are identifiable by `plan_graph IS NOT NULL`.

### RunAsync with failed output step
`client.RunAsync<T>(plan, outputStep, ct)` throws a `JobFailedException` if the output step fails (after exhausting retries). The exception contains the step's error. Same behavior as `client.RunAsync<T>("JobName", args, ct)` for regular jobs — the caller gets an exception, not a default value.

### AllDone steps with failed data dependencies
If a step has `TriggerRule.AllDone` but also has a data dependency (via `StepRefValue` in arguments) on a step that failed, it cannot run — the failed step has no result to pass. This is correct: you can't clean up a resource that was never created. Use ordering-only `DependsOn` for deps that might fail, and data flow only for deps that must succeed.

---

## API Surface Summary

### v1

```csharp
// Construction
new PlanBuilder()

// PlanBuilder — new steps (zero async methods)
Step<T>           plan.Run<T>(string jobName, object? args = null);
Step              plan.Run(string jobName, object? args = null);
StreamStep<T>     plan.Stream<T>(string jobName, object? args = null);
Step<T[]>         plan.WhenAll<T>(params Step<T>[] steps);
Step<T[]>         plan.WhenAll<T>(IEnumerable<Step<T>> steps);
void              plan.WithFailurePolicy(StepFailurePolicy policy);

// Step<T[]> — fan-out from arrays
Step<TResult[]>   step.ForEach<TItem, TResult>(Func<Step<TItem>, Step<TResult>> body);
Step              step.ForEach<TItem>(Func<Step<TItem>, Step> body);
StreamStep<TResult> step.StreamForEach<TItem, TResult>(Func<Step<TItem>, Step<TResult>> body);

// StreamStep<T> — fan-out from streams
Step<TResult[]>   step.ForEach<TItem, TResult>(Func<Step<TItem>, Step<TResult>> body);
Step              step.ForEach<TItem>(Func<Step<TItem>, Step> body);
StreamStep<TResult> step.StreamForEach<TItem, TResult>(Func<Step<TItem>, Step<TResult>> body);

// Step<T> — fluent configuration
Step<T>           step.DependsOn(params Step[] steps);
Step<T>           step.WithName(string name);
Step<T>           step.WithTriggerRule(TriggerRule rule);

// IJobClient — execution (ad-hoc plans)
Task<T>           client.RunAsync<T>(PlanBuilder plan, Step<T> outputStep, CancellationToken ct);
Task<string>      client.RunAsync(PlanBuilder plan, CancellationToken ct);  // returns plan run ID
Task<string>      client.TriggerAsync(PlanBuilder plan, CancellationToken ct);  // returns plan run ID

// IJobClient — execution (registered plans, same overloads as jobs)
Task<T>           client.RunAsync<T>(string name, object? args, CancellationToken ct);
Task<string>      client.RunAsync(string name, object? args, CancellationToken ct);  // returns run ID
Task<string>      client.TriggerAsync(string name, object? args, CancellationToken ct);  // returns run ID

// IJobClient — general-purpose run query (new, plan-agnostic)
Task<PagedResult<JobRun>>   client.GetRunsAsync(RunFilter filter, CancellationToken ct);

// RunFilter — new fields for plan queries
string? filter.PlanRunId
string? filter.PlanStepName

// Extension methods — convenience wrappers over RunFilter (not on IJobClient interface)
Task<JobRun?>               client.GetStepRunAsync(string planRunId, string stepName, CancellationToken ct);
Task<IReadOnlyList<JobRun>> client.GetStepRunsAsync(string planRunId, string stepName, CancellationToken ct);

// Registration (returns JobBuilder for .WithCron(), .WithQueue(), etc.)
JobBuilder app.AddPlan(string name, Delegate planBuilder);

// JobContext — plan-aware properties
string? ctx.PlanRunId    // set when running as a plan step, null otherwise

// Enums
enum TriggerRule { AllSuccess, AllDone }
enum StepFailurePolicy { CancelDependents, CancelAll, ContinueAll }
```

### v2+

```csharp
// Engine-native LINQ operators (extension methods on Step<T[]>)
Step<T[]>     step.Where(Expression<Func<T, bool>> predicate);
Step<TOut[]>  step.Select<TOut>(Expression<Func<T, TOut>> selector);
Step<int>     step.Count();
Step<TOut>    step.Sum/Min/Max<TOut>(Expression<Func<T, TOut>> selector);
Step<T>       step.First/FirstOrDefault(Expression<Func<T, bool>> predicate);
Step<bool>    step.Any/All(Expression<Func<T, bool>> predicate);
Step<T[]>     step.OrderBy/OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector);

// Conditionals
Step<T>  plan.If<T>(Step<bool> condition, Step<T> then, Step<T>? @else = null);
Step<T>  plan.Switch<T>(Step<string> key, params (string value, Step<T> branch)[] branches);

// Composition
Step<T>  plan.RunPlan<T>(string planName, object? args);
Step<T>  plan.WaitForSignal<T>(string signalName);

// Per-step overrides (static or dynamic via Step<T>)
Step<T>  step.WithPriority(int | Step<int>);
Step<T>  step.NotBefore(DateTimeOffset | Step<DateTimeOffset>);
Step<T>  step.WithDeduplicationId(string | Step<string>);
Step<T>  step.Optional();
Step<T>  step.WithFallback(string fallbackJobName);

// ForEach partial failure (design TBD — AsRuns/AsResults composition)
Step<Run<TResult>[]> step.ForEach(...).AsRuns();  // lift iterations to run metadata

// Internal jobs
JobBuilder.Internal();

// Additional trigger rules
enum TriggerRule { AllSuccess, AllDone, AllFailed, OneSuccess, OneFailed, NoneSkipped }
```

---

## Implementation Phases

### Phase 1: Core plan engine
- `StepValue` type hierarchy with `JsonDerivedType` serialization
- `PlanStep` type hierarchy with `JsonDerivedType` serialization (v1 types only)
- `PlanGraph` model with version field
- `PlanBuilder` with `Run<T>()`, `Run()`, `WhenAll()`, `WithFailurePolicy()`
- `Step<T>`, `StreamStep<T>`, `Step` handle types with `DependsOn()`, `WithName()`, `WithTriggerRule()`
- `ForEach`/`StreamForEach` extension methods on `Step<T[]>` and `StreamStep<T>`
- `TriggerRule` enum (`AllSuccess`, `AllDone`)
- `JobContext.PlanRunId` for plan-aware job handlers
- Plan execution via `IJobClient` (`RunAsync`, `TriggerAsync` overloads for `PlanBuilder`)
- `GetRunsAsync(RunFilter)` on `IJobClient` (general-purpose, exposes existing store query)
- `PlanRunId` + `PlanStepName` fields on `RunFilter`
- `GetStepRunAsync` / `GetStepRunsAsync` extension methods on `IJobClient`
- Plan columns on runs table (`plan_graph`, `plan_run_id`, `plan_step_id`, `plan_step_name`)
- Step completion → advance plan logic (with advisory lock serialization)
- Reconciler: periodic scan of active plan runs, recompute unblocked steps, idempotently create missing step runs
- Graph validation: cycle detection, duplicate names, missing job references
- Basic dashboard: plan runs show step list with status

### Phase 2: Streaming + registered plans
- `Stream<T>()` step type with event channel wiring
- Streaming source ForEach (incremental fan-out)
- StreamForEach streaming output
- `app.AddPlan()` registration with `Delegate` and `ParameterBinder`
- `InputStep` with `ParamName` for multi-param plans
- `is_plan` + `plan_graph` on jobs table
- Registered plan execution via `IJobClient` (`RunAsync<T>`, `RunAsync`, `TriggerAsync` by name)
- Dashboard plan graph view (dagre layout)
- Plan run graph view with live status updates

### Phase 3: LINQ operators + advanced features
- Engine-native LINQ operators (`Where`, `Select`, `Count`, `Sum`, `Any`, `All`, `OrderBy`, etc.)
- `OperatorExpression` serializable expression tree model
- Expression tree → `OperatorExpression` visitor/converter
- `If()` conditionals, `Switch()` branching
- `RunPlan()` sub-plan composition
- Per-step overrides with `StepValue` (static + dynamic)
- `Optional()` steps, `WithFallback()` per-step fallbacks
- Internal jobs (`JobBuilder.Internal()`)
- `WaitForSignal()` for human approval flows
- ForEach partial failure handling (`AsRuns`/`AsResults` composition — design TBD)
- Additional trigger rules
