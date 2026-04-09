# Surefire API v2 Design

## Goals

- Collapse 58+ `IJobClient` overloads into a clean primitive/sugar split
- Introduce `JobRun` and `JobBatch` as unified public concepts (snapshot data + interaction)
- Make `StreamEventsAsync` the foundational primitive everything else composes from
- Keep a single service to inject: `IJobClient` handles triggering, querying, and control
- No breaking surface: not yet published, so rename freely

---

## Naming Decisions

| Concept | Public Name | Notes |
|---|---|---|
| Client-facing run | `JobRun` | Unified: data properties + interaction methods |
| Client-facing batch | `JobBatch` | New; batch data + interaction methods |
| Batch input item | `BatchItem` | Record; replaces tuple `(string, object?)` |
| Internal DB entity | `RunRecord` | Renamed from `JobRun`; never public |
| Run scheduling options | `RunOptions` | Existing; add `RunId` property |

`JobRun` returned from `TriggerAsync` has all fields known at submission time populated — it is not a
sparse handle. `JobRun` returned from `GetRunAsync` reflects current DB state including live fields.
Same type — users don't need to distinguish. Properties are always valid for what the originating
operation provided, like `FileInfo` vs `FileInfo.Refresh()`.

---

## IJobClient

One service to inject. Handles triggering, running, streaming, querying, and control.

```csharp
public interface IJobClient
{
    // -------------------------------------------------------------------------
    // Primitives — return handles for full control
    // -------------------------------------------------------------------------

    /// <summary>Submits a run and returns a handle. Fire-and-forget or interact via the handle.</summary>
    Task<JobRun> TriggerAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Submits a heterogeneous batch atomically. Per-item options override shared defaults.</summary>
    Task<JobBatch> TriggerManyAsync(IEnumerable<BatchItem> runs, RunOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Fan-out: submits the same job N times atomically.</summary>
    Task<JobBatch> TriggerManyAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    // -------------------------------------------------------------------------
    // Sugar — common cases built on top of primitives
    // -------------------------------------------------------------------------

    /// <summary>Triggers and waits for a typed result. Throws on failure.</summary>
    Task<T> RunAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Triggers and waits for completion (no result). Throws on failure.</summary>
    Task RunAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Triggers and streams output values as the job produces them.</summary>
    IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Fan-out: triggers and waits for all results. Throws on any failure.</summary>
    Task<T[]> RunManyAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Fan-out: triggers and waits for all runs to complete (no result). Throws on any failure.</summary>
    Task RunManyAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Fan-out: triggers and streams output values from all runs as they are produced.</summary>
    IAsyncEnumerable<T> StreamManyAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    // -------------------------------------------------------------------------
    // Query / rehydration — by ID, for observability and cross-process access
    // -------------------------------------------------------------------------

    Task<JobRun?>            GetRunAsync(string runId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<JobRun> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default);
    Task<JobBatch?>          GetBatchAsync(string batchId, CancellationToken cancellationToken = default);

    // -------------------------------------------------------------------------
    // Control — by ID, for when you only have the string (e.g., from logs)
    // -------------------------------------------------------------------------

    Task         CancelAsync(string runId, CancellationToken cancellationToken = default);
    Task         CancelBatchAsync(string batchId, CancellationToken cancellationToken = default);
    Task<JobRun> RerunAsync(string runId, CancellationToken cancellationToken = default);
}
```

`WaitAsync`, `GetBatchAsync`, and `GetRunAsync` are the rehydration paths when you only have an ID
string. `GetRunAsync(id)` gives you a live handle you can call `WaitAsync()` on — no separate
`client.WaitAsync(runId)` overloads are needed.
`TriggerManyAsync` with `RunOptions.RunId` set at the batch level is a validation error — a single
run ID cannot be shared across N runs.

### Overload consolidation rationale

The existing 58+ overloads collapse to 15 interface members. Overloads that varied only by
`CancellationToken` or presence/absence of `args`/`options` are replaced with optional parameters.
`RunOptions` is the extensibility escape hatch — new per-call options go there, not on the interface.

---

## JobRun

Unified concept: snapshot data + interaction. Returned by `TriggerAsync`, `GetRunAsync`,
`RerunAsync`, and `JobBatch.StreamRunsAsync`.

```csharp
public sealed class JobRun
{
    // ------------------------------------------------------------------
    // Immutable — set at trigger time, never change
    // ------------------------------------------------------------------
    public string          Id              { get; init; }
    public string          JobName         { get; init; }
    public DateTimeOffset  CreatedAt       { get; init; }
    public DateTimeOffset  NotBefore       { get; init; }
    public DateTimeOffset? NotAfter        { get; init; }
    public int             Priority        { get; init; }
    public string?         DeduplicationId { get; init; }
    public string?         Arguments       { get; init; }  // serialized; useful for debugging/dashboard
    public string?         Result          { get; internal set; }  // serialized result JSON; null until completed
    public ActivityTraceId TraceId         { get; init; }  // default = no trace
    public ActivitySpanId  SpanId          { get; init; }  // default = no span
    public string?         BatchId         { get; init; }  // set if this run is part of a batch
    public string?         ParentRunId     { get; init; }  // set if spawned by another job (non-batch)
    public string?         RootRunId       { get; init; }
    public string?         RerunOfRunId    { get; init; }

    // ------------------------------------------------------------------
    // Live — updated in-place atomically as the run progresses
    // ------------------------------------------------------------------
    public JobStatus        Status          { get; internal set; }
    public double           Progress        { get; internal set; }
    public string?          Error           { get; internal set; }
    public int              Attempt         { get; internal set; }
    public DateTimeOffset?  StartedAt       { get; internal set; }
    public DateTimeOffset?  CompletedAt     { get; internal set; }
    public DateTimeOffset?  CancelledAt     { get; internal set; }
    public DateTimeOffset?  LastHeartbeatAt { get; internal set; }
    public string?          NodeName        { get; internal set; }  // set when a worker picks up the run

    // ------------------------------------------------------------------
    // Interaction — always live, delegate to IJobClient internally
    // ------------------------------------------------------------------

    /// <summary>
    ///     Waits until the run reaches a terminal status. Updates live fields (Status, Error,
    ///     CompletedAt, etc.) in-place atomically. Never throws for run status — check Status
    ///     after awaiting. Throws on infrastructure/transport failure. Throws
    ///     <see cref="OperationCanceledException"/> if the CancellationToken fires (stops
    ///     waiting only — does not cancel the run itself).
    /// </summary>
    public Task WaitAsync(CancellationToken cancellationToken = default);

    /// <summary>Waits and returns the typed result. Throws <see cref="JobRunFailedException"/>
    /// on Failed or Cancelled. Throws <see cref="InvalidOperationException"/> if the job
    /// produced no result (void job). Throws <see cref="OperationCanceledException"/> if CT
    /// fires (stops waiting; does not cancel the run).</summary>
    [RequiresUnreferencedCode("Deserializes T from JSON. Configure SurefireOptions.JsonSerializerOptions with a JsonSerializerContext for AOT/trimming support.")]
    public Task<T> GetResultAsync<T>(CancellationToken cancellationToken = default);

    /// <summary>Streams output values as the run produces them. Transparent about source:
    /// works whether results come from Output events or a final Result field.</summary>
    [RequiresUnreferencedCode("Deserializes T from JSON. Configure SurefireOptions.JsonSerializerOptions with a JsonSerializerContext for AOT/trimming support.")]
    public IAsyncEnumerable<T> StreamResultsAsync<T>([EnumeratorCancellation] CancellationToken cancellationToken = default);

    /// <summary>Low-level: streams all raw events (status changes, progress, output, logs).
    /// WaitAsync, GetResultAsync, and StreamResultsAsync are all composed from this.</summary>
    public IAsyncEnumerable<RunEvent> StreamEventsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default);

    /// <summary>Cancels this run.</summary>
    public Task CancelAsync(CancellationToken cancellationToken = default);

    /// <summary>Creates a new run based on this one and returns its handle.</summary>
    public Task<JobRun> RerunAsync(CancellationToken cancellationToken = default);
}
```

### Live field semantics

`JobRun` returned from `TriggerAsync` has `Status = Pending` and all fields known at trigger time
populated (Id, JobName, CreatedAt, NotBefore, NotAfter, Priority, DeduplicationId, Arguments, BatchId,
ParentRunId, RootRunId, RerunOfRunId, TraceId, SpanId). This is not a sparse handle — it contains
everything the scheduler knows at the moment of submission.

Live fields (Status, Progress, Error, Attempt, StartedAt, CompletedAt, CancelledAt, LastHeartbeatAt,
NodeName) update in-place atomically as the run progresses. The internal implementation stores these
as a single volatile snapshot object that is replaced atomically — consumers never observe mixed state.

```csharp
var run = await client.TriggerAsync("job", args);   // run.Status = Pending
await run.WaitAsync();                               // run.Status = Succeeded/Failed/Cancelled
if (run.Status == JobStatus.Failed) log(run.Error);
var result = await run.GetResultAsync<Order>();      // throws JobRunFailedException if Failed/Cancelled
```

`GetRunAsync(runId)` returns a `JobRun` with all fields populated from the current DB state. To
refresh a handle's live fields without waiting, call `client.GetRunAsync(run.Id)`.

### Cancellation semantics

| Method | CT fires → |
|---|---|
| `run.WaitAsync(ct)` | Stops waiting; run continues executing |
| `run.GetResultAsync<T>(ct)` | Stops waiting; run continues executing |
| `run.StreamResultsAsync<T>(ct)` | Stops streaming; run continues executing |
| `client.RunAsync<T>(..., ct)` | Cancels the run |
| `client.RunAsync(..., ct)` | Cancels the run |
| `client.StreamAsync<T>(..., ct)` | Cancels the run |
| `client.RunManyAsync<T>(..., ct)` | Cancels all runs in the batch |
| `client.StreamManyAsync<T>(..., ct)` | Cancels all runs in the batch |

Sugar methods own the run lifecycle. Handle methods do not.

### Transparent result source

Both `GetResultAsync<T>` and `StreamResultsAsync<T>` automatically determine where results come
from — callers never need to know whether the job used `yield return` or returned a value.

**`GetResultAsync<T>`** — Result JSON first, Output events as fallback:
1. If `Result` field is non-null → deserialize as `T`
2. Else if Output events exist → materialize: collect all into `T` if T is a collection type
   (`T[]`, `List<T>`, `IReadOnlyList<T>`); take the last (or only) item if T is a scalar
3. Else → `InvalidOperationException` (void job with no results)

**`StreamResultsAsync<T>`** — Output events first, Result JSON as fallback:
1. If Output events exist → yield each as `T` as they arrive
2. Else if `Result` field is non-null → deserialize and yield as a single `T` (or yield each
   element if the deserialized value is a collection)
3. Else → empty stream

`StreamAsync<T>` on `IJobClient` is conceptually `RunAsync<IAsyncEnumerable<T>>` — it triggers the
job and pipes `run.StreamResultsAsync<T>()`. It is kept as a named method for discoverability.

`Result` on public `JobRun` is the raw serialized result JSON — useful for dashboard display and
debugging, same as `Arguments`. Set to `null` until the run reaches a terminal status.

### Run attribution in batches

`JobBatch.StreamResultsAsync<T>` yields plain `T` with no run attribution. The result itself
should contain any correlation data needed (e.g. an order ID in `OrderResult`). If you need the
`JobRun` alongside the result — for the `RunId`, timing data, etc. — use `StreamRunsAsync()` and
call `run.GetResultAsync<T>()` per run:

```csharp
await foreach (var run in batch.StreamRunsAsync())
{
    var result = await run.GetResultAsync<OrderResult>();
    Console.WriteLine($"{run.Id}: {result.OrderId}");
}
```

### Error semantics

`GetResultAsync<T>` throws `JobRunFailedException` when the run is terminal but not successful:

```csharp
public sealed class JobRunFailedException(string runId, JobStatus status, string? error) : Exception
{
    public string    RunId  { get; } = runId;
    public JobStatus Status { get; } = status;   // Failed or Cancelled
    public string?   Error  { get; } = error;
}
```

- `Status = Failed` — run exhausted retries; `Error` contains the last error message
- `Status = Cancelled` — run was cancelled (by dashboard, `CancelAsync`, or a sugar method CT)
- `RunAsync` (void) also throws `JobRunFailedException` on Failed or Cancelled

`OperationCanceledException` is **not** thrown for cancelled runs — that type is reserved for when
the caller's `CancellationToken` fires (stops waiting). A run being cancelled externally is a
different concept and has different recovery semantics.

`WaitAsync()` never throws regardless of terminal status. Check `run.Status` after awaiting.

Users who need soft error handling — inspect rather than throw — should call `run.WaitAsync()` and
check `Status`, or use `run.StreamEventsAsync()` to watch for the terminal event directly.

---

## JobBatch

```csharp
public sealed class JobBatch
{
    // ------------------------------------------------------------------
    // Identity
    // ------------------------------------------------------------------
    public string                BatchId { get; init; }
    public IReadOnlyList<string> RunIds  { get; init; }

    // ------------------------------------------------------------------
    // Live counters — updated in-place as runs complete
    // ------------------------------------------------------------------
    public int Succeeded { get; internal set; }  // runs with JobStatus.Succeeded
    public int Failed    { get; internal set; }  // runs with JobStatus.Failed
    public int Cancelled { get; internal set; }  // runs with JobStatus.Cancelled
    // In-progress = RunIds.Count - Succeeded - Failed - Cancelled

    // ------------------------------------------------------------------
    // Interaction — all composed from StreamEventsAsync
    // ------------------------------------------------------------------

    /// <summary>
    ///     Waits until all runs reach a terminal status. Updates Succeeded/Failed/Cancelled
    ///     in-place. Never throws for run status — check Failed/Cancelled after awaiting.
    ///     Throws on infrastructure/transport failure. Throws
    ///     <see cref="OperationCanceledException"/> if the CancellationToken fires (stops
    ///     waiting only — does not cancel the runs).
    /// </summary>
    public Task WaitAsync(CancellationToken cancellationToken = default);

    /// <summary>Snapshot fetch of all run records at this moment.</summary>
    public Task<IReadOnlyList<JobRun>> GetRunsAsync(CancellationToken cancellationToken = default);

    /// <summary>Yields one JobRun snapshot per run as each reaches a terminal status.</summary>
    public IAsyncEnumerable<JobRun> StreamRunsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default);

    /// <summary>Waits for all runs and returns all results. Throws <see cref="JobRunFailedException"/>
    /// (wrapped in AggregateException) if any run failed or was cancelled.</summary>
    [RequiresUnreferencedCode("Deserializes T from JSON. Configure SurefireOptions.JsonSerializerOptions with a JsonSerializerContext for AOT/trimming support.")]
    public Task<T[]> GetResultsAsync<T>(CancellationToken cancellationToken = default);

    /// <summary>Streams output values across all runs as they are produced (unordered).</summary>
    [RequiresUnreferencedCode("Deserializes T from JSON. Configure SurefireOptions.JsonSerializerOptions with a JsonSerializerContext for AOT/trimming support.")]
    public IAsyncEnumerable<T> StreamResultsAsync<T>([EnumeratorCancellation] CancellationToken cancellationToken = default);

    /// <summary>Low-level: streams all raw events across all runs in the batch.
    /// All other methods compose from this.</summary>
    public IAsyncEnumerable<RunEvent> StreamEventsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default);

    /// <summary>Cancels all runs in the batch.</summary>
    public Task CancelAsync(CancellationToken cancellationToken = default);
}
```

### Composition model

```
JobRun.StreamEventsAsync()     ← primitive (raw event feed for one run)
  ├── WaitAsync()              — terminal status event
  ├── GetResultAsync<T>()      — terminal + extract result
  └── StreamResultsAsync<T>()  — filter Output events → yield T

JobBatch.StreamEventsAsync()           ← primitive (raw event feed across all runs)
  ├── WaitAsync()                      — all runs terminal
  ├── GetRunsAsync()                   — one-time snapshot fetch (not from event stream)
  ├── StreamRunsAsync()                — filter run-terminal events → yield JobRun snapshot
  ├── GetResultsAsync<T>()             — all runs terminal + collect results
  └── StreamResultsAsync<T>()          — filter Output events across all runs → yield T
```

`GetRunsAsync()` and `GetBatchAsync()` on `IJobClient` are snapshot fetches (single DB read),
not event-stream operations.

---

## BatchItem

Replaces the `(string Job, object? Args)` tuple. Per-item `Options` override the shared
`RunOptions` defaults passed to `TriggerManyAsync` — same cascading-defaults pattern as
`HttpClient` + `HttpRequestMessage`. `RunId` lives inside `RunOptions`, not here directly.

```csharp
public record BatchItem(string JobName, object? Args = null, RunOptions? Options = null);
```

`Options` is a positional constructor parameter — no property-initializer syntax required:

```csharp
await client.TriggerManyAsync([
    new BatchItem("process-order", order),
    new BatchItem("send-email",    email, new RunOptions { Priority = 10 }),
    new BatchItem("gen-report",    opts,  new RunOptions { RunId = "report-2026-04" }),
]);
```

---

## RunOptions

Add `RunId` for client-specified idempotency. All other properties unchanged.

```csharp
public sealed record RunOptions
{
    /// <summary>Client-specified run ID. Useful when you need to know the ID before
    /// the run completes, or to ensure idempotent submission.</summary>
    public string? RunId { get; init; }

    public DateTimeOffset? NotBefore       { get; init; }
    public DateTimeOffset? NotAfter        { get; init; }
    public int?            Priority        { get; init; }
    public string?         DeduplicationId { get; init; }
}
```

---

## Internal Changes

| Before | After | Scope |
|---|---|---|
| `public sealed class JobRun` (DB entity) | `internal sealed class RunRecord` | Private to store implementations |
| `RunResult` return type on `RunAsync` | Removed | Replaced by `Task` (void) or `Task<T>` (typed) |
| `ObserveAsync` returning `RunObservation` | Internal only | Public surface replaced by `StreamEventsAsync` |
| Batch stored as coordinator `RunRecord` | Dedicated `BatchRecord` table | `JobBatch` is now the public batch concept |
| `RunRecord.BatchTotal/Completed/Failed` | Moved to `BatchRecord` | `RunRecord` gains nullable `BatchId` FK instead |

### Batch table

The coordinator run pattern is removed entirely. `TriggerManyAsync` creates a `BatchRecord` row and
N `RunRecord` rows atomically. Children reference the batch via `BatchId` (nullable FK), not via
`ParentRunId`.

**Migration note:** Modify the existing v1 migration directly — no v2 migration needed. The library
is unpublished; backwards compatibility is not a concern. Add the `batches` table and `batch_id`
column on `runs` to the initial `MigrateAsync` schema in all store implementations.

```csharp
// Internal — never public
internal sealed class BatchRecord
{
    public string         Id        { get; set; }
    public int            Total     { get; set; }
    public int            Succeeded { get; set; }
    public int            Failed    { get; set; }
    public int            Cancelled { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}
```

`IJobStore` gains three methods:

```csharp
Task CreateBatchAsync(BatchRecord batch, IEnumerable<RunRecord> runs, CancellationToken cancellationToken);
Task<BatchRecord?> GetBatchAsync(string batchId, CancellationToken cancellationToken);
Task UpdateBatchProgressAsync(string batchId, JobStatus terminalStatus, CancellationToken cancellationToken); // increments Succeeded/Failed/Cancelled based on status
```

`GetRunsAsync` never returns coordinator artifacts (they don't exist anymore).

### Event stream / cursor

`RunEventCursor` stays internal. `StreamEventsAsync` reconnects transparently on network errors.
On process restart, streams begin from the start of the event log (Phase 1 accepted limitation).
The underlying event IDs are stable and ordered — adding a public cursor or resumption point to
`StreamEventsAsync` is a future extension that requires no design rework.

---

## What Stays Unchanged

- `RunEvent` and `RunEventType` — public; used by `StreamEventsAsync`
- `RunFilter` — public; used by `GetRunsAsync`; gains `BatchId` filter; loses `IsBatchCoordinator` (coordinator runs removed)
- `JobStatus` — public; `Pending/Running/Retrying/Succeeded/Failed/Cancelled` (`Pending` is the initial state, not `Queued`)
- `RunEventCursor` — **internal only**; used by `StreamEventsAsync` for transparent reconnection
- `JobContext` — unchanged; used by job handlers
- `IJobStore`, `INotificationProvider` — internal contracts; `IJobStore` gains three batch methods
- Dashboard SSE endpoint — unchanged; already built on the event infrastructure

---

## Sugar Composition

All sugar methods compose from primitives in the implementation:

```csharp
// RunAsync<T> = TriggerAsync + run.GetResultAsync<T>
async Task<T> RunAsync<T>(string job, object? args, RunOptions? options, CancellationToken cancellationToken)
{
    var run = await TriggerAsync(job, args, options, cancellationToken);
    return await run.GetResultAsync<T>(cancellationToken);
}

// StreamAsync<T> = TriggerAsync + run.StreamResultsAsync<T>
IAsyncEnumerable<T> StreamAsync<T>(string job, object? args, RunOptions? options, CancellationToken cancellationToken)
    => TriggerAsync(job, args, options, cancellationToken)
        .ToAsyncEnumerable()
        .SelectMany(run => run.StreamResultsAsync<T>(cancellationToken));

// RunManyAsync<T> = TriggerManyAsync (fan-out) + batch.GetResultsAsync<T>
async Task<T[]> RunManyAsync<T>(string job, IEnumerable<object?> args, RunOptions? options, CancellationToken cancellationToken)
{
    var batch = await TriggerManyAsync(job, args, options, cancellationToken);
    return await batch.GetResultsAsync<T>(cancellationToken);
}
```

---

## Phase 2: Durable Orchestration

Out of scope for this refactor. The `.Durable()` follow-on (replay-based durability, like
Temporal/Azure Durable Functions) is designed in `docs/design/phase2-durable.md`. That work
builds on top of this API — `JobBuilder.Durable()` opts a job handler into replay semantics.

---

## Resolved design questions

| Question | Decision |
|---|---|
| `RunId` on `RunOptions` | Keep — useful for idempotent submission (e.g., derive ID from trace ID). Not Phase 2 specific. |
| `GetResultAsync<T>` source detection | Result JSON first; Output events as fallback (materialized into T or T[]). Throws `InvalidOperationException` only for truly void jobs (no Result and no Output events). |
| `StreamResultsAsync<T>` source detection | Output events first (streamed as they arrive); Result JSON as fallback (yielded as single item or per-element if collection). Empty stream for void jobs. |
| `BatchItem.RunId` | Removed. Use `new BatchItem("job", options: new RunOptions { RunId = "..." })`. |
| `BatchItem.Job` field name | Renamed to `JobName` — consistent with `JobRun.JobName`. |
| Run attribution in `StreamResultsAsync<T>` | Not provided. Yields plain `T`. Attribution via `StreamRunsAsync()` + `run.GetResultAsync<T>()`. |
| Input streaming scope | Trigger-time only (Phase 1). `SendInputAsync` on `JobRun` deferred. |
| `Arguments` on `JobRun` | Included as serialized JSON string — useful for dashboard and debugging. |
| `Result` on `JobRun` | Included as serialized JSON string — same as `Arguments`; useful for dashboard/observability. Set to null until terminal. |
| `LastHeartbeatAt` on `JobRun` | Included — worker liveness signal. |
| `QueuePriority` internal field | Not exposed on public `JobRun`; `Priority` covers the public concept. |
| Coordinator run removal | Removed. Dedicated `batches` table + `BatchRecord`. Simplifies all store implementations. |
| Cursor / resumption on restart | Phase 1: always starts from beginning. Architecture supports future cursor extension. |
| `JobRun.WaitAsync()` return type | `Task` (void). Mutates live fields in-place atomically. Throws on infrastructure failure or CT. Never throws for run status — check `Status` after. |
| `WaitAsync()` transport failure | Throws the underlying infrastructure exception (IOException etc.) — callers should not silently swallow infra failures. |
| `JobBatch.WaitAsync()` throws? | Same as `JobRun.WaitAsync()` — throws on infra/CT, never on run status. Check `batch.Failed > 0` after awaiting. |
| Exception for failed/cancelled runs | Single `JobRunFailedException` with `Status` property (Failed or Cancelled). `OperationCanceledException` reserved for CancellationToken only. |
| `RunAsync` (void) throws? | Yes — throws `JobRunFailedException` on Failed or Cancelled, same as `RunAsync<T>`. |
| `client.WaitAsync(runId)` overloads | Removed. Use `GetRunAsync(id)` + `run.WaitAsync()` or `run.GetResultAsync<T>()` instead. |
| `RerunAsync` on batch child | Creates a standalone run with no batch association. |
| `TriggerManyAsync` with batch-level `RunOptions.RunId` | Validation error — a single run ID cannot be shared across N runs. |
| `ParentRunId` vs `BatchId` | `BatchId` = batch membership (set by `TriggerManyAsync`). `ParentRunId` = non-batch job hierarchy (when a job spawns another job). |
| `RunFilter.IsBatchCoordinator` | Removed — coordinator runs no longer exist. |
| `RunFilter.BatchId` | Added — allows filtering runs by batch. |
| Batch counters | `Succeeded` / `Failed` / `Cancelled` — drop `Total` (use `RunIds.Count`) and `Completed` (ambiguous). |
| `CancelBatchAsync` on `IJobClient` | Added — needed for dashboard and programmatic cancellation. |
| Void `RunManyAsync` sugar | Added — `Task RunManyAsync(string, IEnumerable<object?>, ...)` for fire-and-wait without results. |
| `TraceId` / `SpanId` types | `ActivityTraceId` / `ActivitySpanId` structs (System.Diagnostics) — not nullable strings. |
| AOT / trimming | Phase 1 known limitation. Generic methods annotated with `[RequiresUnreferencedCode]`. Users configure `SurefireOptions.JsonSerializerOptions` with a `JsonSerializerContext`. |
| `cancellationToken` vs `ct` | `cancellationToken` everywhere in public API — BCL convention. |
| `JobRun` constructor | Internal constructor only — prevents user construction that would NPE on the internal service reference. |
| `[EnumeratorCancellation]` | Applied to `CancellationToken` on all `IAsyncEnumerable`-returning methods for proper `await foreach` integration. |
| `JobStatus.Completed` | Renamed to `JobStatus.Succeeded` — unambiguous, consistent with `batch.Succeeded`. |
| `JobStatus.DeadLetter` | Renamed to `JobStatus.Failed` — clearer user-facing name; consistent with `batch.Failed`. |
