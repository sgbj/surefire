# Surefire Durable Execution Design Document

**Status:** Draft
**Date:** 2026-04-03

---

## 1. Problem Statement

Surefire's public API is built on three primitives: `RunAsync`, `TriggerAsync`, and `StreamAsync`. These compose naturally because they map 1:1 to simple concepts: run a job, fire a job, stream a job's output. The goal is to keep these three primitives as the *only* public API for job submission and consumption.

The problem is that without framework-level durability, these primitives break down when jobs call other jobs:

```csharp
app.AddJob("BigJob", async (IJobClient client, CancellationToken ct) =>
{
    var tasks = Enumerable.Range(0, 10_000)
        .Select(i => client.RunAsync<int>("Add", new { a = i, b = i }, ct));
    var results = await Task.WhenAll(tasks);
    return results.Sum();
});
```

**What goes wrong today:**

| Problem | Impact |
|---------|--------|
| Parent holds a concurrency slot for the entire duration | Deadlock when MaxNodeConcurrency is reached -- parent waits for children that can't get a slot |
| Process crash at child 5,000 | 5k orphaned runs, parent gone, no recovery, no indication of what happened |
| Retry of parent after stale detection | Resubmits all 10k children as duplicates |
| Querying 10k child statuses | No efficient aggregate query, no grouping key without batch concept |
| Collecting results from 10k children | No framework support -- user must poll or build their own tracking |

Today, these problems are "solved" by a separate batch API (`TriggerAllAsync`, `RunAllAsync`, `RunEachAsync`, `StreamEachAsync`) backed by a batch coordinator run. But this approach has its own problems:

- The batch coordinator pretends to be a run, causing special-case logic throughout (stale detection exclusion, executor skipping, dashboard filtering)
- The batch counter can lose increments under load (DB connection failures), leaving the coordinator permanently stuck
- There's no continuation mechanism -- no way to run code after a batch completes without holding a slot
- The batch API only supports homogeneous batches (N runs of the same job)
- It's a parallel API surface that may become redundant if durable execution is implemented

**The core question:** Can durable execution eliminate the need for batch-specific APIs entirely, letting `RunAsync`/`TriggerAsync`/`StreamAsync` be the only primitives?

---

## 2. Goals and Non-Goals

### Goals

- **Unified API:** Users write normal C# with `RunAsync`/`TriggerAsync`/`StreamAsync` inside job handlers. The framework handles durability transparently.
- **Opt-in via `.Durable()`:** Non-durable jobs work exactly as they do today. Durable is a job-level opt-in, not a global mode.
- **Crash recovery:** If a durable job's process crashes, it resumes from where it left off on restart. Already-completed child calls are not re-executed.
- **Slot efficiency:** Durable jobs release their concurrency slot while waiting for children, avoiding the deadlock problem.
- **Bulk optimization:** When a durable job makes many concurrent child calls, the framework batch-inserts them.
- **Result collection:** Durable jobs can collect typed results from child runs without manual polling.
- **Streaming support:** Durable jobs can produce and consume `IAsyncEnumerable<T>` streams, with crash recovery resuming from the last checkpoint.

### Non-Goals (for initial implementation)

- **Durable timers/sleeps** (e.g., Temporal's `workflow.Sleep`) -- out of scope
- **Signals/queries** (external input to a running durable job) -- out of scope
- **Saga compensation** (automatic rollback on failure) -- out of scope
- **Cross-version replay** (changing handler code while runs are in-flight) -- explicitly unsupported initially; document that in-flight durable runs should complete before deploying handler changes
- **Making all jobs durable by default** -- durable has constraints (determinism) that shouldn't be forced on simple jobs

---

## 3. Proposed API Surface

### Job Registration

```csharp
// Non-durable (unchanged, no constraints)
app.AddJob("Add", (int a, int b) => a + b);

// Durable (opt-in, determinism required)
app.AddJob("BigJob", async (IJobClient client, CancellationToken ct) =>
{
    var tasks = Enumerable.Range(0, 10_000)
        .Select(i => client.RunAsync<int>("Add", new { a = i, b = i }, ct));
    var results = await Task.WhenAll(tasks);
    return results.Sum();
}).Durable();
```

### What `.Durable()` means to the user

- Your handler may be called multiple times (replay). Don't worry about it -- the framework handles deduplication.
- Calls to `client.RunAsync`, `client.TriggerAsync`, and `client.StreamAsync` are recorded. On replay, completed calls return cached results instantly.
- Your handler must be deterministic: same inputs must produce the same sequence of client calls. Avoid `Random`, `DateTime.Now`, `Guid.NewGuid()` -- use framework-provided equivalents via `JobContext` if needed.
- Your job doesn't hold a concurrency slot while waiting for children.

### What the user does NOT need to know

- How replay works internally
- That their handler "suspends" and "resumes"
- The existence of a history table
- Batch insertion optimization details

### Fan-Out Patterns

```csharp
// Fan-out: many children, collect all results
app.AddJob("FanOut", async (IJobClient client, CancellationToken ct) =>
{
    var tasks = items.Select(i => client.RunAsync<int>("Process", i, ct));
    return await Task.WhenAll(tasks);
}).Durable();

// Heterogeneous fan-out: different job types
app.AddJob("Pipeline", async (IJobClient client, CancellationToken ct) =>
{
    var validated = await client.RunAsync<bool>("Validate", order, ct);
    if (!validated) return new { Status = "rejected" };

    var charged = await client.RunAsync<Receipt>("Charge", order, ct);
    await client.TriggerAsync("SendEmail", new { order, charged }, ct);
    return new { Status = "complete", charged.TransactionId };
}).Durable();
```

### Fan-In Pattern

```csharp
// Fan-in: collect results from many sources, aggregate
app.AddJob("Aggregate", async (IJobClient client, CancellationToken ct) =>
{
    var salesTask = client.RunAsync<decimal>("FetchSales", new { region = "NA" }, ct);
    var costsTask = client.RunAsync<decimal>("FetchCosts", new { region = "NA" }, ct);
    var forecastTask = client.RunAsync<decimal>("FetchForecast", new { region = "NA" }, ct);

    await Task.WhenAll(salesTask, costsTask, forecastTask);

    return new
    {
        Sales = salesTask.Result,
        Costs = costsTask.Result,
        Margin = salesTask.Result - costsTask.Result,
        Forecast = forecastTask.Result
    };
}).Durable();
```

### Chaining / Continuations

No special API needed. Sequential `await`s in a durable job ARE continuations:

```csharp
app.AddJob("MultiStage", async (IJobClient client, CancellationToken ct) =>
{
    // Stage 1: fan-out
    var rawResults = await Task.WhenAll(
        items.Select(i => client.RunAsync<Data>("Extract", i, ct)));

    // Stage 2: aggregate (this IS the continuation -- no separate API)
    var summary = await client.RunAsync<Summary>("Transform", rawResults, ct);

    // Stage 3: load
    await client.RunAsync("Load", summary, ct);

    return summary;
}).Durable();
```

### Streaming

```csharp
// Durable job that produces a stream
app.AddJob("GenerateReport", async (IJobClient client, CancellationToken ct) =>
{
    var data = await client.RunAsync<Dataset>("FetchData", ct);
    return GenerateRows(data);

    async IAsyncEnumerable<ReportRow> GenerateRows(Dataset data)
    {
        foreach (var record in data.Records)
        {
            yield return Transform(record);
        }
    }
}).Durable();

// Durable job that consumes a stream from a child
app.AddJob("StreamPipeline", async (IJobClient client, CancellationToken ct) =>
{
    var count = 0;
    await foreach (var item in client.StreamAsync<int>("Generate", ct))
    {
        count++;
        // If process crashes here, stream resumes from last checkpoint
    }
    return count;
}).Durable();
```

### Arguments and Results

No change from current model. Arguments are anonymous objects / POCOs serialized as JSON. Results are typed via generics. The durable framework records these in the history alongside call metadata.

```csharp
// Arguments: any serializable object
await client.RunAsync<Receipt>("Charge", new { orderId = 123, amount = 49.99m }, ct);

// Results: typed deserialization
var receipt = await client.RunAsync<Receipt>("Charge", args, ct);

// Fan-out results: standard Task<T>[] via Task.WhenAll
var results = await Task.WhenAll(
    items.Select(i => client.RunAsync<int>("Process", i, ct)));
```

---

## 4. Technical Approach: Replay

### Why replay?

C# async state machines cannot be serialized, suspended to disk, and resumed later. The alternative approaches and why they don't work:

| Approach | Problem |
|----------|---------|
| Serialize the async state machine | Compiler implementation detail, not stable across builds |
| Keep the Task alive in memory | Lost on process crash, which is the whole point of durability |
| Custom awaiter that blocks until children complete | Holds the thread/slot, doesn't solve the deadlock |
| Explicit checkpoint API (`ctx.Checkpoint()`) | Requires users to manually manage state, bad DX |

Replay is the proven approach (Azure Durable Functions, Temporal). The handler is invoked from scratch, but calls to `client.RunAsync` etc. consult a history of prior executions. Completed calls return cached results. The handler's code is the state machine -- the framework replays it.

### Replay mechanics

A durable job's lifecycle:

```
INVOCATION 1 (fresh):
  handler starts
  → client.RunAsync("A", args1) → not in history → submit child A → record PENDING
  → client.RunAsync("B", args2) → not in history → submit child B → record PENDING
  → await Task.WhenAll(a, b) → children pending → YIELD (save replay point, release slot)

  ... time passes, child A completes with result "hello" ...
  ... time passes, child B completes with result "world" ...

INVOCATION 2 (replay, triggered by child completion):
  handler starts from the beginning
  → client.RunAsync("A", args1) → in history, COMPLETED → return "hello" immediately
  → client.RunAsync("B", args2) → in history, COMPLETED → return "world" immediately
  → await Task.WhenAll(a, b) → both resolved → continues past
  → return "hello world" → COMPLETE
```

### Context-aware client (no wrapper)

There is no separate `DurableJobClient`. The existing `JobClient` checks whether it's inside a durable job via `JobContext.Current`. If a durable replay context is active, the client consults the history before making real calls. If not, it behaves exactly as it does today. One class, one code path, zero wrappers.

```
client.RunAsync("Child", args)
    │
    ├─ JobContext.Current?.DurableContext is null
    │   → normal path: create run, return task (existing behavior)
    │
    └─ JobContext.Current?.DurableContext is { } ctx
        → ctx.NextSequence()
        → history has completed entry? return cached result
        → history has pending entry? return awaiter for existing child
        → no entry? buffer call, will flush on yield
```

The durable context lives on `JobContext` (which already uses `AsyncLocal<T>`). The executor sets it up before invoking a durable handler and loads the replay history into it. `JobClient` has a single check at the top of its core methods -- the rest of the logic flows naturally.

### Call sequence tracking

Each client call within a durable handler is assigned a **sequence number** based on its position in the execution. This is how replay matches calls to history entries:

```
Sequence 1: client.RunAsync("A", { x: 1 })      → history[1] = completed, result: 42
Sequence 2: client.RunAsync("B", { y: 2 })      → history[2] = completed, result: 99
Sequence 3: client.RunAsync("C", { z: 3 })      → history[3] = pending
```

On replay, the handler re-executes, producing the same sequence of calls (because it's deterministic). Call #1 and #2 hit history cache. Call #3 is pending, so the handler yields.

### Yield / suspension mechanism

"Yielding" means the handler's Task returns without producing a final result. Implementation options:

**Option A: Internal exception**
The durable context on `JobContext` throws an internal `DurableYieldException` when all buffered calls are pending. The executor catches this specifically, knows the job is waiting, and marks it as `Suspended`. This is how Azure Durable Functions works internally.

**Option B: TaskCompletionSource that never completes**
Pending calls return a `Task<T>` backed by a `TaskCompletionSource` that is never completed. When the handler awaits it, the async state machine parks. The executor detects this via a timeout or coordination signal, then lets the state machine get GC'd. On child completion, a fresh invocation replays.

**Option C: Cooperative yield via CancellationToken**
The durable context cancels a special token when all pending calls are submitted, signaling the handler to exit. Requires handler cooperation.

**Recommendation:** Option A is the most proven. The yield exception is an internal detail -- the user never sees it. The executor catches it, persists the replay state, releases the slot, and schedules re-invocation when children complete.

### Batch insertion optimization

When a durable handler makes many calls before awaiting:

```csharp
// These calls happen synchronously (no await between them)
var t1 = client.RunAsync("Add", new { a = 1, b = 2 });
var t2 = client.RunAsync("Add", new { a = 3, b = 4 });
var t3 = client.RunAsync("Add", new { a = 5, b = 6 });
await Task.WhenAll(t1, t2, t3);
```

The durable context buffers all three calls. When the handler awaits (hitting the yield point), the framework flushes all buffered calls as a single bulk insert. For 10k calls, this is one DB operation, not 10k.

**Detection:** The durable context tracks calls in a buffer list. When a yield is triggered (all pending), it flushes the buffer to the store via a single `CreateRunsAsync` call. No special user action required.

**Sequential awaits:** If the user writes:

```csharp
var a = await client.RunAsync("X", args1);  // flush 1 call, yield, replay
var b = await client.RunAsync("Y", args2);  // flush 1 call, yield, replay
var c = await client.RunAsync("Z", args3);  // flush 1 call, yield, replay
```

This results in 3 separate yields and 3 replays. Each replay is fast (prior calls are cache hits), but it's O(n) replays for n sequential calls. This is correct but not optimal. Users who want bulk parallelism naturally use `Task.WhenAll`, which gives them batch insertion for free.

**Should we optimize sequential awaits?** Possible approaches:
- **Speculative execution:** After the first call completes, don't replay immediately. Wait a short window for more calls to complete. Batches the "wake up" but adds latency.
- **Document it:** Sequential awaits are fine for small N. For large fan-out, use `Task.WhenAll`. This is the same guidance Temporal gives.
- **Pre-flight analysis:** Not feasible -- the call sequence depends on runtime values.

**Recommendation:** Document the pattern. `Task.WhenAll` for parallelism is idiomatic C# already. Trying to optimize sequential awaits adds complexity with marginal benefit.

---

## 5. Streaming Interaction

Streaming is Surefire's differentiating feature. The interaction between durable execution and `IAsyncEnumerable<T>` needs careful design.

### Scenario 1: Durable job produces a stream

```csharp
app.AddJob("Generate", (CancellationToken ct) =>
{
    return ProduceItems();
    async IAsyncEnumerable<int> ProduceItems()
    {
        for (var i = 0; i < 1000; i++)
            yield return i;
    }
}).Durable();
```

**Question:** What does durability mean for a stream producer?

The producer is emitting items that are already persisted as output events in the store. If the process crashes at item 500, the run is re-executed on recovery. But items 0-499 are already in the event store. The producer would need to know to skip them.

**Approach:** On replay, the framework tells the producer "you already emitted 500 items." The producer either:
- (a) Re-produces from the start, and the framework deduplicates/skips already-persisted items based on sequence numbers, or
- (b) The producer is given a cursor/offset to resume from.

Option (a) is simpler but wastes work. Option (b) is efficient but leaks durability concerns into the producer.

**Recommendation:** Option (a) for simplicity. Output events already have sequence numbers. On replay, the framework's stream interceptor skips items that match already-persisted events. The producer re-runs its logic but the duplicate items are silently dropped. For most producers this is fine -- the items are fast to regenerate. For expensive producers (e.g., each item requires an API call), the user should use durable child calls instead.

### Scenario 2: Durable job consumes a stream from a child

```csharp
app.AddJob("Consumer", async (IJobClient client, CancellationToken ct) =>
{
    var total = 0;
    await foreach (var item in client.StreamAsync<int>("Generate", ct))
    {
        total += item;
    }
    return total;
}).Durable();
```

**Question:** If the process crashes at item 500 of 1000, how does replay handle the partially-consumed stream?

**Challenge:** The stream is stateful. The consumer has accumulated `total = sum(0..499)`. On replay:
- The `StreamAsync("Generate", ...)` call is in history -- the child run was already created
- But the 500 items already consumed are NOT individually recorded in history (that would be 500 history entries for one stream)
- The consumer's local state (`total`) is lost

**Approach options:**

**A. Checkpoint the stream cursor periodically:**
Every N items (or every N seconds), the framework checkpoints the stream position and any accumulated state. On replay, resume from the last checkpoint.

- Pro: Efficient, minimal replay
- Con: Requires serializable accumulator state. What is "state"? The framework can't introspect the handler's local variables.

**B. Replay the entire stream from the beginning:**
On crash recovery, the child's stream is re-consumed from item 0. The consumer re-processes all items.

- Pro: Simple, no special checkpointing
- Con: Wasteful for long streams. Consumer must be idempotent (processing item 0-499 again must produce the same effect).

**C. Checkpoint stream cursor only (not accumulator state):**
Resume the stream from item 500, but the consumer's local state is lost. The consumer is responsible for rebuilding any accumulated state from items 500+, or the consumer must be written to be stateless per-item.

- Pro: Simple framework implementation
- Con: Only works for stateless consumers (map/filter style), not aggregations

**D. Record each consumed item as a history entry:**
Each `MoveNextAsync()` on the stream is a recorded step, like a `RunAsync` call.

- Pro: Full replay fidelity
- Con: Massive history for large streams. A 10k item stream = 10k history entries.

**Recommendation:** This is an open question that needs prototyping. Likely answer:

- For streams of moderate size (< 1000 items): Option B (replay from start) is acceptable and simple
- For large streams: Option A with explicit opt-in checkpointing
- Streams consumed inside durable jobs should be documented as "re-consumed from the beginning on crash recovery unless explicitly checkpointed"
- The framework should provide a checkpoint helper for users who need it:

```csharp
app.AddJob("Consumer", async (IJobClient client, JobContext ctx, CancellationToken ct) =>
{
    var total = 0;
    await foreach (var item in client.StreamAsync<int>("Generate", ct))
    {
        total += item;
        // Optional: checkpoint every 100 items
        if (item % 100 == 0)
            await ctx.CheckpointAsync(new { total, position = item });
    }
    return total;
}).Durable();
```

On replay, `ctx.GetCheckpoint<T>()` returns the last saved state, and the stream resumes from that position. This is opt-in -- most users won't need it.

### Scenario 3: Durable job streams results from multiple children

```csharp
app.AddJob("StreamFanOut", async (IJobClient client, CancellationToken ct) =>
{
    // Launch 100 stream-producing children
    var streams = Enumerable.Range(0, 100)
        .Select(i => client.StreamAsync<Data>("Producer", new { partition = i }, ct));

    // Merge and process
    await foreach (var item in MergeStreams(streams))
    {
        await client.RunAsync("Sink", item, ct);
    }
}).Durable();
```

This combines fan-out (100 children), streaming (each child produces a stream), and durable calls (`RunAsync("Sink", ...)` for each merged item). This is the most complex case.

**How it works with replay:**
1. First invocation: 100 `StreamAsync` calls are recorded. Children are batch-inserted. Framework begins consuming merged stream.
2. For each merged item, `RunAsync("Sink", item)` is recorded as a durable call.
3. On crash: replay re-issues the 100 `StreamAsync` calls (cache hits -- children exist). Stream consumption resumes (see Scenario 2 for stream replay). Already-completed `RunAsync("Sink", ...)` calls are cache hits.

**Concern:** If each child produces 1000 items, and each item triggers a `RunAsync("Sink", ...)`, that's 100k history entries. This is expensive.

**Mitigation:** Consider whether the `Sink` calls need to be durable. If `Sink` is idempotent, the user could use `TriggerAsync` (fire-and-forget) instead, which doesn't need history tracking in the same way. The framework could also batch `TriggerAsync` calls without recording individual history entries, since fire-and-forget doesn't need replay fidelity.

### Scenario 4: Streaming between durable jobs (pipe pattern)

```csharp
app.AddJob("Double", (IAsyncEnumerable<int> values) =>
    values.Select(x => x * 2))
    .Durable();

app.AddJob("Pipeline", async (IJobClient client, CancellationToken ct) =>
{
    var doubled = client.StreamAsync<int>("Double",
        new { values = client.StreamAsync<int>("Generate", ct) }, ct);
    return await doubled.SumAsync(ct);
}).Durable();
```

**Question:** The inner `StreamAsync("Generate")` produces a stream that's passed as an argument to `StreamAsync("Double")`. Both are child runs. On crash recovery, the pipe needs to resume.

**Analysis:** The existing stream cursor infrastructure (`RunEventCursor`) already handles this. `Generate` persists its output events. `Double` consumes them via cursor. The cursor position is the natural checkpoint. On crash:
- `Generate` may still be running (or completed) -- its output events are persisted
- `Double` resumes consuming from its last cursor position
- The durable parent replays, gets cache hits for both `StreamAsync` calls, and resumes consuming `doubled` from its last position

This mostly works with existing infrastructure. The durable layer just needs to record that the child runs were created and what their IDs are.

---

## 6. Determinism

### The constraint

Durable handlers must be deterministic: given the same inputs, they must produce the same sequence of `client.RunAsync` / `client.TriggerAsync` / `client.StreamAsync` calls in the same order.

### What breaks determinism

| Pattern | Problem | Alternative |
|---------|---------|-------------|
| `Random.Shared.Next()` | Different value on replay | `ctx.Random.Next()` (seeded) |
| `DateTime.Now` | Different time on replay | `ctx.Time.GetUtcNow()` (recorded) |
| `Guid.NewGuid()` | Different GUID on replay | `ctx.NewGuid()` (recorded) |
| `Dictionary` iteration | Non-deterministic order in some runtimes | Use `SortedDictionary` or explicit ordering |
| Conditional calls based on external state | External state may change between replays | Use a durable call to fetch it |
| `Task.WhenAny` | Winner may differ on replay | See below |

### Task.WhenAny

`Task.WhenAny` with durable children is tricky. If you have:

```csharp
var winner = await Task.WhenAny(
    client.RunAsync<int>("FastJob", ct),
    client.RunAsync<int>("SlowJob", ct));
```

On the first execution, `FastJob` wins. On replay, both are cache hits (both completed by now), and `Task.WhenAny` might return either. The sequence of subsequent calls could differ.

**Solution:** The durable context records WHICH task won. On replay, it forces the same winner. This requires intercepting `Task.WhenAny` or providing a durable equivalent:

```csharp
// Option: framework-provided WhenAny that records the winner
var winner = await ctx.WhenAny(taskA, taskB);
```

**Open question:** Is this worth supporting in v1, or should we document that `Task.WhenAny` is not supported in durable handlers?

### Enforcement

How strictly should we enforce determinism?

**Option A: Runtime validation**
On replay, compare each call's job name + serialized args against the history. If they don't match, throw a `NonDeterministicExecutionException`. This catches bugs early but adds overhead.

**Option B: Documentation only**
Trust the user. Document the constraints. No runtime checks.

**Option C: Analyzer / Roslyn diagnostics**
Ship a source generator or analyzer that warns about `Random`, `DateTime.Now`, etc. inside handlers marked `.Durable()`. Compile-time checks, no runtime cost.

**Recommendation:** Option A (runtime validation) for correctness, with Option C as a stretch goal for better DX. A durable framework that silently produces wrong results is worse than one that throws on misuse.

---

## 7. State Management and Storage

### History table schema

```sql
CREATE TABLE surefire_durable_history (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        TEXT NOT NULL,           -- the durable job's run ID
    sequence      INT NOT NULL,            -- call position within the handler
    call_type     TEXT NOT NULL,           -- 'RunAsync', 'TriggerAsync', 'StreamAsync'
    job_name      TEXT NOT NULL,           -- target job name
    args_hash     TEXT,                    -- hash of serialized args (for validation)
    child_run_id  TEXT,                    -- the child run that was created
    status        TEXT NOT NULL,           -- 'pending', 'completed', 'failed', 'cancelled'
    result        JSONB,                   -- serialized result (for completed calls)
    error         TEXT,                    -- error message (for failed calls)
    created_at    TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ,

    UNIQUE (run_id, sequence)
);
```

### Run status additions

New status in the `JobStatus` enum:

- **Suspended** -- The durable job has yielded and is waiting for children. Not counted as "active" for concurrency limits. Not eligible for stale detection (until a configurable suspension timeout). Re-activated when a child completes.

### History lifecycle

1. **Created** when the durable handler makes a client call during execution
2. **Updated** when the child run completes (result/error recorded)
3. **Deleted** when the durable run itself reaches a terminal state and retention period expires (cleaned up alongside the run)

### History size concerns

A durable job with 10k fan-out children = 10k history rows. This is manageable -- it's metadata, not large payloads. The result column stores the serialized return value, which for most jobs is small (a number, a receipt object, etc.).

For durable jobs that consume streams with per-item `RunAsync` calls (100k+ history rows), this could be expensive. Mitigation:
- Batch inserts for history rows (same as child run creation)
- Consider a compaction mechanism for completed history
- Document that very high-cardinality durable patterns (100k+ calls per handler) should use `TriggerAsync` (fire-and-forget) rather than `RunAsync` for individual items

---

## 8. Edge Cases

### 8.1 Nested durable jobs

A durable job calls `RunAsync` on another durable job, which itself calls `RunAsync` on children.

```csharp
app.AddJob("Outer", async (IJobClient client, CancellationToken ct) =>
{
    return await client.RunAsync<int>("Inner", ct);
}).Durable();

app.AddJob("Inner", async (IJobClient client, CancellationToken ct) =>
{
    return await client.RunAsync<int>("Leaf", ct);
}).Durable();
```

**Analysis:** Each durable job has its own history. `Outer`'s history records one call to `Inner`. `Inner`'s history records one call to `Leaf`. They don't interfere. The `ParentRunId` / `RootRunId` hierarchy tracks the nesting.

### 8.2 Durable job calls non-durable job

This is the common case. `BigJob` (durable) calls `Add` (non-durable). The child runs normally. The durable history records the call and result. No issues.

### 8.3 Non-durable job calls durable job

A non-durable parent calls `RunAsync("DurableChild", ...)`. The child is durable, the parent is not. The child gets full durable treatment. The parent holds its slot while waiting (non-durable behavior). No conflict, but the parent doesn't benefit from slot release.

### 8.4 Child failure

A durable job's child fails (dead-letters):

```csharp
var result = await client.RunAsync<int>("FlakyJob", args, ct);
// ^ FlakyJob dead-letters after retries
```

The history entry is updated with status `failed` and the error. On replay, `RunAsync<T>` throws `JobRunFailedException` (existing behavior). The durable handler can catch it:

```csharp
try
{
    var result = await client.RunAsync<int>("FlakyJob", args, ct);
}
catch (JobRunFailedException ex)
{
    // Handle failure, maybe try alternative
    var result = await client.RunAsync<int>("FallbackJob", args, ct);
}
```

**Determinism concern:** The catch block introduces a branch. On replay, the framework sees: sequence 1 = FlakyJob (failed), sequence 2 = FallbackJob. The handler replays identically because the failure is recorded.

### 8.5 Partial fan-out failure

10k children, 9,999 succeed, 1 fails:

```csharp
var tasks = items.Select(i => client.RunAsync<int>("Process", i, ct));
var results = await Task.WhenAll(tasks);  // throws AggregateException
```

`Task.WhenAll` throws an `AggregateException` containing the one failure. The durable handler can catch this and handle it. On replay, 9,999 calls return cached success results, 1 throws cached failure.

### 8.6 Cancellation

User cancels a durable job via `client.CancelAsync(runId)`:
- If the job is `Suspended`: transition to `Cancelled`, cancel all pending children
- If the job is executing (in a replay): the `CancellationToken` is signalled, handler should exit

### 8.7 Timeout during suspension

A durable job suspends, but children never complete (child is stuck, child's executor is down, etc.):
- Configurable suspension timeout (distinct from execution timeout)
- After timeout, the durable run is failed/retried per its retry policy
- On retry, replay re-submits children that weren't created yet, re-awaits pending ones

### 8.8 Handler code changes between replays

User deploys a code change while a durable run is suspended. On replay, the handler produces a different call sequence than what's in history.

**If runtime validation is enabled (recommended):** `NonDeterministicExecutionException` is thrown. The run is failed with a clear error message.

**Guidance:** Wait for in-flight durable runs to complete before deploying handler changes. Future versions could add versioning support.

### 8.9 Large result payloads

A child returns a 10MB JSON result. The history stores this in the `result` column. For 10k children each returning 10MB, that's 100GB of history data.

**Mitigation:**
- Results above a configurable threshold are stored as a reference (e.g., pointer to the child run's result in the event store) rather than inline in history
- The replay loads the result on-demand when the handler accesses it
- Document guidance: for large results, prefer streaming over returning large objects

### 8.10 Idempotency of child creation

On replay, the durable handler calls `client.RunAsync("Add", args)`. The history says this call was already made and the child run ID is `abc123`. The framework should NOT create a new child -- it should reuse the existing one.

**Implementation:** The client checks the durable context's history by sequence number. If a matching entry exists with a `child_run_id`, it attaches to that existing run (calls `WaitAsync(childRunId)` or returns the cached result). No new run is created.

### 8.11 Process crash during flush

The framework is batch-inserting 10k child runs when the process crashes at run 5,000. On restart:

1. Replay the handler. First 5,000 calls have history entries (they were persisted). Next 5,000 don't.
2. The framework submits the remaining 5,000 as a new batch.
3. **Concern:** Were the first 5,000 runs inserted atomically, or could some be missing? This depends on the store's transaction behavior for `CreateRunsAsync`.

**Requirement:** The bulk insert must be atomic (all or nothing) or the framework must handle partial inserts by checking which child run IDs actually exist.

**Recommendation:** Insert in transactional batches (e.g., 500 at a time). On replay, check which children from the history actually exist in the store. Re-create missing ones.

---

## 9. Performance Considerations

### Replay cost

Each replay re-executes the handler from the start. For a handler with N completed calls, the replay is O(N) history lookups. These are in-memory after the first fetch (load all history for this run_id at the start of replay).

| Fan-out size | Replays needed | History lookups per replay | Total lookups |
|-------------|----------------|---------------------------|---------------|
| 10 (parallel) | 2 (submit + complete) | 10 | 20 |
| 10 (sequential) | 11 (1 per call + final) | 1..10 | 55 |
| 10,000 (parallel) | 2 | 10,000 | 20,000 |
| 10,000 (sequential) | 10,001 | 1..10,000 | ~50M |

**Takeaway:** Parallel fan-out (`Task.WhenAll`) is dramatically more efficient than sequential. This should be prominently documented.

### Optimization: eager completion

If all children are already complete when the framework decides to replay (e.g., fast children), skip the suspension entirely. Load history, see all complete, replay once, done. Only 1 invocation total.

### Optimization: batched wakeup

When a durable job has 10k pending children, don't replay after every single child completion. Wait until all (or a configurable threshold of) pending children complete, then replay once. This avoids 10k unnecessary replays for parallel fan-out.

**Implementation:** The `Suspended` status includes metadata about how many children are pending. The wakeup trigger fires when pending hits zero (or when the last batch of children completes).

### Memory during replay

Loading 10k history entries into memory for replay is fine (each entry is small -- sequence number, status, result reference). Loading 10k *results* could be expensive if results are large. Use lazy loading: only deserialize a result when the handler actually accesses it.

---

## 10. Alternatives Considered

### 10.1 Keep batch-specific APIs, no durable

**What:** Ship `TriggerAllAsync`, `RunAllAsync`, etc. as the primary fan-out mechanism. Fix the batch coordinator model (separate table). Add a simple continuation (job name triggered on completion).

**Pros:**
- Simpler implementation
- No replay complexity, no determinism constraints
- Ships faster

**Cons:**
- Two parallel API surfaces (primitives + batch APIs)
- Batch API only supports homogeneous batches (N runs of same job)
- No crash recovery for the parent -- if the process dies while waiting on `RunAllAsync`, there's no resumption
- The concurrency slot problem remains (parent holds slot during `RunAllAsync`)
- Continuations are a separate concept with their own API for arguments/results
- Will likely be deprecated if durable is added later

### 10.2 Explicit suspension points

**What:** Instead of transparent replay, provide explicit APIs for checkpoint/resume:

```csharp
app.AddJob("BigJob", async (JobContext ctx, IJobClient client, CancellationToken ct) =>
{
    var batchId = await client.TriggerAllAsync("Add", argsList, ct);
    var results = await ctx.SuspendUntilBatchCompleteAsync<int>(batchId, ct);
    return results.Sum();
}).Durable();
```

**Pros:**
- No replay, no determinism constraints
- User explicitly controls suspension points
- Simpler implementation

**Cons:**
- Requires new batch-specific APIs (`TriggerAllAsync`, `SuspendUntilBatchCompleteAsync`)
- User must manage state across suspension manually
- Not composable -- each new pattern needs a new suspension API
- Doesn't feel like "normal C#"

### 10.3 Event-driven decomposition (no parent job)

**What:** Instead of a parent job that fans out, model the workflow as independent jobs connected by events:

```csharp
app.AddJob("Process", (int item) => item * 2);

// Somewhere in application code:
for (var i = 0; i < 10000; i++)
    await client.TriggerAsync("Process", new { item = i });

// Separate job reacts to completions
app.AddJob("Aggregate", /* triggered by event when all Process runs complete */);
```

**Pros:**
- No parent job, no slot holding, no concurrency issue
- Each piece is simple and independent
- Natural for event-driven architectures

**Cons:**
- User must decompose their logic into separate jobs wired by events
- Loses the natural imperative flow of "do A, then B, then C"
- Complex coordination logic (tracking which runs are part of which "workflow") falls on the user
- Error handling across the workflow is manual
- Not what users expect from a job scheduling library

### 10.4 Hybrid: durable for orchestration, batch for bulk

**What:** Use durable execution for orchestration (calling different jobs in sequence/parallel) but keep an optimized batch API for the specific case of "N runs of the same job":

```csharp
// Batch: optimized bulk path
var results = await client.RunAllAsync<int>("Add", argsList, ct);

// Durable: general orchestration
app.AddJob("Pipeline", async (IJobClient client, CancellationToken ct) =>
{
    var validated = await client.RunAsync<bool>("Validate", args, ct);
    var charged = await client.RunAsync<Receipt>("Charge", args, ct);
    return charged;
}).Durable();
```

**Pros:**
- Batch path is optimized and doesn't need replay
- Durable path handles complex orchestration
- Each tool used where it fits best

**Cons:**
- Two mental models for the user
- "When do I use batch vs durable?" is a question that shouldn't exist
- If durable can handle batch patterns efficiently (via batch insertion optimization), the separate batch API is redundant

**Assessment:** If the durable framework can batch-insert concurrent `RunAsync` calls efficiently, this hybrid adds complexity without clear benefit. The batch API becomes a premature optimization that adds API surface.

---

## 11. Open Questions

### 11.1 Should `.Durable()` be the default eventually?

If durable execution works well, should all jobs be durable by default? Arguments:
- **Yes:** It's strictly better -- crash recovery, slot efficiency. Why would anyone NOT want this?
- **No:** Determinism constraints, replay overhead, simpler jobs don't need it. A job that does `(int a, int b) => a + b` gains nothing from durability.

Likely answer: No. Simple leaf jobs (no child calls) don't benefit from durability. `.Durable()` should remain opt-in for jobs that orchestrate other jobs.

### 11.2 How to handle `TriggerAsync` (fire-and-forget) in durable context?

`TriggerAsync` creates a run but doesn't wait for it. In durable context:
- **Should it be recorded in history?** If so, on replay it's a no-op (child already exists). This prevents duplicate creation on crash recovery.
- **Should it block the handler?** No -- fire-and-forget should remain non-blocking.

Likely answer: Record the call in history (for idempotency), but don't create a pending entry that blocks suspension. The durable job can suspend/complete without waiting for fire-and-forget children.

### 11.3 Can we avoid the "10k sequential replays" problem?

For sequential patterns:
```csharp
for (var i = 0; i < 10000; i++)
    results.Add(await client.RunAsync<int>("Add", new { n = i }, ct));
```

This is O(n^2) total work (replay 1 + replay 2 + ... + replay 10000). Possible optimizations:
- **Batched wakeup:** Don't replay after each child. Wait for all pending to complete.
- **But:** In sequential code, only one child is pending at a time. Each completion triggers a replay.
- **Could the framework detect the sequential loop pattern?** Not reliably at runtime.
- **Could we provide a bulk helper?** e.g., `await ctx.ForEachAsync(items, (i) => client.RunAsync(...))` that internally parallelizes and batches. But this is a new API.

Likely answer: Document that sequential loops should use `Task.WhenAll` for parallelism. For truly sequential dependencies (each call depends on the previous result), the O(n) replays are unavoidable and acceptable for moderate N.

### 11.4 What is the suspension timeout?

How long can a durable job stay suspended before it's considered stuck?
- Too short: jobs with slow children get falsely timed out
- Too long: actually-stuck jobs sit indefinitely

Should this be configurable per-job? Per-global-option? Based on the children's expected duration?

### 11.5 Stream checkpoint API design

If we add explicit checkpointing for stream consumers (Section 5, Scenario 2):
- What is the API? `ctx.CheckpointAsync(state)`?
- How often should users checkpoint?
- What happens if checkpoint writes fail?
- Does this complicate the "just works" promise of `.Durable()`?

### 11.6 History cleanup

When should history be deleted?
- When the durable run completes? (immediately -- but loses debuggability)
- After a retention period? (consistent with run retention)
- Never? (grows unbounded)

Likely answer: Tie to run retention. When a completed durable run is purged, its history is purged too.

### 11.7 Testing durable jobs

How do users unit-test durable handlers?
- Mock `IJobClient` as today? Works, but doesn't test replay behavior.
- Provide a `DurableTestHarness` that simulates replay?
- In-memory durable execution for integration tests?

### 11.8 Observability

How do users debug durable replays?
- Dashboard should show replay count, history entries, suspension/resumption timeline
- Structured logging for each replay with sequence numbers
- Metrics: `surefire.durable.replays`, `surefire.durable.history_size`, `surefire.durable.suspension_duration`

---

## 12. Recommendation

Durable execution is the feature that justifies the primitives-only API. Without it, `RunAsync`/`TriggerAsync`/`StreamAsync` are insufficient for reliable fan-out, and batch-specific APIs become necessary. With it, the three primitives are the entire public surface -- everything else is an internal optimization.

The implementation is significant but scoped:
- History table and CRUD operations
- Durable replay logic in `JobClient` via `JobContext.DurableContext`
- `Suspended` status in the run state machine
- Batch insertion optimization for concurrent calls
- Wakeup trigger when children complete
- Runtime determinism validation

The streaming interaction (Section 5) is the area with the most design risk. Simple cases (durable job calls `RunAsync` on children) are well-understood. Stream production, consumption, and piping in durable context need prototyping before committing to an approach.

### Suggested next steps

1. **Prototype the core replay loop** -- JobClient durable context, history table, yield/replay for a simple fan-out case. Validate that the mechanics work in practice.
2. **Prototype streaming + durable** -- Specifically test: durable job consuming a child's stream, crash, resume. Find where the abstraction breaks.
3. **Benchmark replay cost** -- Measure the overhead of 10k history lookups on replay. Validate that parallel fan-out is efficient enough.
4. **Design the `Suspended` status** -- How it interacts with stale detection, the dashboard, cancellation.
5. **Decide on `Task.WhenAny`** -- Support it in v1 or explicitly exclude it.
6. **Ship without batch-specific public APIs** -- Keep `TriggerAllAsync`/`RunAllAsync` internal or experimental while durable is being built.
