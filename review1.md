# Surefire — Comprehensive Code Review

**Reviewed:** All source files (7,881 LOC core, 10,649 LOC providers + dashboard), all tests (10,584 LOC across 9 projects), docs, samples, project files.

---

## Executive Summary

Surefire is an ambitious, well-architected distributed job scheduling library for .NET 10 that delivers on its "Minimal APIs for jobs" promise. The public API (`IJobClient` + `host.AddJob(…)`) is genuinely clean, the feature scope is impressive (batches, streaming I/O, rate limiting, cron, OpenTelemetry, dashboard), and the conformance test suite ensures all four store backends behave identically. The codebase makes deliberate use of modern C# features — `extension` members, `Lock`, `Guid.CreateVersion7()`, primary constructors, source-generated logging — which is appropriate for a .NET 10 library.

That said, there are meaningful issues to address. The most impactful is the `IJobClient` design: the handle pattern (`JobRun`/`JobBatch` as active objects backed by an internal interface) makes the library hard to unit test — users who mock `IJobClient` cannot construct handles with working methods. Flattening to a pure `IJobClient` with `JobRun`/`JobBatch` as data records solves this cleanly. Heavy reliance on reflection (`MakeGenericMethod`, `DynamicInvoke`) in hot paths hurts performance and Native AOT readiness. `IJobStore` being internal blocks third-party store implementations. There is clear code duplication between `JobClient` and `SurefireExecutorService` that should be consolidated. And a stuck-batch gap in the maintenance service is a correctness concern for production use.

**Verdict:** Strong foundation, ship-worthy architecture. The handle/testability redesign and stuck-batch fix are the most important things to address before v1.

---

## Table of Contents

1. [Architecture & Design](#1-architecture--design)
2. [API Design & DX](#2-api-design--dx)
3. [Modern .NET Features](#3-modern-net-features)
4. [Performance & Scalability](#4-performance--scalability)
5. [Dead Code & Duplication](#5-dead-code--duplication)
6. [Naming & Consistency](#6-naming--consistency)
7. [Error Handling & Resilience](#7-error-handling--resilience)
8. [Observability](#8-observability)
9. [Testing](#9-testing)
10. [Store Implementations](#10-store-implementations)
11. [Dashboard](#11-dashboard)
12. [Project Configuration & Packaging](#12-project-configuration--packaging)
13. [Documentation & Samples](#13-documentation--samples)
14. [Comparison to Alternatives](#14-comparison-to-alternatives)
15. [Prioritised Recommendations](#15-prioritised-recommendations)

---

## 1. Architecture & Design

### 1.1 Overall Structure — ✅ Good

The layered architecture is clean:

```
IJobClient (public API)
  ↓
JobClient (orchestration, observation, streaming)
  ↓
IJobStore (internal storage contract)
  ↓
InMemory / Sqlite / PostgreSql / SqlServer / Redis
```

Separation of concerns is mostly respected: background services handle distinct lifecycle phases (initialisation, scheduling, execution, maintenance, retention), the store contract is data-access-only, and the client is the sole orchestration point.

### 1.2 IJobStore is internal — ⚠️ High Impact Decision

**File:** `IJobStore.cs`

The entire store contract (~416 lines) is `internal`. Store implementations use `InternalsVisibleTo` in `Surefire.csproj`. This means:

- ❌ Third-party store implementations are impossible without source-level hacks.
- ❌ Users cannot create test doubles of `IJobStore` for integration testing.
- ✅ Gives you freedom to evolve the contract without semver breaks.

**Recommendation:** Make `IJobStore` public. The interface is large but stable; hiding it only frustrates users who need a custom store or want to test against a fake. A `Surefire.Abstractions` package or a clear "this may change before v1" disclaimer in the XML docs is sufficient protection during development. No reader/writer split needed — the interface is coherent as a single contract.

### 1.3 Large Classes — ⚠️ Consider Extraction

**`JobClient.cs` (1,368 lines)** and **`SurefireExecutorService.cs` (1,239 lines)** are large but not god classes — each has a coherent responsibility. The real concern is code duplication between them (see §5) and the design of the handle pattern (see §2.2), not line count per se.

If the handle pattern is dropped and the flat `IJobClient` approach is adopted (see §2.2), `JobClient` will shed the delegation boilerplate significantly. After that refactoring, extraction of shared helpers is still worthwhile:
- `ParameterBinder` — reflection-based parameter preparation (shared between both classes)
- `RunObserver` — observation loop logic (polling + notification subscription)

### 1.4 Registration Model — `host.AddJob()` vs `services.AddJob()`

**File:** `HostExtensions.cs`

Jobs are registered on `IHost`, not `IServiceCollection`:

```csharp
var app = builder.Build();
app.AddJob("MyJob", (int x) => x * 2);
```

This is deliberate (Minimal APIs parity) but has consequences:
- Jobs are registered after DI container is built → no way to conditionally register via DI configuration.
- The `JobRegistry` must be mutable after startup, which introduces thread-safety concerns during the gap between `Build()` and `Run()`.

This trade-off is acceptable given the Minimal APIs analogy, but should be documented clearly.

### 1.5 Notification-Driven + Polling Hybrid — ✅ Excellent

The dual wakeup model (subscribe to `INotificationProvider` channels for instant response, fall back to configurable polling interval) is the right approach. It gives sub-second latency with notification providers (Redis pub/sub, PostgreSQL LISTEN/NOTIFY) while remaining correct with just the in-memory or polling fallback.

### 1.6 Compare-and-Swap State Machine — ✅ Excellent

`RunStatusTransition` with expected-status + expected-attempt CAS semantics, combined with `RunTransitionRules` defining the valid state graph, is textbook correct for distributed state machines. The factory methods (`Claim()`, `Complete()`, `Fail()`, etc.) make invalid transitions unrepresentable. This is one of the strongest parts of the design.

---

## 2. API Design & DX

### 2.1 Public API Surface — ⚠️ Needs Redesign (see §2.2)

`IJobClient` currently has 15 methods split into four groups (Primitives, Sugar, Query, Control). The structure is clean, but the handle pattern creates testability problems and `IJobClientInternal` is an unnecessary internal seam. See §2.2 for the proposed flat redesign, which keeps the same 4-group structure but moves all operations onto a single mockable interface.

### 2.2 Handle Pattern — 🔴 Testability Problem

`JobRun` and `JobBatch` currently carry an `IJobClientInternal` reference so handle methods like `.WaitAsync()` and `.CancelAsync()` can delegate back to `JobClient`. This pattern has a significant problem: **`IJobClientInternal` is internal**, which means users who mock `IJobClient` in tests cannot construct `JobRun`/`JobBatch` objects with functional handle methods. Calling `.WaitAsync()` on a test-constructed `JobRun` throws `InvalidOperationException`.

**Recommended approach — Drop handles, flatten to `IJobClient`:**

Make `JobRun` and `JobBatch` **pure data records** (all `init`, no client reference). Delete `IJobClientInternal` entirely. Move all wait/stream/observe operations onto `IJobClient` with run/batch IDs as parameters. Users mock one interface. Everything is explicit and predictable.

The proposed flat API:

```csharp
public interface IJobClient
{
    // ── Trigger (fire, return data snapshot) ─────────────────────────
    Task<JobRun>   TriggerAsync(string job, object? args = null, RunOptions? options = null, CancellationToken ct = default);
    Task<JobBatch> TriggerBatchAsync(IEnumerable<BatchItem> runs, RunOptions? options = null, CancellationToken ct = default);
    Task<JobBatch> TriggerBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken ct = default);

    // ── Sugar (trigger + wait/stream in one call) ─────────────────────
    Task          RunAsync(string job, object? args = null, RunOptions? options = null, CancellationToken ct = default);
    Task<T>       RunAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken ct = default);
    IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken ct = default);
    Task          RunBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken ct = default);
    Task<T[]>     RunBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken ct = default);
    IAsyncEnumerable<T> StreamBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken ct = default);

    // ── Observe by ID ─────────────────────────────────────────────────
    Task<JobRun>   WaitAsync(string runId, CancellationToken ct = default);
    Task<T>        WaitAsync<T>(string runId, CancellationToken ct = default);
    IAsyncEnumerable<RunEvent> GetEventsAsync(string runId, CancellationToken ct = default);
    Task<JobBatch> WaitBatchAsync(string batchId, CancellationToken ct = default);
    IAsyncEnumerable<RunEvent> GetBatchEventsAsync(string batchId, CancellationToken ct = default);

    // ── Query ──────────────────────────────────────────────────────────
    Task<JobRun?>   GetRunAsync(string runId, CancellationToken ct = default);
    IAsyncEnumerable<JobRun> GetRunsAsync(RunFilter? filter = null, CancellationToken ct = default);
    Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken ct = default);

    // ── Control ───────────────────────────────────────────────────────
    Task         CancelAsync(string runId, CancellationToken ct = default);
    Task         CancelBatchAsync(string batchId, CancellationToken ct = default);
    Task<JobRun> RerunAsync(string runId, CancellationToken ct = default);
}
```

Key design decisions:
- `TriggerAsync` / `TriggerBatchAsync` — fire and return a snapshot; doesn't wait.
- `RunAsync` / `RunBatchAsync` / `StreamAsync` / `StreamBatchAsync` — sugar that triggers + waits/streams in one call.
- `WaitAsync(runId)` returns the terminal `JobRun` snapshot; `WaitAsync<T>(runId)` returns the typed result.
- `Many` renamed to `Batch` throughout for consistency with `TriggerBatchAsync`.
- `GetEventsAsync` / `GetBatchEventsAsync` return raw `IAsyncEnumerable<RunEvent>` for advanced observation (composable: streaming typed output, batch progress, etc. all derive from events).
- No `StreamAsync<T>(runId)` — avoids a confusing overload collision with the trigger-side `StreamAsync<T>(job, args)`. Advanced users re-attach via `GetEventsAsync`.

This shrinks `JobClient` meaningfully, deletes `IJobClientInternal`, and makes the library fully mockable by users.

### 2.3 Cancellation Semantics — ✅ Well Thought Out

The distinction between "cancel the run" (via `client.CancelAsync`) and "cancel the wait" (via `CancellationToken` on `WaitAsync`) is critical and correctly implemented. `RunAsync` cancels the run because it owns the lifecycle; `WaitAsync` only stops waiting. This follows the principle of least surprise.

### 2.4 FluentBuilder API — ✅ Good with One Concern

`JobBuilder` provides a clean fluent chain:

```csharp
app.AddJob("Flaky", handler)
   .WithRetry(3, TimeSpan.FromSeconds(1))
   .WithQueue("heavy")
   .OnDeadLetter(ctx => { ... });
```

**Concern:** `AddJob` in `HostExtensions` calls `registry.AddOrUpdate` which creates a `RegisteredJob` capturing the builder's internal lists. However, the builder reference is returned to the caller for further fluent configuration. If the lists in `JobDefinition` are the *same* list references that the builder mutates, then:

```csharp
var builder = app.AddJob("Flaky", handler);
// At this point, RegisteredJob already captured the definition
builder.WithRetry(3); // Mutates the definition... after it was already captured
```

This is likely fine because `AddOrUpdate` captures the whole `JobDefinition` object (not a copy), but it's fragile — if anyone ever copies the definition at registration time, late mutations would be lost. Worth adding a comment or a test.

---

## 3. Modern .NET Features

### 3.1 C# Extension Members — ✅ Forward-Looking

**File:** `JobStatus.cs`

```csharp
public static extension(JobStatus)
{
    public bool IsTerminal => ...;
    public bool IsActive => ...;
}
```

Extension members on enums are a newer C# feature shipping with .NET 10. This is appropriate for a library targeting .NET 10 — no concern here.

### 3.2 Other Modern Features — ✅ Well Utilised

| Feature | Usage | Location |
|---------|-------|----------|
| `Lock` type (.NET 9) | Thread synchronisation in InMemoryJobStore | `InMemoryJobStore.cs` |
| `Guid.CreateVersion7()` | Time-ordered IDs | Throughout |
| Primary constructors | DI injection | Background services, health check |
| Collection expressions `[]` | Array/list init | Throughout |
| Source-generated logging | `[LoggerMessage]` attributes | All services |
| `required` members | Init-only DTOs | RunFailureEnvelope, NodeInfo |
| `file`-scoped types | Internal helpers | SurefireLoggerProvider |

### 3.3 Native AOT / Trimming Readiness — 🔴 Not Ready

The heavy reflection usage makes this library incompatible with Native AOT and IL trimming:

- `MakeGenericMethod` (9 call sites across JobClient + SurefireExecutorService)
- `DynamicInvoke` (2 call sites in SurefireExecutorService)
- `MethodInfo.Invoke` with runtime-constructed parameters
- `NullabilityInfoContext` in JobArgumentsSchemaBuilder
- `Type.GetInterfaces().FirstOrDefault()` pattern for type discovery

**Recommendation:** For v1, document that AOT is not supported. For v2, consider a source-generator approach for handler registration that generates strongly-typed invocation code at compile time, similar to how Minimal APIs uses `RequestDelegateFactory`.

---

## 4. Performance & Scalability

### 4.1 Reflection in Hot Paths — 🔴 High Priority

Every job execution involves:

1. **Parameter binding** — Iterates handler parameters, checks types via reflection, deserialises from JSON.
2. **Handler invocation** — `DynamicInvoke` or `MethodInfo.Invoke` on every run.
3. **Result processing** — `TryGetAsyncEnumerableElementType` + `MakeGenericMethod` for streaming results.

For a job scheduler handling thousands of runs/second, this is measurable overhead. `DynamicInvoke` is particularly slow (~10-50x vs direct call).

**Recommendation:** Cache the reflection work per job definition. At registration time, compile a `Func<IServiceProvider, object[], CancellationToken, Task<object?>>` delegate using expression trees or `MethodInfo.CreateDelegate`. This moves the reflection cost to startup.

### 4.2 InMemoryJobStore Single Lock — ⚠️ Use ReaderWriterLockSlim

**File:** `InMemoryJobStore.cs`

All operations acquire a single `Lock`, serialising reads and writes:

```csharp
private readonly Lock _lock = new();
// Every method:
lock (_lock) { ... return Task.FromResult(result); }
```

The in-memory store should be production-suitable for single-node deployments with appropriate retention settings. Under typical workloads reads (GetRun, GetRuns, GetEvents, queries) vastly outnumber writes (claim, transition, create). The single lock means read-heavy dashboards and observation loops contend unnecessarily with the executor.

**Recommendation:** Replace `Lock` with `ReaderWriterLockSlim`. All read paths (`GetRunAsync`, `GetRunsAsync`, `GetEventsAsync`, `GetBatchAsync`, etc.) acquire a read lock; write paths acquire a write lock. This is the standard approach for this pattern and is straightforwardly correct given the existing lock scope structure.

### 4.3 Double Enumeration in GetRunsAsync — ⚠️ Performance

**File:** `InMemoryJobStore.cs`, `GetRunsAsync` method

The LINQ filter chain is evaluated twice — once for `Count()` and once for `Skip()/Take()`:

```csharp
var total = query.Count();             // Enumerates all filtered records
var items = query.Skip(...).Take(...)  // Enumerates again from scratch
    .ToList();
```

This is safe (inside `lock`) but runs every filter predicate twice. **Fix:** Materialise the filtered (but not paged) set once, then derive count and page from the list:

```csharp
var all   = query.ToList();
var total = all.Count;
var items = all.Skip(skip).Take(take).ToList();
```

For large in-memory stores this halves filter work at the cost of a single intermediate allocation — a net win for production use.

### 4.4 `RunRecord → JobRun` Allocation

**File:** `JobClient.cs`

Every observation poll, every query, every batch stream creates new `JobRun` objects from `RunRecord`. With the proposed flat API (§2.2) where `JobRun` becomes a pure data record and there's no `IJobClientInternal` injection, these allocations are simpler and `JobRun` can become a `record` or `readonly struct` if appropriate.

### 4.5 Channel Backpressure — ✅ Good

`SurefireLogEventPump` uses `BoundedChannelOptions` with `FullMode.DropWrite` and capacity 1024. This is correct — log events should never block the executor. The instrumentation counter tracks dropped events.

---

## 5. Dead Code & Duplication

### 5.1 Duplicate `TryGetAsyncEnumerableElementType` — 🔴 Must Fix

**Files:** `JobClient.cs:1388`, `SurefireExecutorService.cs:965`

Identical implementations (~15 lines each). Both check:
1. Direct `IAsyncEnumerable<T>` match
2. Interface scan for `IAsyncEnumerable<T>`

**Fix:** Extract to a shared `internal static class TypeHelpers` in the `Surefire` namespace.

### 5.2 Duplicate `TryGetCollectionElementType` — 🔴 Must Fix

**Files:** `JobClient.cs:1214`, `SurefireExecutorService.cs:743`

Similar but not identical — the executor version has an `out bool asArray` parameter and supports arrays; the client version doesn't. This is a code smell: both are doing the same conceptual work (type introspection for streaming) but have diverged.

**Fix:** Unify into a single method with the superset signature. The client can ignore the `asArray` output.

### 5.3 Duplicate Parameter/Argument Preparation Logic

Both `JobClient` (for preparing run arguments from user input) and `SurefireExecutorService` (for preparing handler parameters from stored JSON) contain parallel reflection-heavy code for:
- Detecting `IAsyncEnumerable<T>` parameters
- Detecting `CancellationToken` parameters
- Detecting DI-injected service parameters
- Detecting `JobContext` parameters

This duplication will drift over time.

**Fix:** Extract a `ParameterMetadata` class that analyses handler signatures once at registration and caches the results.

### 5.4 SQL Schema Duplication Across Stores

The four SQL-based stores each contain ~150-200 lines of CREATE TABLE statements that define essentially the same schema. While the SQL dialects differ, the logical schema is identical.

**Recommendation:** This is harder to fix (SQL dialects vary), but consider generating the schema from a shared definition or at least maintaining a canonical schema document that the SQL strings must match.

---

## 6. Naming & Consistency

### 6.1 Overall Naming — ✅ Consistent

- Types use PascalCase throughout
- Methods use PascalCase with Async suffix correctly
- Private fields use `_camelCase`
- Constants and enum values use PascalCase (C# convention)
- No Hungarian notation

### 6.2 `RunRecord` vs `JobRun` — ✅ Good Distinction

Internal `RunRecord` (flat DTO for storage) vs public `JobRun` (rich handle with methods). The naming clearly communicates intent.

### 6.3 `SurefireOptions` Configuration Actions — ⚠️ Minor Inconsistency

**File:** `SurefireOptions.cs`

```csharp
options.UsePostgreSql(connectionString);  // Store + notifications
options.UseRedis(connectionString);        // Store + notifications
options.UseSqlite(connectionString);       // Store only
options.UseSqlServer(connectionString);    // Store only
```

SQLite and SQL Server don't have notification providers, so their `Use*` methods only register the store. This asymmetry is logical but undocumented — users might wonder why SQLite doesn't get instant wakeup.

**Recommendation:** Add XML docs on the `Use*` extension methods explaining what each registers (store only vs store + notifications).

### 6.4 `NodeInfo` as Mutable Class — ⚠️ Inconsistent

**File:** `NodeInfo.cs`

Most DTOs are records or record structs (`BatchCounters`, `DashboardStats`, `PagedResult<T>`, `RunObservation`). `NodeInfo` is a mutable `sealed class` with `required` init properties. This should be a `sealed record` for consistency.

### 6.5 `MisfirePolicy` vs `MissedFirePolicy`

**File:** `MisfirePolicy.cs`

The enum is `MisfirePolicy` but the concept described in docs and scheduler is "missed fire". Either is fine, but be consistent in documentation — `api-v2.md` uses "missed fires" while the code uses "misfire".

---

## 7. Error Handling & Resilience

### 7.1 Exception Types — ✅ Good

Three custom exceptions with clear semantics:
- `RunConflictException` — deduplication conflict
- `RunNotFoundException` — run doesn't exist
- `JobRunFailedException` — wraps handler failures for `RunAsync<T>`

### 7.2 Stuck Batch — No Recovery Sweep — 🔴 Gap

**File:** `BatchCompletionHandler.cs`, `SurefireMaintenanceService.cs`

`MaybeCompleteBatchAsync` correctly guards against publishing completion if `TryCompleteBatchAsync` fails — that's right. The real problem is **there is no recovery sweep for batches that get permanently stuck**.

The failure scenario:
1. All N child runs complete successfully (or fail) and transition to terminal states.
2. On the Nth completion, `TryCompleteBatchAsync` fails transiently (e.g., DB write error).
3. Result: all child runs are terminal, but the batch record stays non-terminal. The `BatchTerminated` notification is never published.

The maintenance service (`RecoverStaleRunningRunsAsync`) only processes runs in `Running` status with a stale heartbeat — it cannot discover this stuck batch because all its children are already terminal and invisible to that query.

**Fix:** Add a maintenance sweep — either a new store method `GetStuckBatchesAsync` (batches where all children are terminal but the batch is not) or a periodic check that re-runs `MaybeCompleteBatchAsync` against non-terminal batches whose counters indicate completion. `TryCompleteBatchAsync` is already idempotent (returns `false` if already terminal), so safe to retry.

### 7.3 Stale Run Recovery — ✅ Robust

**File:** `SurefireMaintenanceService.cs`

The heartbeat + stale recovery pattern is solid:
1. Nodes heartbeat periodically
2. Maintenance checks for runs on nodes that haven't heartbeated past `InactiveThreshold`
3. Stale runs are transitioned to Retrying → re-queued

This handles node crashes correctly.

### 7.4 Graceful Shutdown — ✅ Good

**File:** `SurefireExecutorService.cs`

The shutdown sequence:
1. `StoppingToken` fires → claim loop stops
2. Wait for in-flight handlers up to `ShutdownTimeout`
3. Cancel remaining handlers
4. Log warnings for abandoned runs

### 7.5 PostgreSQL Reconnection — ✅ Robust

**File:** `PostgreSqlNotificationProvider.cs`

Exponential backoff with jitter (200ms base, 30s cap) on LISTEN connection failures. The dispatch loop is decoupled via a bounded channel, so reconnection doesn't block event delivery from already-received notifications.

---

## 8. Observability

### 8.1 OpenTelemetry Integration — ✅ Good

**File:** `SurefireInstrumentation.cs`

- `ActivitySource` for distributed tracing (activities per job execution)
- Counter for completed runs (by job name, status)
- Histogram for execution duration (by job name)
- Meter disposal implemented correctly

### 8.2 Structured Logging — ✅ Excellent

All background services use `[LoggerMessage]` source-generated logging with semantic parameters:

```csharp
[LoggerMessage(Level = LogLevel.Information, Message = "Claimed run {RunId} for job {JobName}")]
static partial void LogRunClaimed(ILogger logger, string runId, string jobName);
```

This is the recommended .NET pattern — zero-allocation when the log level is disabled.

### 8.3 Log Event Pump — ✅ Clever

**File:** `SurefireLogEventPump.cs`, `SurefireLoggerProvider.cs`

The custom logger provider captures log entries from within job handlers (gated by `JobContext.Current != null`) and pumps them to the store as run events. This gives the dashboard real-time log streaming without requiring users to change their logging code. The bounded channel with drop semantics prevents slow stores from blocking handlers.

### 8.4 Health Check — ✅ Simple and Correct

**File:** `SurefireHealthCheck.cs`

Checks that the store is responsive and the node heartbeat is recent. Returns `Degraded` if heartbeat is stale, `Unhealthy` on exception. Integrates with ASP.NET Core health check infrastructure.

### 8.5 Missing: Metrics for Store Operations

There are no metrics on store operation latency (claim time, create time, query time). For production use with SQL stores, knowing that `ClaimRunAsync` takes 50ms vs 500ms is critical.

**Recommendation:** Add a histogram for store operation duration, tagged by operation name.

---

## 9. Testing

### 9.1 Test Structure — ✅ Excellent Architecture

The conformance test pattern is the standout testing design:

```
StoreConformanceBase (abstract)
  ├── ClaimConformanceTests (509 lines, 41 tests)
  ├── RunCrudConformanceTests (973 lines)
  ├── TransitionConformanceTests (293 lines)
  ├── EventConformanceTests (319 lines)
  ├── BatchConformanceTests (289 lines)
  ├── CancelConformanceTests (214 lines)
  ├── PurgeConformanceTests (211 lines)
  ├── MaintenanceConformanceTests (242 lines)
  └── ... (19 conformance suites total)

Each store project inherits all suites:
  InMemoryClaimTests : ClaimConformanceTests
  SqliteClaimTests : ClaimConformanceTests
  PostgreSqlClaimTests : ClaimConformanceTests
  ...
```

This is textbook "write tests once, run against all implementations." Combined with Testcontainers for PostgreSQL, Redis, and SQL Server, this gives high confidence in store equivalence.

### 9.2 Coverage — ⚠️ Gaps

**Well tested:**
- Store conformance (19 suites × 4-5 stores)
- JobClient contract tests (1,088 lines)
- Runtime worker tests (1,670 lines)
- Dashboard endpoint tests (667 lines)
- Retry policy edge cases

**Under-tested or missing:**
- `SurefireSchedulerService` — no dedicated unit tests for cron scheduling logic, missed fire policies
- `SurefireMaintenanceService` — only conformance-level; no unit tests for heartbeat edge cases
- `SurefireRetentionService` — no dedicated tests visible
- `NotificationChannels` — channel name hashing/validation not directly tested
- `JobArgumentsSchemaBuilder` — no tests for schema generation
- `ParameterBinder` logic in JobClient/SurefireExecutorService — covered indirectly through integration but no focused unit tests on edge cases (e.g., what happens when a handler parameter type can't be resolved)

### 9.3 Test/Source Ratio — Adequate

```
Source:  18,530 lines (all src/ projects)
Tests:   10,584 lines (all test/ projects)
Ratio:   0.57
```

For a library with this complexity, a ratio of 0.57 is acceptable but could be higher. The conformance pattern compensates by giving multiplicative coverage.

### 9.4 Concurrent Claim Tests — ⚠️ Potentially Flaky

**File:** `ClaimConformanceTests.cs`

```csharp
// 10 trials, 10 threads each
for (var trial = 0; trial < 10; trial++)
{
    var tasks = Enumerable.Range(0, 10)
        .Select(_ => store.ClaimRunAsync(...));
    var results = await Task.WhenAll(tasks);
    Assert.Single(results, r => r is not null);
}
```

Concurrent tests with fixed trial counts can be flaky under CI load. Consider using `Assert.All` with relaxed timing or increasing the trial count.

---

## 10. Store Implementations

### 10.1 Overview

| Store | Lines | Migration Strategy | Notification Support | Key Technology |
|-------|-------|--------------------|---------------------|----------------|
| InMemory | 1,442 | N/A | InMemoryNotificationProvider | Lock + dictionaries |
| SQLite | 1,953 | SQL DDL v1/v2/v3 | None | WAL mode, indexed queries |
| PostgreSQL | 2,336 | Advisory lock + DDL | LISTEN/NOTIFY | JSONB, TIMESTAMPTZ |
| SQL Server | 2,134 | sp_getapplock + DDL | None | NVARCHAR, DATETIMEOFFSET |
| Redis | 3,291 | N/A (schemaless) | Redis pub/sub | Lua scripts, sorted sets |

### 10.2 Redis Complexity — ⚠️ Maintainability Risk

At 3,291 lines, the Redis store is the largest by far. It encodes all logic in Lua scripts (for atomicity) which are:
- Hard to test in isolation
- Hard to debug
- No compile-time type checking
- Performance characteristics opaque (Redis single-threaded execution during Lua)

The Lua scripts are well-structured (embedded as string constants), but this is inherently fragile.

**Recommendation:** Add inline comments in Lua scripts explaining the invariants being maintained. Consider a Lua script test harness.

### 10.3 SQLite WAL Mode — ✅ Correct

SQLite is configured with WAL (Write-Ahead Logging) mode, which allows concurrent reads during writes. This is the right choice for a job store.

### 10.4 PostgreSQL Advisory Locks for Migrations — ✅ Correct

Using `pg_advisory_lock` for migration synchronisation prevents concurrent migration races in multi-node deployments.

### 10.5 Missing: Connection Resilience

None of the SQL stores have explicit retry logic for transient database errors (connection drops, deadlocks). They rely on the caller (background services) to retry at the loop level, which works but means a single transient failure can lose a claim cycle.

**Recommendation:** Consider wrapping store operations with a retry policy (e.g., Polly or simple exponential backoff) for transient SQL errors, especially for `ClaimRunAsync` which is latency-sensitive.

---

## 11. Dashboard

### 11.1 Architecture — ✅ Clean

**File:** `DashboardEndpoints.cs` (719 lines)

The dashboard is a Minimal API endpoint group with:
- Embedded SPA UI (React/Vite, built during MSBuild)
- SSE streaming for real-time log viewing
- RESTful CRUD for jobs, runs, queues, nodes
- SPA fallback routing (404 → index.html)

### 11.2 SSE Implementation — ⚠️ Complex

The SSE streaming endpoint (lines ~340-480) uses semaphores, task completion sources, and multi-stage event draining. While functional, it's the most complex endpoint and could benefit from extraction into a helper class.

### 11.3 Cron Next-Run Calculation — ⚠️ Silent Failures

**File:** `DashboardModels.cs` (lines ~189-209)

```csharp
try { return cronExpression.GetNextOccurrence(...); }
catch { return null; }
```

Silent failure on malformed cron expressions means the dashboard shows "no next run" without explanation. Consider logging a warning or returning an error indicator.

---

## 12. Project Configuration & Packaging

### 12.1 Central Package Management — ✅ Good

`Directory.Packages.props` with `ManagePackageVersionsCentrally` ensures version consistency across all projects.

### 12.2 TreatWarningsAsErrors — ✅ Good

`Directory.Build.props` enables `TreatWarningsAsErrors`, which prevents warning accumulation.

### 12.3 Target Framework — Appropriate

`net10.0` is correct for a library shipping with .NET 10. Consider multi-targeting `net9.0;net10.0` if you want broader adoption before .NET 10 GA.

### 12.4 InternalsVisibleTo — ✅ Correctly Scoped

```xml
<InternalsVisibleTo Include="Surefire.Tests.InMemory" />
<InternalsVisibleTo Include="Surefire.Tests" />
<InternalsVisibleTo Include="Surefire.Tests.Integration" />
```

Test projects have access to internals. Store projects presumably also need this (SQLite, PostgreSQL, etc.) — verify all store projects are listed or use `[assembly: InternalsVisibleTo]` in AssemblyInfo.

---

## 13. Documentation & Samples

### 13.1 README — ✅ Good but Incomplete

The README covers the happy path well with clear examples. Missing:
- Configuration reference (what does each `SurefireOptions` property do?)
- Store comparison table (which stores support notifications? Which need external setup?)
- Migration guide (how to upgrade between versions)
- Performance characteristics / benchmarks
- Limitations section (no AOT, no custom stores, etc.)

### 13.2 Design Docs — ✅ Excellent

`api-v2.md` (540 lines) and `phase2-durable.md` (931 lines) are thorough, well-reasoned design documents that capture decisions, alternatives, and open questions. These are invaluable for onboarding contributors.

### 13.3 Sample — ✅ Comprehensive

`Surefire.Sample/Program.cs` demonstrates 12 different job patterns in 165 lines, covering most features. It's a good "kitchen sink" example.

---

## 14. Comparison to Alternatives

| Feature | Surefire | Hangfire | Quartz.NET | Wolverine |
|---------|----------|----------|------------|-----------|
| **API Style** | Minimal APIs (`AddJob` + lambdas) | Attribute-based + `BackgroundJob.Enqueue` | `IJob` interface | Message handlers |
| **Streaming I/O** | ✅ First-class `IAsyncEnumerable` | ❌ | ❌ | ❌ |
| **Batches** | ✅ Native with atomic trigger | ✅ (Pro) | ❌ | ❌ |
| **Dashboard** | ✅ Embedded SPA | ✅ (mature) | ❌ (3rd party) | ❌ |
| **Rate Limiting** | ✅ Fixed/sliding window | ❌ | ❌ | ❌ |
| **Stores** | 4 (In-Memory, SQLite, PG, MSSQL, Redis) | 2+ (SQL, Redis, etc.) | 3+ (ADO.NET, RAM) | PG, MSSQL, RabbitMQ |
| **OpenTelemetry** | ✅ Native | ❌ | ❌ | ✅ |
| **Cron** | ✅ Cronos | ✅ | ✅ (native) | ❌ |
| **Native AOT** | ❌ (reflection-heavy) | ❌ | ❌ | Partial |
| **Maturity** | Pre-release | Mature (10+ years) | Mature (20+ years) | Growing |

**Surefire's differentiators:**
1. Streaming I/O is unique — no other .NET job library supports `IAsyncEnumerable` parameters/results.
2. The Minimal APIs DX is genuinely simpler than Hangfire's attribute model or Quartz's `IJob` interface.
3. Built-in rate limiting is a rare feature.
4. The batch primitive is more powerful than Hangfire Pro's batches.

**Surefire's weaknesses vs alternatives:**
1. No AOT support (Hangfire/Quartz don't have it either, but it's increasingly expected).
2. No distributed locking/coordination beyond the store (Wolverine has Marten/RabbitMQ sagas).
3. No message bus integration (Hangfire has MassTransit integration).
4. Much less battle-tested than Hangfire/Quartz.

---

## 15. Prioritised Recommendations

### 🔴 P0 — Fix Before Release

1. **Redesign `IJobClient` — drop handles, flatten API** (§2.2): Make `JobRun`/`JobBatch` pure data records. Delete `IJobClientInternal`. Move all wait/stream/observe operations onto `IJobClient`. Rename `TriggerManyAsync`/`RunManyAsync`/`StreamManyAsync` to `TriggerBatchAsync`/`RunBatchAsync`/`StreamBatchAsync` for naming consistency. This is the highest-leverage change for testability and API clarity.

2. **Make `IJobStore` public** (§1.2): Remove the `internal` modifier. Add XML doc noting the interface may evolve before v1. This unblocks third-party stores and test doubles.

3. **Add stuck-batch recovery sweep** (§7.2): Add a maintenance sweep for non-terminal batches whose child run counters indicate all runs are complete. `TryCompleteBatchAsync` is idempotent so re-driving it is safe.

4. **Cache reflection work per job definition** (§4.1) — At registration time, analyse handler parameters and compile a `HandlerMetadata` object. Use it in both `JobClient` and `SurefireExecutorService` instead of re-reflecting on every invocation. Eliminates `DynamicInvoke`.

5. **Replace `Lock` with `ReaderWriterLockSlim` in `InMemoryJobStore`** (§4.2) and fix double enumeration in `GetRunsAsync` (§4.3) — needed for production single-node use.

6. **Collapse all store migrations to a single v1** (§16): Since the library is unreleased, remove all incremental `ALTER TABLE` migration paths and consolidate each store's DDL into a single definitive v1 script that represents the current full schema.

7. **Extract duplicate `TryGetAsyncEnumerableElementType` and `TryGetCollectionElementType`** (§5.1/5.2) into shared `TypeHelpers`. Active code duplication that will drift.

### 🟡 P1 — Fix Soon After Release

8. **Add store operation metrics** (§8.5) — Histogram for operation duration tagged by operation name (`claim`, `create`, `query`, `transition`).

9. **Document AOT incompatibility** (§3.3) — Add a remark in the README and package description.

10. **Add XML docs to `Use*` extension methods** (§6.3) — Clarify which services each method registers.

### 🟢 P2 — Nice to Have

11. **Add unit tests for scheduler, retention, and argument schema builder** (§9.2).

12. **Normalise `NodeInfo` to a record** (§6.4).

13. **Add connection resilience to SQL stores** (§10.5) — Transient error retry for `ClaimRunAsync` and `TryCreateRunAsync`.

14. **Explore source-generator approach for handler invocation** (§3.3) — Eliminate reflection in hot paths for v2, enabling AOT support.

15. **Add Lua script inline documentation** in RedisJobStore (§10.2).

---

## 16. Schema Migrations — Collapse to Single Version

**Files:** `SqliteJobStore.cs`, `PostgreSqlJobStore.cs`, `SqlServerJobStore.cs`

Each store currently has 3 migration versions applied incrementally at startup:
- **v1**: Initial schema (all tables, indexes)
- **v2**: `ALTER TABLE surefire_jobs ADD COLUMN timeout_ticks` (SQLite also migrates data from `timeout_ms`)
- **v3**: `ALTER TABLE surefire_batches ADD COLUMN cancelled`

Since this library has no production deployments yet, there are no existing databases to protect. The incremental migration machinery — `HasMigrationVersionAsync`, `HasColumnAsync` guards, version tracking rows, multi-step ALTER paths — exists purely to service databases that will never exist in the wild.

**Recommendation:** Collapse to a single v1 migration per store:
1. Rewrite each store's `MigrateAsync` to a simple "create tables if not exist" check: if `MAX(version) >= 1` (or just the presence of the migration row), skip.
2. The v1 DDL is the **current complete schema** — all columns, including `timeout_ticks` and `cancelled`, defined in their final form from the start.
3. Delete all `HasColumnAsync` guards, all `ALTER TABLE` blocks, and all multi-version version tracking.
4. Remove `HasMigrationVersionAsync` (SQLite) and any v2/v3 checking logic.

The migration tracking table and advisory lock mechanism (PostgreSQL) are good patterns and should be kept — just with a single version entry.

---

## 17. Top 5 Implementation Plan

The following items are the highest-priority changes in recommended implementation order. Each has a specific plan and required test coverage.

---

### Item 1: Flat `IJobClient` API — Drop Handle Pattern

**Why first:** Every other change touches `JobClient` or tests. Settling the API surface first prevents double-work.

**What changes:**

_Delete:_
- `IJobClientInternal.cs` entirely

_`JobRun.cs` — becomes a pure data record:_
- Remove `_client` field and both constructors
- Remove all delegate methods: `WaitAsync`, `GetResultAsync<T>`, `StreamResultsAsync<T>`, `StreamEventsAsync`, `CancelAsync`, `RerunAsync`
- Remove `CopyLiveFields`, `GetClient`
- All properties stay — `init`-only. `internal set` on mutable live fields (`Status`, `Progress`, etc.) may become `init` too depending on whether `WaitAsync` on `IJobClient` returns a new snapshot (preferred) or mutates in place
- `GetResult<T>` and `TryGetResult<T>` stay — they're pure operations on the data

_`JobBatch.cs` — same treatment:_
- Remove `_client`, constructors, all delegate methods, `CopyLiveFields`, `GetClient`
- Keep counters and identity fields as `init`
- Simplify `FromRecord` factory methods (no client overload needed)

_`IJobClient.cs` — add the missing operations:_
```csharp
// Observe by ID (replaces IJobClientInternal)
Task<JobRun>                     WaitAsync(string runId, CancellationToken ct = default);
Task<T>                          WaitAsync<T>(string runId, CancellationToken ct = default);
IAsyncEnumerable<RunEvent>       GetEventsAsync(string runId, CancellationToken ct = default);
Task<JobBatch>                   WaitBatchAsync(string batchId, CancellationToken ct = default);
IAsyncEnumerable<RunEvent>       GetBatchEventsAsync(string batchId, CancellationToken ct = default);

// Renames (breaking, but unreleased)
// TriggerManyAsync  → TriggerBatchAsync
// RunManyAsync      → RunBatchAsync  
// StreamManyAsync   → StreamBatchAsync
```

_`JobClient.cs`:_
- Promote the 10 methods from `IJobClientInternal` to `IJobClient` implementations (they already exist in `JobClient`, just exposed differently)
- Remove the `IJobClientInternal` interface implementation annotation
- `WaitAsync` returns a new terminal `JobRun` snapshot (no mutation of caller's object)

**Required tests:**
- `IJobClient` can be mocked with `NSubstitute`/`Moq` and `JobRun`/`JobBatch` constructed with object initializers — verify no exceptions
- `WaitAsync(runId)` returns a `JobRun` with terminal `Status`
- `WaitAsync<T>(runId)` returns the typed result, throws `JobRunFailedException` on failure/cancellation
- `GetEventsAsync(runId)` streams events in order and terminates after terminal event
- `WaitBatchAsync(batchId)` returns terminal `JobBatch` with correct counters
- `GetBatchEventsAsync(batchId)` streams events across all batch children
- Rename conformance: `TriggerBatchAsync`, `RunBatchAsync`, `StreamBatchAsync` behave identically to their predecessors

---

### Item 2: Stuck Batch Recovery Sweep

**Why second:** Correctness gap. A transient store failure during batch completion permanently corrupts batch state — it will never auto-recover.

**What changes:**

_`IJobStore.cs` — new method:_
```csharp
/// Returns IDs of non-terminal batches where succeeded + failed + cancelled >= total.
/// These batches are completable but were not finalized (e.g., due to a transient write failure).
Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(CancellationToken cancellationToken = default);
```

_Each store implementation:_
- **InMemory:** iterate `_batches.Values` under read lock, filter `!status.IsTerminal && (succeeded + failed + cancelled >= total)`
- **SQL stores:** `SELECT id FROM surefire_batches WHERE status NOT IN (2,4,5) AND succeeded + failed + cancelled >= total`

_`BatchCompletionHandler.cs` — new method:_
```csharp
public async Task RecoverBatchAsync(string batchId, CancellationToken cancellationToken)
{
    var batch = await store.GetBatchAsync(batchId, cancellationToken);
    if (batch is null || batch.Status.IsTerminal) return;

    var batchStatus = batch.Failed > 0 ? JobStatus.Failed
        : batch.Cancelled > 0 ? JobStatus.Cancelled
        : JobStatus.Succeeded;

    if (!await store.TryCompleteBatchAsync(batchId, batchStatus, timeProvider.GetUtcNow(), cancellationToken))
        return;

    await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId, cancellationToken);
}
```

_`SurefireMaintenanceService.cs` — add to `RunMaintenanceTickAsync`:_
```csharp
var stuckBatchIds = await store.GetCompletableBatchIdsAsync(cancellationToken);
foreach (var batchId in stuckBatchIds)
    await batchCompletionHandler.RecoverBatchAsync(batchId, cancellationToken);
```

**Required tests:**
- **Conformance:** `BatchConformanceTests` — `GetCompletableBatchIdsAsync` returns a batch whose children are all terminal but `TryCompleteBatchAsync` was never called; returns nothing for in-progress or already-terminal batches
- **Integration:** Create a batch, complete all child runs, simulate `TryCompleteBatchAsync` failure (mock store or test hook), verify `GetCompletableBatchIdsAsync` returns the batch ID, verify `RecoverBatchAsync` drives it to terminal and publishes the notification
- **Maintenance sweep test:** Verify `RunMaintenanceTickAsync` calls `RecoverBatchAsync` for each stuck batch and does not call it for healthy batches

---

### Item 3: Make `IJobStore` Public

**Why third:** Unblocks third-party stores and is required before any user tests that mock the store. Small change, high leverage.

**What changes:**

_`IJobStore.cs`:_
- Remove `internal` — change to `public`
- Add XML doc: `/// <remarks>This interface is public but may change before v1.0. Implement with caution.</remarks>`

_`Surefire.csproj`:_
- The `InternalsVisibleTo` entries for store implementation projects may still be needed if those projects reference other internal types (e.g., `RunRecord`, `BatchRecord`, `RunStatusTransition`). Audit each — keep only entries that are still required.

_No behaviour changes._

**Required tests:**
- Compile-time verification: a test project without `InternalsVisibleTo` can implement `IJobStore` and register it via `services.AddSingleton<IJobStore, MyStore>()` — this is a build test, not a runtime test
- Existing conformance tests continue to pass (no regression)

---

### Item 4: `HandlerMetadata` — Cache Reflection Per Job Definition

**Why fourth:** Every job invocation currently pays full reflection cost. For a library claiming production readiness, `DynamicInvoke` on every execution is not acceptable.

**What changes:**

_New type `HandlerMetadata.cs`:_
```csharp
internal sealed class HandlerMetadata
{
    // Built once at AddJob() time via Analyse(Delegate handler)
    public required Type? JsonArgumentType { get; init; }   // null if no JSON arg
    public required Type? OutputElementType { get; init; }  // T for IAsyncEnumerable<T>, null otherwise
    public required Type? ResultType { get; init; }         // T for Task<T>, null for Task/void
    public required bool HasJobContext { get; init; }
    public required bool HasCancellationToken { get; init; }
    public required IReadOnlyList<Type> DiServiceTypes { get; init; }

    // Compiled invocation delegate — avoids DynamicInvoke entirely
    // Signature: (object handler, IServiceProvider sp, object? jsonArg, JobContext ctx, CancellationToken ct) => Task
    public required Func<object, IServiceProvider, object?, JobContext, CancellationToken, Task> Invoke { get; init; }

    public static HandlerMetadata Analyse(Delegate handler) { ... } // expression tree compilation
}
```

_`RegisteredJob.cs` / `JobDefinition.cs`:_
- Add `HandlerMetadata Metadata` property populated at `AddJob()` time

_`SurefireExecutorService.cs`:_
- Replace all `MakeGenericMethod` / `DynamicInvoke` / `MethodInfo.Invoke` call sites with `registeredJob.Metadata.Invoke(...)`
- Replace `TryGetAsyncEnumerableElementType` call with `metadata.OutputElementType`

_`JobClient.cs`:_
- Replace `TryGetCollectionElementType` / `TryGetAsyncEnumerableElementType` usage with `metadata.OutputElementType` / `metadata.JsonArgumentType`
- Result: `TryGetAsyncEnumerableElementType` and `TryGetCollectionElementType` helpers can be deleted entirely (§5.1, §5.2 resolved as a side effect)

**Expression tree compilation approach** (not `DynamicInvoke`, not simple `Delegate.Method.Invoke`):
- Build a `LambdaExpression` from the handler's parameter metadata
- `Expression.Compile()` produces a strongly-typed delegate
- Cost: O(1) per job definition at startup
- Benefit: native call performance per invocation, no boxing beyond what the job args already require

**Required tests:**
- `HandlerMetadata.Analyse` correctly classifies: no-arg handler, single JSON arg, DI service arg, `JobContext` arg, `CancellationToken` arg, `IAsyncEnumerable<T>` return, `Task<T>` return, combinations
- Compiled `Invoke` delegate calls the handler with the correct arguments — use a spy/counter
- Verify `Invoke` is only constructed once per job name (register same job twice, verify Analyse is not called twice)
- Integration: full run execution via compiled delegate produces identical results to existing tests

---

### Item 5: `InMemoryJobStore` — `ReaderWriterLockSlim` + Double-Enumeration Fix

**Why fifth:** The in-memory store must be production-quality for single-node deployments. The current single `Lock` serialises concurrent reads; `ReaderWriterLockSlim` allows all read operations to proceed concurrently.

**What changes:**

_`InMemoryJobStore.cs`:_

Replace synchronisation primitive:
```csharp
// Before
private readonly Lock _lock = new();

// After
private readonly ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);
```

Implement `IDisposable`:
```csharp
public void Dispose() => _rwLock.Dispose();
```

Classify every method:
- **Read lock** (concurrent reads permitted): `GetJobAsync`, `GetJobsAsync`, `GetRunAsync`, `GetRunsAsync`, `GetEventsAsync`, `GetBatchAsync`, `GetBatchRunIdsAsync`, `GetCompletableBatchIdsAsync`, `GetDashboardStatsAsync`, `GetQueueStatsAsync`, `GetNodesAsync`, `PingAsync`
- **Write lock** (exclusive): everything else — `UpsertJobAsync`, `CreateRunAsync`, `TryTransitionRunAsync`, `ClaimRunAsync`, `AppendEventsAsync`, `HeartbeatAsync`, `TryIncrementBatchProgressAsync`, `TryCompleteBatchAsync`, `CreateBatchAsync`, `UpsertQueueAsync`, `UpsertRateLimitAsync`, `CancelExpiredRunsWithIdsAsync`, etc.

Pattern for each read method:
```csharp
_rwLock.EnterReadLock();
try { /* read from dictionaries */ }
finally { _rwLock.ExitReadLock(); }
```

Fix double-enumeration in `GetRunsAsync`:
```csharp
// Inside read lock, after building filter chain:
var all   = query.ToList();        // single pass — filters applied once
var total = all.Count;
var items = all.Skip(skip).Take(take).ToList();
return Task.FromResult(new PagedResult<RunRecord>(items, total));
```

**Important constraint:** No method called under a read lock may call a method that acquires a write lock (no lock upgrades with `NoRecursion` policy). Verify no internal call chains violate this — the current code does not appear to have any, but this must be audited.

**Required tests:**
- Concurrent read test: 50 `GetRunAsync` tasks launched simultaneously while a write is in progress — verify no `LockRecursionException` and all reads complete
- `GetRunsAsync` pagination: total count matches actual items when result set is larger than one page — this directly covers the double-enumeration fix
- `IDisposable`: `InMemoryJobStore` disposes without throwing
- Regression: full conformance suite passes with `ReaderWriterLockSlim`

---

*Review performed by reading all 18,530 lines of source and 10,584 lines of tests across 9 test projects. Store implementations verified for schema consistency across SQLite, PostgreSQL, SQL Server, and Redis. Recommendations updated following design discussion.*
