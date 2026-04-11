# Surefire repository review

This review is based on the current working tree as of 2026-04-11. The goal assumed here is: **make Surefire the strongest possible modern .NET 10+ distributed job scheduling library**, with breaking changes allowed before first release.

## Overall assessment

Surefire already has several things that make it attractive:

- strong conformance/integration test coverage
- a compelling batch + event model
- modern .NET 10 choices (`TimeProvider`, nullable, analyzers, central package management)
- a useful dashboard story
- provider breadth (in-memory, PostgreSQL, SQL Server, SQLite, Redis)

But I would **not** call it release-ready yet. The highest-value issues are:

1. a real runtime contract failure in batch late-output streaming
2. public API/docs drift and surface-area bloat
3. insecure-by-example dashboard usage
4. platform-sensitive time-zone behavior
5. very large core/provider classes that will slow iteration and make bugs harder to remove

---

## Highest-priority findings

### 1. Release blocker: current baseline has a failing contract test

**Why it matters:** this is the clearest signal that a promised runtime behavior is not stable.

**Evidence**

- `dotnet test surefire.slnx -nologo` currently has **1 failing test**
- failing test: `Surefire.Tests.JobClientContractTests.StreamEachAsync_TerminalCoordinator_GracePoll_CapturesLateOutput`
- test location: `test\Surefire.Tests\JobClientContractTests.cs:941-1015`
- related implementation: `src\Surefire\JobClient.cs:679-711`

**Problem**

The batch stream path is supposed to capture output that arrives after the batch is already terminal, during a grace-poll window. That contract currently fails in baseline. This is especially important because late-output/event ordering is one of the more subtle distributed-runtime behaviors users will rely on.

**Recommendation**

- treat this as a pre-release blocker
- stabilize `StreamEachAsync<T>` terminal-drain behavior before adding more batch API surface
- add provider-parity coverage specifically for late output and terminal drain behavior

---

### 2. Docs and public API are out of sync

**Why it matters:** users will judge the library first by the README/docs and package API. If examples do not compile against `IJobClient`, confidence drops fast.

**Evidence**

- docs use `TriggerAllAsync`, `RunAllAsync<T>`, and `RunEachAsync`: `docs\src\content\docs\guides\jobs.md:124-144`
- public client contract does **not** expose those methods: `src\Surefire\IJobClient.cs:7-60`
- implementation still contains internal/public-adjacent batch helpers in `src\Surefire\JobClient.cs`

**Problem**

The docs present one batch model, while the main public abstraction presents another. That creates confusion around what the actual supported client API is.

**Recommendation**

Pick one of these two directions and commit to it:

1. **minimal surface:** update docs to match `IJobClient` exactly
2. **richer batch API:** promote the intended batch methods into the supported public contract and document them there

Do not keep “hidden but effectively public” concepts as the main documented story.

---

### 3. `IJobClient` is already too large for a “minimal API” library

**Why it matters:** a minimal .NET library wins when the default path is obvious.

**Evidence**

- `src\Surefire\IJobClient.cs:7-60`
- overlapping concepts: trigger vs run vs wait vs stream vs batch stream vs batch wait
- several members are public but hidden with `EditorBrowsable(EditorBrowsableState.Never)`

**Problem**

The API already asks users to understand too many adjacent concepts:

- `TriggerAsync`
- `RunAsync`
- `WaitAsync`
- `StreamAsync`
- `RunBatchAsync`
- `StreamBatchAsync`
- `WaitEachAsync`
- `StreamEachAsync`
- `ObserveAsync`
- `StreamBatchEventsAsync`

That makes discovery harder, and the `EditorBrowsable(Never)` members are a sign the design wants fewer public methods than it currently has.

**Recommendation**

Collapse toward a smaller contract, for example:

- `TriggerAsync`
- `RunAsync<T>`
- `GetRunAsync`
- `GetRunsAsync`
- `CancelAsync`
- `WaitAsync`
- one clear batch primitive
- one clear streaming primitive

Then layer advanced observation/cursor APIs elsewhere, or hide them behind dedicated handles rather than the main client.

---

### 4. Dashboard security story is too weak by default

**Why it matters:** dashboards that can trigger/cancel jobs are admin surfaces.

**Evidence**

- dashboard endpoints expose mutating operations in `src\Surefire.Dashboard\DashboardEndpoints.cs`
- no built-in auth guidance in endpoint mapping
- README examples show `app.MapSurefireDashboard();` directly: `README.md:14`, `README.md:88`, `README.md:165-166`
- no `RequireAuthorization` usage in `src\Surefire.Dashboard\DashboardEndpoints.cs`

**Problem**

The endpoints are map-group based and can be protected externally, but the examples currently make it too easy for adopters to expose a powerful admin surface without an authorization policy.

**Recommendation**

- keep the API composable, but document secure usage everywhere
- show:

```csharp
app.MapSurefireDashboard()
   .RequireAuthorization("SurefireAdmin");
```

- add dashboard tests/examples for protected deployment
- consider an overload or helper that nudges users toward auth by default

This is less about hard-coding auth into the library and more about making the safe path the obvious path.

---

### 5. Time zone handling is likely platform-sensitive and under-specified

**Why it matters:** scheduling bugs are credibility killers.

**Evidence**

- docs/comments say IANA IDs: `src\Surefire\JobBuilder.cs:26`, `src\Surefire\JobDefinition.cs:29`
- runtime resolves with `TimeZoneInfo.FindSystemTimeZoneById(...)`: `src\Surefire\SurefireSchedulerService.cs:165-180`
- dashboard also resolves directly with `TimeZoneInfo.FindSystemTimeZoneById(...)`: `src\Surefire.Dashboard\DashboardModels.cs:196-206`
- CI only runs on Ubuntu: `.github\workflows\ci.yml:13-42`

**Problem**

The code/documentation imply a specific time-zone contract, but the implementation depends on platform zone IDs. Even if this works on Linux, it is not proven in Windows CI, and the dashboard/runtime may not behave identically across environments.

**Recommendation**

- define exactly which zone identifiers are supported
- normalize/validate time zones at registration time, not later in scheduler/dashboard paths
- add Windows CI and DST-focused scheduler tests
- avoid duplicated resolution logic in scheduler and dashboard

---

## Important medium-priority findings

### 6. Health checks are only partial for a distributed system

**Evidence**

- `src\Surefire\SurefireHealthCheck.cs:5-36`

**Problem**

The health check validates store reachability and local node heartbeat, but not notification infrastructure. A node can be “healthy” while cross-node wakeups are degraded or broken.

**Recommendation**

- include notification-provider health where possible
- degrade rather than mark fully healthy when the store is fine but wakeup delivery is compromised

---

### 7. Core and provider classes are too large

**Evidence**

- `src\Surefire\JobClient.cs` — **1454** lines
- `src\Surefire\SurefireExecutorService.cs` — **1073**
- `src\Surefire\InMemoryJobStore.cs` — **1685**
- `src\Surefire.PostgreSql\PostgreSqlJobStore.cs` — **1860**
- `src\Surefire.Sqlite\SqliteJobStore.cs` — **1917**
- `src\Surefire.SqlServer\SqlServerJobStore.cs` — **2045**
- `src\Surefire.Redis\RedisJobStore.cs` — **3071**
- `src\Surefire.Dashboard\DashboardEndpoints.cs` — **624**

**Problem**

This is the main maintainability drag in the repo. Even if the code is good today, these files make it harder to reason about correctness, harder to review, and harder to keep provider behavior aligned.

**Recommendation**

Split by responsibility:

- `JobClient`: trigger/query/wait/batch/stream responsibilities
- `SurefireExecutorService`: claim loop, invocation pipeline, failure/retry handling, shutdown handling
- stores: shared SQL fragments/helpers, claim logic, event queries, rate-limit logic, mapping logic
- dashboard: endpoint registration vs response shaping vs static asset delivery

---

### 8. Provider operational consistency still needs work

**Problem**

The provider implementations are powerful, but the operational story is uneven:

- Redis notification/provider resilience could be stronger
- timeout/retry behavior differs by provider
- rate-limit decisions are not especially observable
- Redis retention/key cleanup still needs careful attention

**Recommendation**

- add explicit provider-behavior docs: polling vs notifications, retries, timeout handling, durability tradeoffs
- add provider-parity tests around claim ordering, dedup, rate limits, retention, and terminal event delivery
- invest in metrics/logging around rate-limit and wakeup paths

---

### 9. CI/platform coverage is too narrow for infrastructure software

**Evidence**

- `.github\workflows\ci.yml:13-42` uses only `ubuntu-latest`

**Problem**

For a .NET 10 scheduling library with time-zone logic and multiple providers, Linux-only CI is not enough. Windows support should be proven, not assumed.

**Recommendation**

- run CI on Windows and Linux
- if feasible, split fast matrix vs full integration matrix
- specifically validate scheduler/time-zone behavior on Windows

---

### 10. Release/package hygiene is still incomplete

**Evidence**

- no explicit package version metadata found in project/package config during review
- dashboard UI package is placeholder version `0.0.0`: `src\Surefire.Dashboard\ui\package.json:4`

**Problem**

That does not necessarily block local development, but it is not polished release hygiene for a library you want people to adopt quickly and trust.

**Recommendation**

- define explicit package/release versioning strategy
- align package/UI version metadata
- document prerelease expectations clearly in README/docs

---

## Lower-priority cleanup / simplification opportunities

### 11. Hidden public APIs should be reduced, not just hidden

`EditorBrowsable(Never)` is useful sparingly, but here it looks like a workaround for an oversized public surface. Prefer actually moving advanced APIs behind more intentional abstractions.

### 12. Dashboard endpoint code should be decomposed

`src\Surefire.Dashboard\DashboardEndpoints.cs` is doing too much: route registration, request parsing, response shaping, event streaming, and static UI serving.

### 13. More explicit guidance on provider tradeoffs would help adoption

The README’s provider table is good, but you can go further:

- best provider for “default production”
- best provider for “single-node embedded app”
- best provider for “high-throughput low-latency”
- operational caveats for SQL Server / SQLite polling modes

### 14. Add more benchmark/perf evidence before release

This is the kind of library where users will ask:

- claim latency
- throughput under contention
- batch fan-out cost
- dashboard query cost
- store/provider comparison

Even a small benchmark suite would strengthen the project story considerably.

---

## Things worth keeping

### 1. Conformance-test strategy

This is one of the strongest parts of the repo. The shared conformance model is the right approach for multi-provider confidence.

### 2. Batch + event model

The combination of durable runs, events, batch progress, and output streaming is a real differentiator when it works reliably.

### 3. Modern .NET choices

- `net10.0`
- nullable enabled
- warnings as errors
- `TimeProvider`
- health checks
- OpenTelemetry integration

These choices fit the project’s positioning well.

### 4. Overall product direction

A minimal-API distributed job library with first-class dashboarding, observability, scheduling, retries, and batching is a strong niche. The repo already feels closer to “serious library” than “toy side project.”

---

## Recommended action order

### Before release

1. Fix the failing late-output batch streaming contract.
2. Make docs match the real public API.
3. Shrink/simplify `IJobClient`.
4. Add secure dashboard usage guidance and tests.
5. Add Windows CI and harden time-zone behavior.

### Next wave

6. Split the large core/provider classes by responsibility.
7. Improve provider operational consistency and observability.
8. Tighten release/version/package metadata.
9. Add provider-parity tests for edge-case distributed behaviors.

---

## Final verdict

**Surefire has the ingredients of a library people could genuinely choose for .NET 10+ apps, but it needs another quality pass before first release.**

The project’s biggest opportunity is not adding more features. It is:

- making the current contracts rock-solid
- reducing API ambiguity
- improving trust through docs/security/platform coverage
- and lowering maintenance risk by decomposing the biggest classes

If those areas are addressed, Surefire will be much more compelling as a modern, production-worthy distributed job scheduling library.
