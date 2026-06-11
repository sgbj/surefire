# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Dashboard authentication. The dashboard is now secure by default: it requires a browser token
  (Aspire-style) exchanged for a cookie at `{prefix}/login`. Configure via
  `SurefireDashboardOptions.AuthMode` (`BrowserToken` (default), `HostAuthorization`, or
  `Unsecured`) and `SurefireDashboardOptions.BrowserToken` (or the
  `Surefire:Dashboard:BrowserToken` configuration key).

### Breaking changes

- The dashboard requires authentication by default. On upgrade: hosts that already chain their
  own `.RequireAuthorization(...)` on `MapSurefireDashboard()` should set
  `options.AuthMode = DashboardAuthMode.HostAuthorization` (chained policies otherwise combine
  with the built-in token policy); local-dev setups that want the old open behavior must opt in
  explicitly with `options.AuthMode = DashboardAuthMode.Unsecured`. With no configuration, the
  login URL (including a generated token) is logged at startup.

## [0.4.0] - 2026-06-07

### Added

- Durable orchestration: mark a job `.Durable()` to coordinate multi-step workflows that trigger child runs, suspend while they wait, and replay from recorded history, surviving restarts and crashes.
- Replay-safe helpers on `JobContext` — `RecordAsync`, `NewGuidAsync`, `NewGuidV7Async`, `GetUtcNowAsync`, `NextInt32Async`, and `NextDoubleAsync` — plus `IsDurable` and `IsReplaying` for deterministic durable handlers.
- `Suspended` run status for durable runs waiting on children, surfaced across the dashboard timeline and status filters; suspended runs are not claimed by workers and hold no concurrency slot.
- Run expiration via `RunOptions.ExpiresAt`, `BatchRunOptions.ExpiresAt`, and `SurefireOptions.RunExpirationPeriod` to cancel runs that outlive a deadline.
- Job source code on the dashboard through `JobDefinition.SourceCode`.
- `DurableExecutionSnapshot`, `DurableRecord`, `DurableStepRecord`, `DurableReplayMismatchException`, `DurableSuspendOutcome`, `InputPumpArgumentState`, and `InvalidInputHistoryException` for the durable execution and input-history APIs.

### Changed

- `IJobStore` now coordinates durable runs: status transitions key off a lease epoch (`RunStatusTransition.ExpectedLeaseEpoch`), and run/batch creation records durable steps. New `TrySuspendRunAsync`, `CreateDurableRecordAsync`, `LoadExecutionSnapshotAsync`, and `GetInputPumpStateAsync` members support suspension and replay.
- The dashboard shows job source code and reports durable suspension, run expiration, and replay/failure counts, alongside further UI improvements.

### Breaking changes

These affect custom `IJobStore` implementations only; applications using the built-in stores are unaffected.

- Run status transitions now key off a lease epoch instead of an attempt number. `RunStatusTransition.ExpectedAttempt` (`int`) is replaced by `ExpectedLeaseEpoch` (`long`), and the `RunStatusTransition` factory methods (`PendingToRunning`, `RunningToSucceeded`, `RunningToFailed`, `RunningToPending`, `ToCanceled`) take `long expectedLeaseEpoch` in place of `int expectedAttempt`.
- `IJobStore.TryCancelRunAsync` now takes `long? expectedLeaseEpoch` instead of `int? expectedAttempt`.
- `IJobStore.CreateBatchAsync` and `IJobStore.TryCreateRunAsync` now take an optional `DurableStepRecord?` parameter.
- `IJobStore` gains new required members for durable orchestration: `TrySuspendRunAsync`, `CreateDurableRecordAsync`, `LoadExecutionSnapshotAsync`, `GetInputPumpStateAsync`, and `AppendEventsIfRunNonTerminalAsync`.

## [0.3.0] - 2026-05-14

### Added

- Native AOT and trimming support for the core package, storage providers, and dashboard.
- Source generation for trim-safe job registration, lifecycle callbacks, `IJobClient` calls, and `BatchItem.Create`.
- `RunArguments` and descriptor-based APIs for pre-serialized arguments and AOT-safe job and callback registration.
- `services.AddSurefireDashboard(configure?)` for dashboard services, options, and JSON metadata.

### Changed

- Reflection-based registration and client APIs remain available, and now report AOT/trim warnings when analyzers are enabled unless Surefire can replace the call with generated code.
- Dashboard setup now registers its JSON metadata through DI and enables ASP.NET Core request delegate generation for AOT builds.

### Fixed

- Cron jobs using the default `Skip` misfire policy now enqueue the next scheduled occurrence while still skipping backlog.

### Breaking changes

- `MapSurefireDashboard(prefix, configure)` was removed. Configure dashboard options with `services.AddSurefireDashboard(...)`, then call `app.MapSurefireDashboard(prefix?)`.
- `BatchItem` now stores `RunArguments?` instead of `object?`. Use `BatchItem.Create(...)` for object arguments, or construct `BatchItem` with pre-built `RunArguments`.

## [0.2.0] - 2026-05-10

### Added

- `RunOrderDirection` enum and `RunFilter.Direction` for ascending or descending ordering on `IJobStore.GetRunsAsync`.
- `SurefireDashboardOptions.MaxTreeRuns` (default `50_000`) to cap the run tree response, configurable via a new `configure` parameter on `MapSurefireDashboard`.
- `GET {prefix}/api/runs/{id}/tree` returns the full run tree in a single response with `Truncated` and `TotalCount`.

### Changed

- `MapSurefireDashboard` signature is now `(prefix, configure)`; callers passing only the prefix are unaffected.
- Dashboard trace view shows the whole run tree with live updates.
- Dashboard UI rework

### Removed

- `GET {prefix}/api/runs/{id}/trace` and `GET {prefix}/api/runs/{id}/children` endpoints (use `/tree`).
- Public response types `RunTraceResponse`, `RunChildrenResponse`, and `SiblingsCursorResponse`.

## [0.1.0-preview.1] - 2026-05-02

First public preview. APIs and storage schemas may still change before 1.0.

### Added

- `AddSurefire` and `AddJob` for registering jobs as delegates with parameters resolved from DI and run arguments.
- Per-job configuration: cron schedules with timezones, retries with backoff and jitter, timeouts, queues, rate limits, max concurrency, priorities, tags, and continuous jobs.
- Misfire policies (`Skip`, `FireOnce`, `FireAll`) with an optional cap on `FireAll` catch-up.
- `IJobClient` for triggering, running, and observing jobs from anywhere, including from inside other jobs.
- Streaming with `IAsyncEnumerable<T>` for both inputs and outputs, plus batch helpers (`RunBatchAsync`, `StreamBatchAsync`).
- Lifecycle callbacks (`OnSuccess`, `OnRetry`, `OnDeadLetter`) per job and globally.
- Filter pipeline (`IJobFilter`) for cross-cutting behavior at the global or per-job level.
- Cascade cancellation across parent and child runs, run expiration via `NotAfter`, and deduplication via `DeduplicationId`.
- Multi-node coordination with claim/retry handling, heartbeats, and stale-node recovery.
- Embedded dashboard with live logs, progress, run history, trace tree, queues, nodes, and a command palette. Includes a REST API at `{prefix}/api/`.
- Storage providers: in-memory (default), PostgreSQL (with `LISTEN`/`NOTIFY`), SQL Server, SQLite, and Redis (with Pub/Sub).
- OpenTelemetry metrics and traces, plus an ASP.NET Core health check.
