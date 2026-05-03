# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
