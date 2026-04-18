---
title: Resilience
description: How Surefire handles transient failures, retries, and store outages.
---

Surefire handles transient store failures automatically. No external resilience library is required.

## Transient error classification

Each store backend classifies its own transient exceptions via `IJobStore.IsTransientException`. When a transient error occurs in an internal service loop (executor, maintenance, retention, or log pump), Surefire retries with exponential backoff instead of crashing the service.

| Store | Transient errors |
|---|---|
| PostgreSQL | Serialization failure (`40001`), deadlock (`40P01`) |
| SQL Server | Timeout (`-2`), deadlock (`1205`), lock timeout (`1222`), and transient cloud errors |
| SQLite | `SQLITE_BUSY` (5), `SQLITE_LOCKED` (6) |
| In-memory / Redis | None (errors are not retried) |

Non-transient exceptions propagate immediately and stop the affected service, which is the correct behavior for programming errors or permanent failures.

## Backoff strategy

All internal service loops use exponential backoff with jitter:

- **Executor**: 500 ms initial, 30 s cap
- **Maintenance**: 1 s initial, 30 s cap
- **Retention**: 1 s initial, 30 s cap
- **Log pump**: 200 ms initial, 10 s cap
- **PostgreSQL notifications**: 200 ms initial, 30 s cap

Jitter prevents synchronized retry storms across nodes. The backoff counter resets after each successful operation.

## Log event buffering

Job log output flows through a bounded in-memory channel (capacity 8,192 entries) before being flushed to the store. If the channel fills during a store outage, new entries are dropped with `DropWrite` semantics — the oldest buffered entries are preserved and the newest are shed. Dropped entries are recorded via the `surefire.log_entries.dropped` metric with a `surefire.drop.reason` tag (`channel_full` or `flush_failed`).

## Observability

Surefire emits a `surefire.store.retries` counter tagged by `surefire.service` (executor, maintenance, retention, pump). Use this to alert on sustained store connectivity issues. See [Metrics and tracing](/surefire/guides/metrics/) for setup.

## SQLite busy handling

The SQLite store sets `PRAGMA busy_timeout = 24000`, which tells SQLite's native busy handler to wait up to 24 seconds before returning `SQLITE_BUSY`. This is handled at the C library level with sub-millisecond granularity, far more efficient than application-layer retries.

## No external dependencies

Surefire does not depend on Polly, `Microsoft.Extensions.Resilience`, or any other resilience library. The retry logic is minimal and purpose-built for the specific failure modes each store backend produces.
