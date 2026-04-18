---
title: Architecture
description: Core runtime components and execution flow.
---

## Core concepts

- **Jobs** are registered at startup with a name and a delegate. Think of them as function definitions.
- **Runs** are individual executions of a job. Each run tracks its status, arguments, result, logs, and timing.
- **Batches** are grouped runs created by `TriggerBatchAsync` and related APIs. A batch has a batch record and child runs.
- **Queues** route jobs to specific worker pools with priority, concurrency limits, rate limiting, and pause/resume.
- **Nodes** are your app instances. Each one runs scheduler and executor services that process runs.
- **Stores** hold everything: jobs, runs, events, and node state. Surefire ships with in-memory, PostgreSQL, SQL Server, Redis, and SQLite stores.

## How a job runs

1. A run gets created, either from a cron schedule or a client call (`TriggerAsync`, `RunAsync`, `TriggerBatchAsync`, etc.).
2. A scheduler picks up the run on an available node.
3. Your delegate executes. Logs, progress, and the result are captured along the way.
4. The run status is updated and any subscribers (like the dashboard) are notified.

## Notifications

Surefire uses a pub/sub abstraction for real-time updates. In-memory notifications work in a single process. PostgreSQL and Redis providers include cross-node notifications.

## Logging

`ILogger` output during job execution is captured per run and streamed live in the dashboard.

## Stores

Stores keep job and run state. Every store implementation supports the same features. The in-memory store holds state in the hosting process and loses it when the process stops. The database-backed stores persist state across restarts and coordinate across nodes:

- **In-memory** — default store, process-local, non-durable.
- [PostgreSQL](/surefire/storage/postgresql/) — database-backed store with `LISTEN/NOTIFY` notifications.
- [SQL Server](/surefire/storage/sqlserver/) — database-backed store for SQL Server deployments.
- [Redis](/surefire/storage/redis/) — Redis-backed store with Pub/Sub notifications.
- [SQLite](/surefire/storage/sqlite/) — file-backed store.
