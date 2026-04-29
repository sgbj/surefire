---
title: Architecture
description: Core runtime components and execution flow.
---

## Core concepts

- **Jobs** are registered at startup with a name and a delegate. Think of them as function definitions.
- **Runs** are individual executions of a job. Each run tracks its status, arguments, result, logs, and timing.
- **Batches** are grouped runs created by `TriggerBatchAsync` and the `RunBatchAsync` / `StreamBatchAsync` family. A batch has its own record plus child runs.
- **Queues** group jobs for routing, with per-queue priority, concurrency limits, rate limits, and pause/resume.
- **Nodes** are your app instances. Each one polls for work and runs whatever it has registered to handle.
- **Stores** hold everything: jobs, runs, events, and node state.

## How a job runs

1. A run is created, either from a cron schedule or a client call (`TriggerAsync`, `RunAsync`, `TriggerBatchAsync`, etc.).
2. A scheduler claims the run on an available node.
3. Your delegate executes. Logs, progress, and the result are captured along the way.
4. The run reaches a terminal status, and subscribers (the dashboard, the client that triggered it) are notified.

## Notifications

Surefire uses a pub/sub abstraction for low-latency wakeups. The in-memory provider works within a single process. PostgreSQL and Redis providers carry notifications across nodes. SQL Server and SQLite rely on the polling interval instead.

## Node recovery

Each node sends a heartbeat every `HeartbeatInterval` (default 30 seconds). If a node goes silent for longer than `InactiveThreshold` (default 2 minutes), another node picks up its in-flight runs and routes them through the retry flow.

## Logging

`ILogger` output produced during a run is captured per-run and streamed live to the dashboard.

## Stores

Every store implements the same surface. The in-memory store keeps state in the hosting process and loses it on restart. The other stores persist state and coordinate work across nodes:

- In-memory: default, process-local, non-durable.
- [PostgreSQL](/surefire/storage/postgresql/): durable store with `LISTEN`/`NOTIFY` for cross-node notifications.
- [SQL Server](/surefire/storage/sqlserver/): durable store with polling-based wakeups.
- [SQLite](/surefire/storage/sqlite/): file-backed store for single-node setups.
- [Redis](/surefire/storage/redis/): Redis-backed store with Pub/Sub notifications.
