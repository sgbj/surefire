---
title: Architecture
description: How Surefire works under the hood.
---

## Core concepts

Surefire has four main pieces:

- **Jobs** are registered at startup with a name and a delegate. Think of them as function definitions.
- **Runs** are individual executions of a job. Each run tracks its status, arguments, result, logs, and timing.
- **Nodes** are your app instances. Each one runs a scheduler that picks up pending runs and executes them.
- **Stores** hold everything. Jobs, runs, events, node state. Ships with in-memory, PostgreSQL, SQL Server, and SQLite stores.

## How a job runs

1. A run gets created, either from a cron schedule, a `TriggerAsync` call, or as part of a plan.
2. The scheduler on one of your nodes picks it up and claims it atomically. No two nodes can grab the same run.
3. A DI scope is created, parameters are resolved, and your delegate executes.
4. Logs, progress, and the result are captured along the way.
5. The run status is updated and any subscribers (like the dashboard) are notified.

## Run claiming

Each pending run is claimed atomically. The store updates the status to `Running` and assigns the node in a single operation. This prevents double execution even with many nodes polling at the same time.

The claim respects several constraints:

- **Priority** - higher priority runs are claimed first.
- **NotBefore** - scheduled runs wait until their time.
- **NotAfter** - expired runs are skipped and eventually cancelled.
- **MaxConcurrency** - per-job and per-queue concurrency limits.
- **Queue pausing** - runs in paused queues are skipped.
- **Rate limits** - job-level and queue-level rate limits are checked before claiming.

## Notifications

Surefire uses a pub/sub system for real-time updates. The dashboard's live log streaming and progress updates are built on top of this. The SSE endpoint subscribes to a run's channels and forwards events to the browser.

The in-memory provider works within a single process. For multi-node deployments, the PostgreSQL package includes a provider using `LISTEN/NOTIFY`.

## Logging

Any `ILogger` output during job execution is automatically captured and stored per run. The dashboard streams these logs in real time while the job is running. After the run completes, logs are available through the API.

## Stores

The store is the single source of truth for all Surefire state. Every store implementation supports the same features. The in-memory store is great for development but loses everything when the process stops. For production, use one of the database-backed stores:

- [PostgreSQL](/surefire/storage/postgresql/) - recommended for most deployments. Includes `LISTEN/NOTIFY` for instant job pickup.
- [SQL Server](/surefire/storage/sqlserver/) - for teams already on SQL Server.
- [SQLite](/surefire/storage/sqlite/) - file-based, good for single-node or embedded scenarios.
