---
title: Architecture
description: How Surefire works.
---

## Overview

Surefire has a few moving parts:

- **Jobs** are registered at startup — a name and a delegate.
- **Runs** are individual executions of a job. They go through a lifecycle: pending, running, completed/failed/cancelled.
- **Nodes** are your app instances. Each one runs a scheduler that picks up pending runs.
- **Stores** hold everything — job definitions, runs, logs. Ships with an in-memory store and a PostgreSQL store.

## How a job runs

1. A run gets created — either from a cron schedule or a `TriggerAsync` call.
2. The scheduler picks it up and claims it atomically (no two nodes grab the same run).
3. A DI scope is created, parameters are resolved, and your delegate runs.
4. Logs, progress, and the result are captured along the way.
5. When it's done, the run status is updated and any subscribers (like the dashboard) are notified.

## Run claiming

Each pending run is claimed atomically — the store updates the status to `Running` and assigns the node in a single operation. This prevents double execution even with multiple nodes polling at the same time.

The claim also respects `NotBefore` (for scheduled runs) and `MaxConcurrency` (if the job limits how many can run at once).

## Retries

When a job fails and has `MaxAttempts > 1`:

1. The current run is marked as `Failed`.
2. A new run is created with the next attempt number and a delay based on the backoff policy (fixed or exponential).
3. The retry links back to the original run via `RetryOfRunId`.
4. If all attempts are used up, the final run is marked `Dead Letter` instead of `Failed`.

## Node health

Nodes send heartbeats on an interval. If a node goes silent for longer than `StaleNodeThreshold` (default 2 minutes), another node will clean up after it:

- Runs that were still executing on the dead node get failed and retried (if retries are configured).
- Job definitions that no remaining node can handle are removed from the store. They're automatically re-created when a capable node starts up.
- The stale node is removed from the registry.

## Notifications

Surefire uses a pub/sub system (`INotificationProvider`) for real-time updates. The dashboard's live log streaming and progress updates are built on this — the SSE endpoint subscribes to a run's channels and forwards messages to the browser.

The in-memory provider works within a single process. The `Surefire.PostgreSql` package includes a PostgreSQL-backed provider using LISTEN/NOTIFY for multi-process deployments.

## Logging

Any `ILogger` output during job execution is automatically captured and stored per-run. The dashboard streams these logs in real-time. After the run completes, logs are available through the API.

## Job statuses

| Status | Meaning |
|---|---|
| Pending | Waiting to be picked up |
| Running | Executing on a node |
| Completed | Finished successfully |
| Failed | Something went wrong |
| Cancelled | Cancelled by a user or during shutdown |
| Dead Letter | Failed after all retry attempts |
