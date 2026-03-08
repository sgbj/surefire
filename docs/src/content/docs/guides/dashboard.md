---
title: Dashboard
description: The built-in Surefire dashboard.
---

## Setup

```csharp
app.MapSurefireDashboard();           // at /surefire
app.MapSurefireDashboard("/admin");   // custom prefix
```

The dashboard is embedded in the `Surefire.Dashboard` package — no extra files or build steps.

## Home

The home page gives you a quick overview:

- **Stat cards** — total jobs, total runs, active runs, and success rate.
- **Runs over time** — a stacked area chart showing runs by status. Toggle between 1h, 24h, 7d, and 30d.
- **Recent runs** — the latest runs with status badges.

![Dashboard home](../../../assets/dashboard.png)

## Jobs

Lists all registered jobs with their name, description, cron schedule, enabled/disabled status, and tags.

![Jobs list](../../../assets/jobs.png)

Click into a job to:

- **Enable or disable** it (disabling stops cron scheduling).
- **Trigger a run** with optional JSON arguments, a scheduled start time, and priority.
- See the job's **run history** with pagination.

## Runs

Lists all runs with filters for job name, status, and date range.

![Runs list](../../../assets/runs.png)

Click into a run to see:

- **Live progress bar** for running jobs.
- **Streaming logs** that update in real-time as the job runs.
- **Arguments and result** as formatted JSON.
- **Error details** for failed runs.
- **Retry chain** — links between retry attempts.
- **Triggered runs** — any child runs this job created.
- **Cancel** a running job or **re-run** a completed one.

![Run detail with progress](../../../assets/run-detail.png)

![Run detail with error](../../../assets/run-detail-error.png)

## Nodes

Lists all scheduler nodes with their last heartbeat, running job count, and registered jobs.

![Nodes list](../../../assets/nodes.png)

Click into a node to see what jobs it handles and its recent run history.

![Node detail](../../../assets/node-detail.png)

## Command palette

Press <kbd>Ctrl+K</kbd> (or <kbd>⌘K</kbd> on Mac) to open the command palette. Search for jobs, runs, or nodes and jump straight to them.

![Command palette](../../../assets/command-palette.png)

## API

The dashboard also exposes a REST API at `{prefix}/api/`:

```
GET  /api/stats                        # dashboard statistics
GET  /api/jobs                         # list all jobs
GET  /api/jobs/{name}                  # get a single job
PATCH /api/jobs/{name}                 # update a job (enable/disable)
POST /api/jobs/{name}/trigger          # trigger a new run
GET  /api/runs?jobName=X&take=20       # list runs with filters
GET  /api/runs/{id}                    # get a single run
GET  /api/runs/{id}/logs               # get parsed log events
GET  /api/runs/{id}/stream             # live logs & progress (SSE)
GET  /api/runs/{id}/trace              # get the trace for a run
POST /api/runs/{id}/cancel             # cancel a running job
POST /api/runs/{id}/rerun              # re-run a completed run
GET  /api/nodes                        # list all nodes
GET  /api/nodes/{name}                 # get a single node
```
