using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;
using Surefire;
using Surefire.Dashboard;

namespace Microsoft.AspNetCore.Builder;

public static class DashboardEndpoints
{
    private const int TracePageSize = 500;

    public static IEndpointConventionBuilder MapSurefireDashboard(this IEndpointRouteBuilder endpoints,
        string prefix = "/surefire")
    {
        var group = endpoints.MapGroup(prefix);
        var api = group.MapGroup("api");

        api.MapGet("/stats", async (DateTimeOffset? since, int? bucketMinutes, IJobStore store, CancellationToken ct) =>
        {
            var stats = await store.GetDashboardStatsAsync(since, bucketMinutes ?? 60, ct);
            var recentRunsPage = await store.GetRunsAsync(
                new() { OrderBy = RunOrderBy.CreatedAt },
                0,
                15,
                ct);

            return TypedResults.Ok(DashboardStatsResponse.From(stats, recentRunsPage.Items));
        });

        api.MapGet("/jobs",
            async (string? name, string? tag, bool? isEnabled, bool? includeInactive, IJobStore store,
                SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
            {
                var now = timeProvider.GetUtcNow();
                var cutoff = now - surefireOpts.InactiveThreshold;
                var filter = new JobListFilter { Name = name, Tag = tag, IsEnabled = isEnabled };
                if (includeInactive != true)
                {
                    filter.HeartbeatAfter = cutoff;
                }

                var jobs = await store.GetJobsAsync(filter, ct);
                return TypedResults.Ok(jobs.Select(j => JobResponse.From(j, cutoff, now)).ToList());
            });

        api.MapGet("/jobs/{name}", async Task<Results<Ok<JobResponse>, ProblemHttpResult>> (string name,
            IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
        {
            var job = await store.GetJobAsync(name, ct);
            if (job is null)
            {
                return NotFoundProblem($"Job '{name}' was not found.");
            }

            var now = timeProvider.GetUtcNow();
            var cutoff = now - surefireOpts.InactiveThreshold;
            return TypedResults.Ok(JobResponse.From(job, cutoff, now));
        });

        api.MapPatch("/jobs/{name}", async Task<Results<Ok<JobResponse>, ProblemHttpResult>> (string name,
            UpdateJobRequest request, IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider,
            CancellationToken ct) =>
        {
            var job = await store.GetJobAsync(name, ct);
            if (job is null)
            {
                return NotFoundProblem($"Job '{name}' was not found.");
            }

            if (request.IsEnabled is { })
            {
                await store.SetJobEnabledAsync(name, request.IsEnabled.Value, ct);
            }

            job = await store.GetJobAsync(name, ct);
            if (job is null)
            {
                return NotFoundProblem($"Job '{name}' was not found.");
            }

            var now = timeProvider.GetUtcNow();
            var cutoff = now - surefireOpts.InactiveThreshold;
            return TypedResults.Ok(JobResponse.From(job, cutoff, now));
        });

        api.MapGet("/jobs/{name}/stats",
            async Task<Results<Ok<JobStatsResponse>, ProblemHttpResult>> (string name, IJobStore store,
                CancellationToken ct) =>
            {
                var job = await store.GetJobAsync(name, ct);
                if (job is null)
                {
                    return NotFoundProblem($"Job '{name}' was not found.");
                }

                return TypedResults.Ok(JobStatsResponse.From(await store.GetJobStatsAsync(name, ct)));
            });

        api.MapPost("/jobs/{name}/trigger",
            async Task<Results<Ok<RunIdResponse>, ProblemHttpResult>> (string name, TriggerJobRequest? request,
                IJobClient client, CancellationToken ct) =>
            {
                object? args = request?.Args;
                RunOptions? runOptions = null;

                if (request?.NotBefore is { } || request?.NotAfter is { } || request?.Priority is { } ||
                    request?.DeduplicationId is { })
                {
                    runOptions = new()
                    {
                        NotBefore = request.NotBefore,
                        NotAfter = request.NotAfter,
                        Priority = request.Priority,
                        DeduplicationId = request.DeduplicationId
                    };
                }

                try
                {
                    var runId = runOptions is { }
                        ? await client.TriggerAsync(name, args, runOptions, ct)
                        : await client.TriggerAsync(name, args, ct);
                    return TypedResults.Ok(new RunIdResponse(runId));
                }
                catch (InvalidOperationException ex)
                {
                    return ConflictProblem(ex.Message);
                }
            });

        api.MapGet("/runs", async (string? jobName, bool? exactJobName, JobStatus? status, string? nodeName,
            string? parentRunId, int? skip, int? take, DateTimeOffset? createdAfter, DateTimeOffset? createdBefore,
            IJobStore store, CancellationToken ct) =>
        {
            var filter = new RunFilter
            {
                JobName = jobName,
                ExactJobName = exactJobName ?? false,
                Status = status,
                NodeName = nodeName,
                ParentRunId = parentRunId,
                OrderBy = RunOrderBy.CreatedAt,
                CreatedAfter = createdAfter,
                CreatedBefore = createdBefore
            };
            var requestedTake = take is null ? 50 : Math.Max(take.Value, 1);
            var runsPage = await store.GetRunsAsync(filter, Math.Max(skip ?? 0, 0), requestedTake, ct);
            return TypedResults.Ok(new PagedResponse<RunResponse>
            {
                Items = runsPage.Items.Select(RunResponse.From).ToList(),
                TotalCount = runsPage.TotalCount
            });
        });

        api.MapGet("/runs/{id}",
            async Task<Results<Ok<RunResponse>, ProblemHttpResult>> (string id, IJobStore store,
                CancellationToken ct) =>
            {
                var run = await store.GetRunAsync(id, ct);
                return run is null
                    ? NotFoundProblem($"Run '{id}' was not found.")
                    : TypedResults.Ok(RunResponse.From(run));
            });

        api.MapGet("/runs/{id}/trace",
            async Task<Results<Ok<List<RunResponse>>, ProblemHttpResult>> (string id, int? limit, IJobStore store,
                TimeProvider timeProvider, CancellationToken ct) =>
            {
                var run = await store.GetRunAsync(id, ct);
                if (run is null)
                {
                    return NotFoundProblem($"Run '{id}' was not found.");
                }

                var requestedLimit = limit is null ? (int?)null : Math.Max(limit.Value, 1);
                var snapshotCreatedBefore = timeProvider.GetUtcNow().AddTicks(1);
                var rootRunId = run.RootRunId ?? run.Id;
                var rootRun = run.RootRunId is null ? run : await store.GetRunAsync(rootRunId, ct);
                if (rootRun is null)
                {
                    return NotFoundProblem($"Root run '{rootRunId}' was not found.");
                }

                if (requestedLimit == 1)
                {
                    return TypedResults.Ok(new List<RunResponse> { RunResponse.From(rootRun) });
                }

                var descendants = new List<RunResponse>();
                var skip = 0;
                var remaining = requestedLimit is null ? (int?)null : requestedLimit.Value - 1;

                while (remaining is null || remaining.Value > 0)
                {
                    var take = remaining is null ? TracePageSize : Math.Min(TracePageSize, remaining.Value);
                    var descendantsPage = await store.GetRunsAsync(
                        new()
                        {
                            RootRunId = rootRunId,
                            OrderBy = RunOrderBy.CreatedAt,
                            CreatedBefore = snapshotCreatedBefore
                        },
                        skip,
                        take,
                        ct);

                    if (descendantsPage.Items.Count == 0)
                    {
                        break;
                    }

                    descendants.AddRange(descendantsPage.Items.Select(RunResponse.From));
                    skip += descendantsPage.Items.Count;
                    if (remaining is { })
                    {
                        remaining -= descendantsPage.Items.Count;
                    }

                    if (skip >= descendantsPage.TotalCount)
                    {
                        break;
                    }
                }

                var trace = new List<RunResponse>(descendants.Count + 1) { RunResponse.From(rootRun) };
                trace.AddRange(descendants);
                return TypedResults.Ok(trace);
            });

        api.MapPost("/runs/{id}/cancel",
            async Task<Results<NoContent, ProblemHttpResult>> (string id, IJobClient client, CancellationToken ct) =>
            {
                try
                {
                    await client.CancelAsync(id, ct);
                    return TypedResults.NoContent();
                }
                catch (RunNotFoundException ex)
                {
                    return NotFoundProblem(ex.Message);
                }
                catch (RunConflictException ex)
                {
                    return ConflictProblem(ex.Message);
                }
            });

        api.MapPost("/runs/{id}/rerun",
            async Task<Results<Ok<RunIdResponse>, ProblemHttpResult>> (string id, IJobClient client,
                CancellationToken ct) =>
            {
                try
                {
                    var newRunId = await client.RerunAsync(id, ct);
                    return TypedResults.Ok(new RunIdResponse(newRunId));
                }
                catch (RunNotFoundException ex)
                {
                    return NotFoundProblem(ex.Message);
                }
                catch (RunConflictException ex)
                {
                    return ConflictProblem(ex.Message);
                }
            });

        api.MapGet("/runs/{id}/logs",
            async Task<Results<Ok<List<JsonElement>>, ProblemHttpResult>> (string id, IJobStore store,
                ILoggerFactory loggerFactory, CancellationToken ct) =>
            {
                var run = await store.GetRunAsync(id, ct);
                if (run is null)
                {
                    return NotFoundProblem($"Run '{id}' was not found.");
                }

                var logger = loggerFactory.CreateLogger(typeof(DashboardEndpoints));
                var events = await store.GetEventsAsync(id, types: [RunEventType.Log], cancellationToken: ct);
                var logs = new List<JsonElement>(events.Count);
                foreach (var @event in events)
                {
                    try
                    {
                        logs.Add(JsonSerializer.Deserialize<JsonElement>(@event.Payload));
                    }
                    catch (JsonException ex)
                    {
                        logger.LogWarning(ex,
                            "Skipping malformed log payload for run '{RunId}', event '{EventId}'.",
                            id,
                            @event.Id);
                    }
                }

                return TypedResults.Ok(logs);
            });

        api.MapGet("/runs/{id}/stream", async Task<Results<ProblemHttpResult, EmptyHttpResult>> (string id,
            long? sinceEventId, HttpResponse response, INotificationProvider notifications, IJobStore store,
            SurefireOptions surefireOptions, ILoggerFactory loggerFactory, CancellationToken ct) =>
        {
            var run = await store.GetRunAsync(id, ct);
            if (run is null)
            {
                return NotFoundProblem($"Run '{id}' was not found.");
            }

            var logger = loggerFactory.CreateLogger(typeof(DashboardEndpoints));

            response.ContentType = "text/event-stream";
            response.Headers.CacheControl = "no-cache";

            var pollingInterval = surefireOptions.PollingInterval;
            using var wakeup = new SemaphoreSlim(0, 1);

            await using var eventSub = await notifications.SubscribeAsync(
                NotificationChannels.RunEvent(id),
                _ =>
                {
                    try
                    {
                        wakeup.Release();
                    }
                    catch (Exception ex) when (ex is SemaphoreFullException or ObjectDisposedException)
                    {
                        logger.LogTrace(ex,
                            "Ignored wakeup release error for run stream '{RunId}' on run event subscription.",
                            id);
                    }

                    return Task.CompletedTask;
                },
                ct);

            var completedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using var completedSub = await notifications.SubscribeAsync(
                NotificationChannels.RunCompleted(id),
                _ =>
                {
                    completedTcs.TrySetResult();
                    try
                    {
                        wakeup.Release();
                    }
                    catch (Exception ex) when (ex is SemaphoreFullException or ObjectDisposedException)
                    {
                        logger.LogTrace(ex,
                            "Ignored wakeup release error for run stream '{RunId}' on completion subscription.",
                            id);
                    }

                    return Task.CompletedTask;
                },
                ct);

            long lastSeenId = 0;
            if (long.TryParse(response.HttpContext.Request.Headers["Last-Event-ID"], out var resumeId))
            {
                lastSeenId = resumeId;
            }
            else if (sinceEventId is > 0)
            {
                lastSeenId = sinceEventId.Value;
            }

            try
            {
                // Send all existing events (resumes from Last-Event-ID on reconnect)
                var existingEvents = await store.GetEventsAsync(id, lastSeenId, cancellationToken: ct);
                foreach (var evt in existingEvents)
                {
                    await WriteEventAsync(response, evt, ct);
                    lastSeenId = evt.Id;
                }

                await response.Body.FlushAsync(ct);

                // Check if already terminal and drain events appended after initial fetch.
                run = await store.GetRunAsync(id, ct);
                if (run is { } && run.Status.IsTerminal)
                {
                    lastSeenId = await DrainTerminalEventsUntilStableAsync(id, lastSeenId, response, store,
                        surefireOptions.PollingInterval, ct);

                    await response.WriteAsync("event: done\ndata: {}\n\n", ct);
                    return TypedResults.Empty;
                }

                // Poll-on-wake loop
                await using var reg = ct.Register(() => completedTcs.TrySetCanceled(ct));

                while (!ct.IsCancellationRequested)
                {
                    await wakeup.WaitAsync(pollingInterval, ct);

                    var newEvents = await store.GetEventsAsync(id, lastSeenId, cancellationToken: ct);
                    foreach (var evt in newEvents)
                    {
                        await WriteEventAsync(response, evt, ct);
                        lastSeenId = evt.Id;
                    }

                    if (newEvents.Count > 0)
                    {
                        await response.Body.FlushAsync(ct);
                    }
                    else
                    {
                        // Send keepalive comment to prevent proxies/load balancers
                        // from closing idle connections (nginx default: 60s)
                        await response.WriteAsync(": keepalive\n\n", ct);
                        await response.Body.FlushAsync(ct);
                    }

                    // Check if run completed
                    if (completedTcs.Task.IsCompleted)
                    {
                        lastSeenId = await DrainTerminalEventsUntilStableAsync(id, lastSeenId, response, store,
                            surefireOptions.PollingInterval, ct);

                        await response.WriteAsync("event: done\ndata: {}\n\n", CancellationToken.None);
                        return TypedResults.Empty;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (IOException)
            {
            } // Client disconnected mid-write

            return TypedResults.Empty;
        });

        api.MapGet("/queues",
            async (IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
            {
                var queues = await store.GetQueuesAsync(ct);
                var queueStats = await store.GetQueueStatsAsync(ct);
                var nodes = await store.GetNodesAsync(ct);
                var activeCutoff = timeProvider.GetUtcNow() - surefireOpts.InactiveThreshold;

                // Map active nodes to queues
                var nodesByQueue = new Dictionary<string, List<string>>();
                foreach (var node in nodes.Where(n => n.LastHeartbeatAt >= activeCutoff))
                {
                    foreach (var queueName in node.RegisteredQueueNames)
                    {
                        if (!nodesByQueue.TryGetValue(queueName, out var list))
                        {
                            nodesByQueue[queueName] = list = [];
                        }

                        list.Add(node.Name);
                    }
                }

                // Merge explicit queue definitions with implicit ones (like "default")
                var allQueueNames = new HashSet<string>(queues.Select(q => q.Name));
                foreach (var key in queueStats.Keys)
                {
                    allQueueNames.Add(key);
                }

                foreach (var key in nodesByQueue.Keys)
                {
                    allQueueNames.Add(key);
                }

                var result = allQueueNames.Select(name =>
                {
                    var def = queues.FirstOrDefault(q => q.Name == name);
                    queueStats.TryGetValue(name, out var stats);
                    return new QueueResponse
                    {
                        Name = name,
                        Priority = def?.Priority ?? 0,
                        MaxConcurrency = def?.MaxConcurrency,
                        IsPaused = def?.IsPaused ?? false,
                        RateLimitName = def?.RateLimitName,
                        PendingCount = stats?.PendingCount ?? 0,
                        RunningCount = stats?.RunningCount ?? 0,
                        ProcessingNodes = nodesByQueue.GetValueOrDefault(name, [])
                    };
                }).OrderByDescending(q => q.Priority).ThenBy(q => q.Name).ToList();

                return TypedResults.Ok(result);
            });

        api.MapPatch("/queues/{name}",
            async (string name, UpdateQueueRequest request, IJobStore store, CancellationToken ct) =>
            {
                var queue = (await store.GetQueuesAsync(ct)).FirstOrDefault(q => q.Name == name);

                // Implicit queues (e.g. "default") aren't in the store — materialize them so we can update properties.
                if (queue is null)
                {
                    queue = new() { Name = name };
                    await store.UpsertQueueAsync(queue, ct);
                }

                if (request.IsPaused is { })
                {
                    await store.SetQueuePausedAsync(name, request.IsPaused.Value, ct);
                }

                return TypedResults.NoContent();
            });

        api.MapGet("/nodes",
            async (bool? includeInactive, IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider,
                CancellationToken ct) =>
            {
                var cutoff = timeProvider.GetUtcNow() - surefireOpts.InactiveThreshold;
                var nodes = await store.GetNodesAsync(ct);
                if (includeInactive is not true)
                {
                    nodes = nodes.Where(n => n.LastHeartbeatAt >= cutoff).ToList();
                }

                return TypedResults.Ok(nodes.Select(n => NodeResponse.From(n, cutoff)).ToList());
            });

        api.MapGet("/nodes/{name}", async Task<Results<Ok<NodeResponse>, ProblemHttpResult>> (string name,
            IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
        {
            var node = (await store.GetNodesAsync(ct)).FirstOrDefault(n => n.Name == name);
            if (node is null)
            {
                return NotFoundProblem($"Node '{name}' was not found.");
            }

            var cutoff = timeProvider.GetUtcNow() - surefireOpts.InactiveThreshold;
            return TypedResults.Ok(NodeResponse.From(node, cutoff));
        });

        var assembly = typeof(DashboardEndpoints).Assembly;
        var fileProvider = new ManifestEmbeddedFileProvider(assembly, "wwwroot");

        var trimmed = prefix.Trim('/');
        var basePath = trimmed.Length > 0 ? $"/{trimmed}/" : "/";

        var indexFile = fileProvider.GetFileInfo("index.html");
        byte[] indexBytes;
        using (var reader = new StreamReader(indexFile.CreateReadStream()))
        {
            var html = reader.ReadToEnd();
            indexBytes = Encoding.UTF8.GetBytes(
                Regex.Replace(html, @"<base\s+href=""[^""]*""", $"<base href=\"{basePath}\""));
        }

        var contentTypeProvider = new FileExtensionContentTypeProvider();

        group.Map("{**path}", async context =>
        {
            var path = context.Request.RouteValues["path"]?.ToString() ?? "";

            if (path == "api" || path.StartsWith("api/"))
            {
                context.Response.StatusCode = 404;
                return;
            }

            var file = fileProvider.GetFileInfo(path);
            if (!file.Exists || file.IsDirectory)
            {
                context.Response.ContentType = "text/html";
                context.Response.Headers.CacheControl = "no-cache";
                await context.Response.Body.WriteAsync(indexBytes);
                return;
            }

            if (!contentTypeProvider.TryGetContentType(file.Name, out var contentType))
            {
                contentType = "application/octet-stream";
            }

            context.Response.ContentType = contentType;
            if (path.StartsWith("assets/"))
            {
                context.Response.Headers.CacheControl = "public, max-age=31536000, immutable";
            }

            await using var stream = file.CreateReadStream();
            await stream.CopyToAsync(context.Response.Body);
        });

        return group;
    }

    private static async Task WriteEventAsync(HttpResponse response, RunEvent evt, CancellationToken ct)
    {
        var sseEventType = evt.EventType switch
        {
            RunEventType.Status => "status",
            RunEventType.Progress => "progress",
            RunEventType.Output => "output",
            RunEventType.OutputComplete => "outputComplete",
            RunEventType.Input => "input",
            RunEventType.InputComplete => "inputComplete",
            RunEventType.InputDeclared => "inputDeclared",
            RunEventType.AttemptFailure => "attemptFailure",
            _ => null // Log events use default "message" (no event: prefix)
        };

        // SSE protocol: \r\n, \r, and \n are all line terminators; each data line needs "data: " prefix.
        // Normalize \r\n and bare \r to \n before splitting.
        var payload = evt.Payload.Contains('\r')
            ? evt.Payload.Replace("\r\n", "\n").Replace('\r', '\n')
            : evt.Payload;
        var dataLines = payload.Contains('\n')
            ? string.Join('\n', payload.Split('\n').Select(line => $"data: {line}"))
            : $"data: {payload}";

        if (sseEventType is { })
        {
            await response.WriteAsync($"id: {evt.Id}\nevent: {sseEventType}\n{dataLines}\n\n", ct);
        }
        else
        {
            await response.WriteAsync($"id: {evt.Id}\n{dataLines}\n\n", ct);
        }
    }

    private static async Task<long> DrainTerminalEventsUntilStableAsync(string runId, long lastSeenId,
        HttpResponse response, IJobStore store, TimeSpan pollingInterval, CancellationToken ct)
    {
        var settleDelay = TimeSpan.FromMilliseconds(Math.Clamp(pollingInterval.TotalMilliseconds / 4d, 5, 50));
        var settleWindow = TimeSpan.FromMilliseconds(Math.Clamp(pollingInterval.TotalMilliseconds * 2d, 20, 500));
        var settleStopwatch = Stopwatch.StartNew();
        var lastEventAt = settleStopwatch.Elapsed;
        var idlePolls = 0;

        while (true)
        {
            var events = await store.GetEventsAsync(runId, lastSeenId, cancellationToken: ct);
            if (events.Count == 0)
            {
                idlePolls++;
                if (idlePolls >= 2 && settleStopwatch.Elapsed - lastEventAt >= settleWindow)
                {
                    break;
                }

                await Task.Delay(settleDelay, ct);
                continue;
            }

            idlePolls = 0;
            lastEventAt = settleStopwatch.Elapsed;
            foreach (var evt in events)
            {
                await WriteEventAsync(response, evt, ct);
                lastSeenId = evt.Id;
            }

            await response.Body.FlushAsync(ct);
        }

        return lastSeenId;
    }

    private static ProblemHttpResult NotFoundProblem(string detail) =>
        TypedResults.Problem(
            statusCode: StatusCodes.Status404NotFound,
            title: "Not Found",
            detail: detail);

    private static ProblemHttpResult ConflictProblem(string detail) =>
        TypedResults.Problem(
            statusCode: StatusCodes.Status409Conflict,
            title: "Conflict",
            detail: detail);
}