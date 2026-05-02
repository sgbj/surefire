using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;
using Surefire;
using Surefire.Dashboard;

namespace Microsoft.AspNetCore.Builder;

/// <summary>
///     Endpoint-routing extensions that mount the Surefire dashboard (both the JSON API used by
///     external tools and the embedded UI) under a configurable URL prefix.
/// </summary>
public static class DashboardEndpoints
{
    private const int DefaultRunsPageSize = 50;
    private const int MaxRunsPageSize = 500;
    private const int MaxRunsLookupIds = 500;
    private const int DefaultChildrenPageSize = 100;
    private const int MaxChildrenPageSize = 500;
    private const int DefaultSiblingWindow = 50;
    private const int MaxSiblingWindow = 200;

    /// <summary>
    ///     Maps the Surefire dashboard endpoints under <paramref name="prefix" />. Mounts:
    ///     <list type="bullet">
    ///         <item>
    ///             <description>
    ///                 <c>{prefix}/api/...</c>: JSON endpoints for jobs, runs, queues, nodes, stats, SSE log
    ///                 streaming, and tree-aware run traces.
    ///             </description>
    ///         </item>
    ///         <item>
    ///             <description><c>{prefix}/...</c>: the embedded single-page UI served from the assembly's resources.</description>
    ///         </item>
    ///     </list>
    ///     The returned <see cref="IEndpointConventionBuilder" /> covers the full group, so callers
    ///     can apply auth, CORS, or rate-limit conventions across every route in one chain. For
    ///     example: <c>app.MapSurefireDashboard().RequireAuthorization("Surefire")</c>.
    /// </summary>
    /// <param name="endpoints">The route builder to mount onto.</param>
    /// <param name="prefix">URL prefix the dashboard is served under. Must begin with <c>/</c>. Defaults to <c>/surefire</c>.</param>
    /// <returns>A convention builder over the entire dashboard route group.</returns>
    /// <remarks>
    ///     Resolves <see cref="Surefire.IJobStore" />, <see cref="Surefire.IJobClient" />,
    ///     <see cref="Surefire.SurefireOptions" />, and <see cref="TimeProvider" /> from DI.
    ///     The dashboard is unauthenticated by default; production deployments should chain
    ///     <c>.RequireAuthorization(...)</c> on the returned builder.
    /// </remarks>
    [RequiresUnreferencedCode("Minimal API endpoint mapping reflects over delegate parameters.")]
    [RequiresDynamicCode("Minimal API endpoint mapping reflects over delegate parameters.")]
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
                var filter = new JobListFilter
                {
                    Name = name,
                    Tag = tag,
                    IsEnabled = isEnabled,
                    HeartbeatAfter = includeInactive == true ? null : cutoff
                };

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
                        : await client.TriggerAsync(name, args, cancellationToken: ct);
                    return TypedResults.Ok(new RunIdResponse(runId.Id));
                }
                catch (RunConflictException ex)
                {
                    return ConflictProblem(ex.Message);
                }
            });

        api.MapGet("/runs", async (string? jobName, string? jobNameContains, JobStatus? status, string? nodeName,
            string? parentRunId, int? skip, int? take, DateTimeOffset? createdAfter, DateTimeOffset? createdBefore,
            IJobStore store, CancellationToken ct) =>
        {
            var filter = new RunFilter
            {
                JobName = jobName,
                JobNameContains = jobNameContains,
                Status = status,
                NodeName = nodeName,
                ParentRunId = parentRunId,
                OrderBy = RunOrderBy.CreatedAt,
                CreatedAfter = createdAfter,
                CreatedBefore = createdBefore
            };

            var requestedTake = Math.Clamp(take ?? DefaultRunsPageSize, 1, MaxRunsPageSize);
            var runsPage = await store.GetRunsAsync(filter, Math.Max(skip ?? 0, 0), requestedTake, ct);
            return TypedResults.Ok(new PagedResponse<RunResponse>
            {
                Items = runsPage.Items.Select(r => RunResponse.From(r)).ToList(),
                TotalCount = runsPage.TotalCount
            });
        });

        api.MapPost("/runs/lookup",
            async Task<Results<Ok<IReadOnlyList<RunResponse>>, ProblemHttpResult>> (RunLookupRequest request,
                IJobStore store, CancellationToken ct) =>
            {
                var ids = request.Ids
                    .Select(id => id.Trim())
                    .Where(id => id.Length > 0)
                    .Distinct(StringComparer.Ordinal)
                    .ToList();

                if (ids.Count > MaxRunsLookupIds)
                {
                    return TypedResults.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Too many run IDs",
                        detail: $"A maximum of {MaxRunsLookupIds} run IDs can be refreshed at once.");
                }

                if (ids.Count == 0)
                {
                    return TypedResults.Ok<IReadOnlyList<RunResponse>>([]);
                }

                var runs = await store.GetRunsByIdsAsync(ids, ct);
                return TypedResults.Ok<IReadOnlyList<RunResponse>>(runs.Select(r => RunResponse.From(r)).ToList());
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
            async Task<Results<Ok<RunTraceResponse>, ProblemHttpResult>> (string id, int? siblingWindow,
                int? childrenTake, IJobStore store, CancellationToken ct) =>
            {
                var focus = await store.GetRunAsync(id, ct);
                if (focus is null)
                {
                    return NotFoundProblem($"Run '{id}' was not found.");
                }

                var window = Math.Clamp(siblingWindow ?? DefaultSiblingWindow, 1, MaxSiblingWindow);
                var childTake = Math.Clamp(childrenTake ?? DefaultChildrenPageSize, 1, MaxChildrenPageSize);

                var ancestors = await store.GetAncestorChainAsync(id, ct);
                var ancestorResponses = ancestors
                    .Select((ancestor, index) => RunResponse.From(ancestor, index))
                    .ToList();

                var focusDepth = ancestorResponses.Count;
                var focusResponse = RunResponse.From(focus, focusDepth);

                var siblingsBefore = new List<RunResponse>();
                var siblingsAfter = new List<RunResponse>();
                string? siblingsAfterCursor = null;
                string? siblingsBeforeCursor = null;

                if (focus.ParentRunId is { } parentId)
                {
                    var siblingDepth = focusDepth;
                    var focusCursor = DirectChildrenPage.EncodeCursor(focus.CreatedAt, focus.Id);

                    var afterPage = await store.GetDirectChildrenAsync(parentId,
                        focusCursor,
                        take: window,
                        cancellationToken: ct);
                    siblingsAfter.AddRange(afterPage.Items.Select(r => RunResponse.From(r, siblingDepth)));
                    siblingsAfterCursor = afterPage.NextCursor;

                    // Reverse keyset DESC by (createdAt, id); reverse client-side for display.
                    // NextCursor points to the oldest row, used to paginate further back.
                    var beforePage = await store.GetDirectChildrenAsync(parentId,
                        beforeCursor: focusCursor,
                        take: window,
                        cancellationToken: ct);
                    siblingsBefore = beforePage.Items
                        .Reverse()
                        .Select(r => RunResponse.From(r, siblingDepth))
                        .ToList();
                    siblingsBeforeCursor = beforePage.NextCursor;
                }
                // Batch focus (no ParentRunId): siblings stay empty. Batch children share a
                // CreatedAt, so a keyset split around the focus isn't possible; the dedicated
                // batch page handles browsing batch children at scale.

                var childrenPage = await store.GetDirectChildrenAsync(id, take: childTake, cancellationToken: ct);
                var childResponses = childrenPage.Items
                    .Select(c => RunResponse.From(c, focusDepth + 1))
                    .ToList();

                var siblingsCursor =
                    siblingsAfterCursor is { } || siblingsBeforeCursor is { }
                        ? new SiblingsCursorResponse
                        {
                            After = siblingsAfterCursor,
                            Before = siblingsBeforeCursor
                        }
                        : null;

                return TypedResults.Ok(new RunTraceResponse
                {
                    Ancestors = ancestorResponses,
                    Focus = focusResponse,
                    SiblingsBefore = siblingsBefore,
                    SiblingsAfter = siblingsAfter,
                    SiblingsCursor = siblingsCursor,
                    Children = childResponses,
                    ChildrenCursor = childrenPage.NextCursor
                });
            });

        // Direct-children pagination. afterCursor / beforeCursor are mutually exclusive.
        // Items return in store-natural order (ASC for afterCursor, DESC for beforeCursor);
        // backward callers reverse client-side.
        api.MapGet("/runs/{id}/children",
            async Task<Results<Ok<RunChildrenResponse>, ProblemHttpResult>> (string id, string? afterCursor,
                string? beforeCursor, int? take, IJobStore store, CancellationToken ct) =>
            {
                var run = await store.GetRunAsync(id, ct);
                if (run is null)
                {
                    return NotFoundProblem($"Run '{id}' was not found.");
                }

                if (!string.IsNullOrEmpty(afterCursor) && !string.IsNullOrEmpty(beforeCursor))
                {
                    return TypedResults.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Invalid pagination cursors",
                        detail: "afterCursor and beforeCursor are mutually exclusive.");
                }

                var resolvedTake = Math.Clamp(take ?? DefaultChildrenPageSize, 1, MaxChildrenPageSize);

                DirectChildrenPage page;
                try
                {
                    page = await store.GetDirectChildrenAsync(id,
                        afterCursor,
                        beforeCursor,
                        resolvedTake,
                        ct);
                }
                catch (FormatException ex)
                {
                    // Surface a 400 rather than 500 since user-facing URLs can be mutated.
                    // Don't silently restart at page 0; that hides bugs and can livelock clients.
                    return TypedResults.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Invalid pagination cursor",
                        detail: ex.Message);
                }

                return TypedResults.Ok(new RunChildrenResponse
                {
                    Items = page.Items.Select(r => RunResponse.From(r)).ToList(),
                    NextCursor = page.NextCursor
                });
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
            });

        api.MapPost("/runs/{id}/rerun",
            async Task<Results<Ok<RunIdResponse>, ProblemHttpResult>> (string id, IJobClient client,
                CancellationToken ct) =>
            {
                try
                {
                    var newRunId = await client.RerunAsync(id, ct);
                    return TypedResults.Ok(new RunIdResponse(newRunId.Id));
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
            async Task<Results<Ok<LogPageResponse>, ProblemHttpResult>> (string id, long? sinceEventId,
                int? take, IJobStore store, ILoggerFactory loggerFactory, CancellationToken ct) =>
            {
                var run = await store.GetRunAsync(id, ct);
                if (run is null)
                {
                    return NotFoundProblem($"Run '{id}' was not found.");
                }

                var resolvedTake = Math.Clamp(take ?? 200, 1, 1000);
                var logger = loggerFactory.CreateLogger(typeof(DashboardEndpoints));
                // Fetch one extra to detect whether more pages exist.
                var events = await store.GetEventsAsync(id, sinceEventId ?? 0,
                    [RunEventType.Log], take: resolvedTake + 1, cancellationToken: ct);

                var logs = new List<JsonElement>(Math.Min(events.Count, resolvedTake));
                long? lastScannedId = null;
                var scanned = 0;
                foreach (var @event in events)
                {
                    if (scanned >= resolvedTake)
                    {
                        break;
                    }

                    scanned++;
                    // Advance unconditionally so malformed-skip still counts against the window;
                    // otherwise pagination could livelock on a run with only unparseable events.
                    lastScannedId = @event.Id;

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

                var hasMore = events.Count > resolvedTake;
                return TypedResults.Ok(new LogPageResponse
                {
                    Items = logs,
                    NextCursor = hasMore ? lastScannedId : null
                });
            });

        api.MapGet("/runs/{id}/stream", async Task<Results<ProblemHttpResult, EmptyHttpResult>> (string id,
            long? sinceEventId, HttpResponse response, IJobClient client, CancellationToken ct) =>
        {
            var run = await client.GetRunAsync(id, ct);
            if (run is null)
            {
                return NotFoundProblem($"Run '{id}' was not found.");
            }

            response.ContentType = "text/event-stream";
            response.Headers.CacheControl = "no-cache";

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
                using var writeLock = new SemaphoreSlim(1, 1);
                using var keepaliveCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var lastWriteTimestamp = Stopwatch.GetTimestamp();

                var keepaliveTask = RunKeepaliveLoopAsync(
                    response,
                    writeLock,
                    () => Stopwatch.GetElapsedTime(Volatile.Read(ref lastWriteTimestamp)),
                    keepaliveCts.Token);

                try
                {
                    await foreach (var evt in client.ObserveRunEventsAsync(id, lastSeenId, ct))
                    {
                        await WriteSseFrameAsync(
                            response,
                            writeLock,
                            frameToken => WriteEventAsync(response, evt, frameToken),
                            () => Volatile.Write(ref lastWriteTimestamp, Stopwatch.GetTimestamp()),
                            ct);
                        lastSeenId = evt.Id;
                    }

                    // Emit the SSE "done" frame so the UI can close its EventSource cleanly.
                    await WriteSseFrameAsync(
                        response,
                        writeLock,
                        frameToken => response.WriteAsync("event: done\ndata: {}\n\n", frameToken),
                        () => Volatile.Write(ref lastWriteTimestamp, Stopwatch.GetTimestamp()),
                        CancellationToken.None);
                    return TypedResults.Empty;
                }
                finally
                {
                    keepaliveCts.Cancel();
                    try
                    {
                        await keepaliveTask;
                    }
                    catch (OperationCanceledException)
                    {
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
            async Task<Results<NoContent, ProblemHttpResult>> (string name, UpdateQueueRequest request,
                IJobStore store, CancellationToken ct) =>
            {
                if (request.IsPaused is { } paused)
                {
                    var updated = await store.SetQueuePausedAsync(name, paused, ct);
                    if (!updated)
                    {
                        return NotFoundProblem($"Queue '{name}' was not found.");
                    }
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
            var node = await store.GetNodeAsync(name, ct);
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

    private static async Task WriteSseFrameAsync(HttpResponse response, SemaphoreSlim writeLock,
        Func<CancellationToken, Task> writer, Action onWritten, CancellationToken ct)
    {
        await writeLock.WaitAsync(ct);
        try
        {
            await writer(ct);
            await response.Body.FlushAsync(ct);
            onWritten();
        }
        finally
        {
            writeLock.Release();
        }
    }

    private static async Task RunKeepaliveLoopAsync(HttpResponse response, SemaphoreSlim writeLock,
        Func<TimeSpan> getIdleDuration, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        while (await timer.WaitForNextTickAsync(ct))
        {
            if (getIdleDuration() < TimeSpan.FromSeconds(15))
            {
                continue;
            }

            await writeLock.WaitAsync(ct);
            try
            {
                await response.WriteAsync(": keepalive\n\n", ct);
                await response.Body.FlushAsync(ct);
            }
            finally
            {
                writeLock.Release();
            }
        }
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
