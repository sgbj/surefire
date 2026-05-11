using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net.ServerSentEvents;
using System.Runtime.CompilerServices;
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
    /// <param name="configure">Optional callback to override <see cref="SurefireDashboardOptions" /> defaults.</param>
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
        string prefix = "/surefire",
        Action<SurefireDashboardOptions>? configure = null)
    {
        var options = new SurefireDashboardOptions();
        configure?.Invoke(options);

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

        api.MapGet("/runs/{id}/tree",
            async Task<Results<Ok<RunTreeResponse>, ProblemHttpResult>> (string id, IJobStore store,
                CancellationToken ct) =>
            {
                var focus = await store.GetRunAsync(id, ct);
                if (focus is null)
                {
                    return NotFoundProblem($"Run '{id}' was not found.");
                }

                return TypedResults.Ok(await BuildRunTreeAsync(store, focus, options.MaxTreeRuns, ct));
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

        api.MapGet("/runs/{id}/stream",
            async Task<Results<ProblemHttpResult, ServerSentEventsResult<string>>> (string id, long? sinceEventId,
                HttpContext httpContext, IJobClient client, TimeProvider timeProvider, CancellationToken ct) =>
            {
                var run = await client.GetRunAsync(id, ct);
                if (run is null)
                {
                    return NotFoundProblem($"Run '{id}' was not found.");
                }

                var resumeFrom = long.TryParse(httpContext.Request.Headers["Last-Event-ID"], out var resumeId)
                    ? resumeId
                    : sinceEventId is > 0
                        ? sinceEventId.Value
                        : 0;

                return TypedResults.ServerSentEvents(StreamRunEventsAsync(client, id, resumeFrom, timeProvider, ct));
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

    private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(15);

    private static async IAsyncEnumerable<SseItem<string>> StreamRunEventsAsync(IJobClient client, string runId,
        long sinceEventId, TimeProvider timeProvider, [EnumeratorCancellation] CancellationToken ct)
    {
        using var enumeratorCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        await using var enumerator = client
            .ObserveRunEventsAsync(runId, sinceEventId, enumeratorCts.Token)
            .GetAsyncEnumerator(enumeratorCts.Token);

        Task<bool>? pendingMoveNext = null;
        try
        {
            while (true)
            {
                pendingMoveNext ??= enumerator.MoveNextAsync().AsTask();

                if (!pendingMoveNext.IsCompleted)
                {
                    using var keepaliveCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    var keepaliveDelay = Task.Delay(KeepAliveInterval, timeProvider, keepaliveCts.Token);
                    var winner = await Task.WhenAny(pendingMoveNext, keepaliveDelay).ConfigureAwait(false);
                    keepaliveCts.Cancel();

                    if (winner != pendingMoveNext)
                    {
                        yield return new SseItem<string>(string.Empty, "keepalive");
                        continue;
                    }
                }

                if (!await pendingMoveNext)
                {
                    break;
                }

                var evt = enumerator.Current;
                pendingMoveNext = null;
                yield return new SseItem<string>(evt.Payload, MapEventType(evt.EventType))
                {
                    EventId = evt.Id.ToString(CultureInfo.InvariantCulture)
                };
            }

            yield return new SseItem<string>("{}", "done");
        }
        finally
        {
            // Observe any still-in-flight MoveNextAsync before disposal. Otherwise an unobserved
            // pending op would race the compiler-generated DisposeAsync and surface as
            // NotSupportedException, swallowing the real teardown cause.
            if (pendingMoveNext is { } pending)
            {
                enumeratorCts.Cancel();
                try { await pending; } catch { }
            }
        }
    }

    private static string? MapEventType(RunEventType type) => type switch
    {
        RunEventType.Status => "status",
        RunEventType.Progress => "progress",
        RunEventType.Output => "output",
        RunEventType.OutputComplete => "outputComplete",
        RunEventType.Input => "input",
        RunEventType.InputComplete => "inputComplete",
        RunEventType.InputDeclared => "inputDeclared",
        RunEventType.AttemptFailure => "attemptFailure",
        _ => null
    };

    /// <summary>
    ///     Builds the run-tree response for <paramref name="focus" />. Always includes the focus
    ///     and its full ancestor chain so depth resolves correctly even when truncation drops
    ///     other descendants. BFS down from the lineage through the descendants page drops
    ///     orphans (rows whose parent chain isn't visible) instead of rendering them at the
    ///     wrong depth on the client. Exposed as internal so tests can exercise truncation at
    ///     small caps; the endpoint reads its cap from <see cref="SurefireDashboardOptions" />.
    /// </summary>
    internal static async Task<RunTreeResponse> BuildRunTreeAsync(IJobStore store, JobRun focus, int maxRuns,
        CancellationToken cancellationToken)
    {
        // Root runs have RootRunId = null and are identified by their own id.
        var rootId = focus.RootRunId ?? focus.Id;

        var ancestors = await store.GetAncestorChainAsync(focus.Id, cancellationToken);

        // Ascending so truncation surfaces rows nearest the root (parents come before children),
        // which gives a useful subtree under the cap. With descending, the page is dominated by
        // deep leaves whose parents aren't visible and would render at the wrong depth.
        var descendantsPage = await store.GetRunsAsync(
            new RunFilter
            {
                RootRunId = rootId,
                OrderBy = RunOrderBy.CreatedAt,
                Direction = RunOrderDirection.Ascending
            },
            skip: 0,
            take: maxRuns,
            cancellationToken);

        // Bucket descendants by parent id for O(N) BFS lookups. Empty string keys the
        // ParentRunId = null group (top-level batch children share a null parent).
        var childrenByParent = new Dictionary<string, List<JobRun>>(StringComparer.Ordinal);
        foreach (var descendant in descendantsPage.Items)
        {
            var parentKey = descendant.ParentRunId ?? string.Empty;
            if (!childrenByParent.TryGetValue(parentKey, out var list))
            {
                childrenByParent[parentKey] = list = [];
            }

            list.Add(descendant);
        }

        var runs = new List<JobRun>(ancestors.Count + descendantsPage.Items.Count + 1);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<JobRun>();

        // Seed BFS with focus's lineage so depth resolves and the user always sees the run
        // they navigated to, even when truncation drops it from the descendants page.
        foreach (var ancestor in ancestors)
        {
            if (seen.Add(ancestor.Id))
            {
                runs.Add(ancestor);
                queue.Enqueue(ancestor);
            }
        }

        if (seen.Add(focus.Id))
        {
            runs.Add(focus);
            queue.Enqueue(focus);
        }

        // Also seed from focus's parent-group so batch-root cases (focus.ParentRunId is null,
        // rootId is a conceptual batch with no run row) still surface the other batch children.
        var siblingsKey = focus.ParentRunId ?? string.Empty;
        if (childrenByParent.TryGetValue(siblingsKey, out var focusSiblings))
        {
            foreach (var sibling in focusSiblings)
            {
                if (seen.Add(sibling.Id))
                {
                    runs.Add(sibling);
                    queue.Enqueue(sibling);
                }
            }
        }

        // BFS from the lineage through the descendants page. Anything unreachable (truncated
        // branches where the parent chain isn't visible) gets dropped.
        while (queue.Count > 0)
        {
            var node = queue.Dequeue();
            if (childrenByParent.TryGetValue(node.Id, out var children))
            {
                foreach (var child in children)
                {
                    if (seen.Add(child.Id))
                    {
                        runs.Add(child);
                        queue.Enqueue(child);
                    }
                }
            }
        }

        var hasRootRow = focus.Id == rootId
            || (ancestors.Count > 0 && ancestors[0].Id == rootId);
        var totalCount = (hasRootRow ? 1 : 0) + descendantsPage.TotalCount;
        var truncated = totalCount > runs.Count;

        runs.Sort(static (a, b) =>
        {
            var cmp = a.CreatedAt.CompareTo(b.CreatedAt);
            return cmp != 0 ? cmp : string.CompareOrdinal(a.Id, b.Id);
        });

        // Iterative (not recursive) so a deep chain can't blow the stack.
        var byId = runs.ToDictionary(r => r.Id, StringComparer.Ordinal);
        var depths = new Dictionary<string, int>(runs.Count, StringComparer.Ordinal);
        var chain = new Stack<JobRun>();

        foreach (var run in runs)
        {
            if (depths.ContainsKey(run.Id))
            {
                continue;
            }

            chain.Clear();
            var current = run;
            int baseDepth;
            while (true)
            {
                if (depths.TryGetValue(current.Id, out var cached))
                {
                    baseDepth = cached;
                    break;
                }

                if (current.ParentRunId is { } parentId && byId.TryGetValue(parentId, out var parent))
                {
                    chain.Push(current);
                    current = parent;
                    continue;
                }

                // Tree root, or a node whose parent isn't in the result set (orphan).
                depths[current.Id] = 0;
                baseDepth = 0;
                break;
            }

            while (chain.Count > 0)
            {
                baseDepth++;
                depths[chain.Pop().Id] = baseDepth;
            }
        }

        return new RunTreeResponse
        {
            RootId = rootId,
            Runs = runs.Select(r => RunResponse.From(r, depths[r.Id])).ToList(),
            Truncated = truncated,
            TotalCount = totalCount
        };
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
