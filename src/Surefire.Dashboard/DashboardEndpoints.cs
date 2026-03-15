using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Surefire;
using Surefire.Dashboard;

namespace Surefire.Dashboard;

public static class DashboardEndpoints
{
    public static IEndpointConventionBuilder MapSurefireDashboard(this IEndpointRouteBuilder endpoints, string prefix = "/surefire")
    {
        var group = endpoints.MapGroup(prefix);
        var api = group.MapGroup("api");

        api.MapGet("/stats", async (DateTimeOffset? since, int? bucketMinutes, IJobStore store, CancellationToken ct) =>
            Results.Ok(await store.GetDashboardStatsAsync(since, bucketMinutes ?? 60, ct)));

        api.MapGet("/jobs", async (string? name, string? tag, bool? isEnabled, bool? includeInactive, bool? includeInternal, IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
        {
            var now = timeProvider.GetUtcNow();
            var cutoff = now - surefireOpts.InactiveThreshold;
            var filter = new JobListFilter { Name = name, Tag = tag, IsEnabled = isEnabled, IncludeInternal = includeInternal };
            if (includeInactive != true)
                filter.HeartbeatAfter = cutoff;
            var jobs = await store.GetJobsAsync(filter, ct);
            return Results.Ok(jobs.Select(j => JobResponse.From(j, cutoff, now)).ToList());
        });

        api.MapGet("/jobs/{name}", async (string name, IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
        {
            var job = await store.GetJobAsync(name, ct);
            if (job is null) return Results.NotFound();
            var now = timeProvider.GetUtcNow();
            var cutoff = now - surefireOpts.InactiveThreshold;
            return Results.Ok(JobResponse.From(job, cutoff, now));
        });

        api.MapPatch("/jobs/{name}", async (string name, UpdateJobRequest request, IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
        {
            var job = await store.GetJobAsync(name, ct);
            if (job is null) return Results.NotFound();

            if (request.IsEnabled is not null)
                await store.SetJobEnabledAsync(name, request.IsEnabled.Value, ct);

            job = await store.GetJobAsync(name, ct);
            if (job is null) return Results.NotFound();
            var now = timeProvider.GetUtcNow();
            var cutoff = now - surefireOpts.InactiveThreshold;
            return Results.Ok(JobResponse.From(job, cutoff, now));
        });

        api.MapGet("/jobs/{name}/stats", async (string name, IJobStore store, CancellationToken ct) =>
        {
            var job = await store.GetJobAsync(name, ct);
            if (job is null) return Results.NotFound();
            return Results.Ok(await store.GetJobStatsAsync(name, ct));
        });

        api.MapPost("/jobs/{name}/trigger", async (string name, TriggerJobRequest? request, IJobClient client, CancellationToken ct) =>
        {
            object? args = request?.Args;
            RunOptions? runOptions = null;

            if (request?.NotBefore is not null || request?.NotAfter is not null || request?.Priority is not null || request?.DeduplicationId is not null)
            {
                runOptions = new RunOptions
                {
                    NotBefore = request.NotBefore,
                    NotAfter = request.NotAfter,
                    Priority = request.Priority,
                    DeduplicationId = request.DeduplicationId
                };
            }

            var runId = runOptions is not null
                ? await client.TriggerAsync(name, args, runOptions, ct)
                : await client.TriggerAsync(name, args, ct);
            return Results.Ok(new { RunId = runId });
        });

        api.MapGet("/runs", async (string? jobName, bool? exactJobName, JobStatus? status, string? nodeName, string? parentRunId, string? retryOfRunId, string? rerunOfRunId, string? planRunId, string? planStepName, int? skip, int? take, DateTimeOffset? createdAfter, DateTimeOffset? createdBefore, IJobStore store, CancellationToken ct) =>
        {
            var filter = new RunFilter
            {
                JobName = jobName,
                ExactJobName = exactJobName ?? false,
                Status = status,
                NodeName = nodeName,
                ParentRunId = parentRunId,
                RetryOfRunId = retryOfRunId,
                RerunOfRunId = rerunOfRunId,
                PlanRunId = planRunId,
                PlanStepName = planStepName,
                Skip = Math.Max(skip ?? 0, 0),
                Take = Math.Clamp(take ?? 50, 1, 500),
                CreatedAfter = createdAfter,
                CreatedBefore = createdBefore
            };
            return Results.Ok(await store.GetRunsAsync(filter, ct));
        });

        api.MapGet("/runs/{id}", async (string id, IJobStore store, CancellationToken ct) =>
        {
            var run = await store.GetRunAsync(id, ct);
            return run is null ? Results.NotFound() : Results.Ok(run);
        });

        api.MapGet("/runs/{id}/trace", async (string id, int? limit, IJobStore store, CancellationToken ct) =>
        {
            var run = await store.GetRunAsync(id, ct);
            if (run is null) return Results.NotFound();
            return Results.Ok(await store.GetRunTraceAsync(id, Math.Clamp(limit ?? 500, 1, 5000), ct));
        });

        api.MapPost("/runs/{id}/cancel", async (string id, IJobClient client, CancellationToken ct) =>
        {
            var run = await client.GetRunAsync(id, ct);
            if (run is null) return Results.NotFound();
            if (run.Status.IsTerminal) return Results.Conflict(new { Error = $"Run '{id}' is already in a terminal state." });
            await client.CancelAsync(id, ct);
            return Results.NoContent();
        });

        api.MapPost("/runs/{id}/rerun", async (string id, IJobClient client, CancellationToken ct) =>
        {
            var run = await client.GetRunAsync(id, ct);
            if (run is null) return Results.NotFound();
            var newRunId = await client.RerunAsync(id, ct);
            return Results.Ok(new { RunId = newRunId });
        });

        api.MapPost("/plans/{planRunId}/signals/{signalName}", async (string planRunId, string signalName, HttpRequest request, IJobClient client, SurefireOptions surefireOpts, CancellationToken ct) =>
        {
            try
            {
                object? payload = null;
                if (request.ContentLength > 0)
                {
                    payload = await JsonSerializer.DeserializeAsync<JsonElement>(request.Body, surefireOpts.SerializerOptions, ct);
                }
                await client.SendSignalAsync(planRunId, signalName, payload, ct);
                return Results.NoContent();
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { Error = ex.Message });
            }
        });

        api.MapGet("/runs/{id}/logs", async (string id, IJobStore store, CancellationToken ct) =>
        {
            var events = await store.GetEventsAsync(id, types: [RunEventType.Log], cancellationToken: ct);
            var logs = events
                .Where(e => e.Payload is not null)
                .Select(e => JsonSerializer.Deserialize<JsonElement>(e.Payload))
                .ToList();
            return Results.Ok(logs);
        });

        api.MapGet("/runs/{id}/stream", async (string id, HttpResponse response, INotificationProvider notifications, IJobStore store, SurefireOptions surefireOptions, CancellationToken ct) =>
        {
            var run = await store.GetRunAsync(id, ct);
            if (run is null)
            {
                response.StatusCode = 404;
                return;
            }

            response.ContentType = "text/event-stream";
            response.Headers.CacheControl = "no-cache";

            var pollingInterval = surefireOptions.PollingInterval;
            using var wakeup = new SemaphoreSlim(0, 1);

            await using var eventSub = await notifications.SubscribeAsync(
                NotificationChannels.RunEvent(id),
                _ => { try { wakeup.Release(); } catch (Exception ex) when (ex is SemaphoreFullException or ObjectDisposedException) { } return Task.CompletedTask; },
                ct);

            var completedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using var completedSub = await notifications.SubscribeAsync(
                NotificationChannels.RunCompleted(id),
                _ => { completedTcs.TrySetResult(); try { wakeup.Release(); } catch (Exception ex) when (ex is SemaphoreFullException or ObjectDisposedException) { } return Task.CompletedTask; },
                ct);

            long lastSeenId = 0;
            if (long.TryParse(response.HttpContext.Request.Headers["Last-Event-ID"], out var resumeId))
                lastSeenId = resumeId;

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

                // Check if already terminal — drain any events written since initial fetch
                run = await store.GetRunAsync(id, ct);
                if (run is not null && run.Status.IsTerminal)
                {
                    var finalEvents = await store.GetEventsAsync(id, lastSeenId, cancellationToken: ct);
                    foreach (var evt in finalEvents)
                    {
                        await WriteEventAsync(response, evt, ct);
                        lastSeenId = evt.Id;
                    }
                    if (finalEvents.Count > 0)
                        await response.Body.FlushAsync(ct);

                    await response.WriteAsync("event: done\ndata: {}\n\n", ct);
                    return;
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
                        await response.Body.FlushAsync(ct);

                    // Check if run completed
                    if (completedTcs.Task.IsCompleted)
                    {
                        // Drain final events
                        var finalEvents = await store.GetEventsAsync(id, lastSeenId, cancellationToken: ct);
                        foreach (var evt in finalEvents)
                        {
                            await WriteEventAsync(response, evt, ct);
                            lastSeenId = evt.Id;
                        }
                        if (finalEvents.Count > 0)
                            await response.Body.FlushAsync(ct);

                        await response.WriteAsync("event: done\ndata: {}\n\n", CancellationToken.None);
                        return;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (IOException) { } // Client disconnected mid-write
        });

        api.MapGet("/queues", async (IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
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
                        nodesByQueue[queueName] = list = [];
                    list.Add(node.Name);
                }
            }

            // Merge explicit queue definitions with implicit ones (like "default")
            var allQueueNames = new HashSet<string>(queues.Select(q => q.Name));
            foreach (var key in queueStats.Keys) allQueueNames.Add(key);
            foreach (var key in nodesByQueue.Keys) allQueueNames.Add(key);

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

            return Results.Ok(result);
        });

        api.MapPatch("/queues/{name}", async (string name, UpdateQueueRequest request, IJobStore store, CancellationToken ct) =>
        {
            var queue = await store.GetQueueAsync(name, ct);

            // Implicit queues (e.g. "default") aren't in the store — materialize them so we can update properties.
            if (queue is null)
            {
                queue = new QueueDefinition { Name = name };
                await store.UpsertQueueAsync(queue, ct);
            }

            if (request.IsPaused is not null)
                await store.SetQueuePausedAsync(name, request.IsPaused.Value, ct);

            queue = await store.GetQueueAsync(name, ct);
            return queue is null ? Results.NotFound() : Results.Ok(queue);
        });

        api.MapGet("/nodes", async (bool? includeInactive, IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
        {
            var cutoff = timeProvider.GetUtcNow() - surefireOpts.InactiveThreshold;
            var nodes = await store.GetNodesAsync(ct);
            if (includeInactive is not true)
                nodes = nodes.Where(n => n.LastHeartbeatAt >= cutoff).ToList();
            return Results.Ok(nodes.Select(n => NodeResponse.From(n, cutoff)).ToList());
        });

        api.MapGet("/nodes/{name}", async (string name, IJobStore store, SurefireOptions surefireOpts, TimeProvider timeProvider, CancellationToken ct) =>
        {
            var node = await store.GetNodeAsync(name, ct);
            if (node is null) return Results.NotFound();
            var cutoff = timeProvider.GetUtcNow() - surefireOpts.InactiveThreshold;
            return Results.Ok(NodeResponse.From(node, cutoff));
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
                html.Replace("<base href=\"/surefire/\"", $"<base href=\"{basePath}\""));
        }

        var contentTypeProvider = new FileExtensionContentTypeProvider();

        group.Map("{**path}", async (HttpContext context) =>
        {
            var path = context.Request.RouteValues["path"]?.ToString() ?? "";

            if (path.StartsWith("api/"))
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
                contentType = "application/octet-stream";

            context.Response.ContentType = contentType;
            if (path.StartsWith("assets/"))
                context.Response.Headers.CacheControl = "public, max-age=31536000, immutable";

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

        if (sseEventType is not null)
            await response.WriteAsync($"id: {evt.Id}\nevent: {sseEventType}\n{dataLines}\n\n", ct);
        else
            await response.WriteAsync($"id: {evt.Id}\n{dataLines}\n\n", ct);
    }
}
