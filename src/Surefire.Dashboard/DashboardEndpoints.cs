using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Surefire;

namespace Surefire.Dashboard;

public static class DashboardEndpoints
{
    public static IEndpointRouteBuilder MapSurefireDashboard(this IEndpointRouteBuilder endpoints, string prefix = "/surefire")
    {
        var group = endpoints.MapGroup(prefix);
        var api = group.MapGroup("api");

        api.MapGet("/stats", async (DateTimeOffset? since, int? bucketMinutes, IJobStore store, CancellationToken ct) =>
            Results.Ok(await store.GetDashboardStatsAsync(since, bucketMinutes ?? 60, ct)));

        api.MapGet("/jobs", async (string? name, string? tag, bool? isEnabled, IJobStore store, CancellationToken ct) =>
        {
            var filter = (name is not null || tag is not null || isEnabled is not null)
                ? new JobFilter { Name = name, Tag = tag, IsEnabled = isEnabled }
                : null;
            return Results.Ok(await store.GetJobsAsync(filter, ct));
        });

        api.MapGet("/jobs/{name}", async (string name, IJobStore store, CancellationToken ct) =>
        {
            var job = await store.GetJobAsync(name, ct);
            if (job is null) return Results.NotFound();
            return Results.Ok(job);
        });

        api.MapPatch("/jobs/{name}", async (string name, HttpRequest request, IJobStore store, CancellationToken ct) =>
        {
            var job = await store.GetJobAsync(name, ct);
            if (job is null) return Results.NotFound();

            JsonDocument doc;
            try { doc = await JsonDocument.ParseAsync(request.Body, cancellationToken: ct); }
            catch (JsonException) { return Results.BadRequest(new { Error = "Invalid JSON in request body" }); }

            using (doc)
            {
                try
                {
                    if (doc.RootElement.TryGetProperty("isEnabled", out var prop))
                        await store.SetJobEnabledAsync(name, prop.GetBoolean(), ct);
                }
                catch (InvalidOperationException) { return Results.BadRequest(new { Error = "Invalid value for 'isEnabled'" }); }
            }

            job = await store.GetJobAsync(name, ct);
            return job is null ? Results.NotFound() : Results.Ok(job);
        });

        api.MapPost("/jobs/{name}/trigger", async (string name, HttpRequest request, IJobClient client, CancellationToken ct) =>
        {
            object? args = null;
            DateTimeOffset? notBefore = null;

            if (request.ContentLength is > 0 || (request.ContentLength is null && request.ContentType?.Contains("application/json") == true))
            {
                try
                {
                    using var doc = await JsonDocument.ParseAsync(request.Body, cancellationToken: ct);
                    if (doc.RootElement.TryGetProperty("args", out var argsElement))
                        args = argsElement.Clone();
                    if (doc.RootElement.TryGetProperty("notBefore", out var nbElement) && nbElement.ValueKind == JsonValueKind.String)
                    {
                        if (!DateTimeOffset.TryParse(nbElement.GetString()!, out var parsed))
                            return Results.BadRequest(new { Error = "Invalid 'notBefore' date format" });
                        notBefore = parsed;
                    }
                }
                catch (JsonException)
                {
                    return Results.BadRequest(new { Error = "Invalid JSON in request body" });
                }
            }

            var runId = await client.TriggerAsync(name, args, notBefore, ct);
            return Results.Ok(new { RunId = runId });
        });

        api.MapGet("/runs", async (string? jobName, JobStatus? status, string? nodeName, string? parentRunId, string? retryOfRunId, string? rerunOfRunId, int? skip, int? take, DateTimeOffset? createdAfter, DateTimeOffset? createdBefore, IJobStore store, CancellationToken ct) =>
        {
            var filter = new RunFilter
            {
                JobName = jobName,
                Status = status,
                NodeName = nodeName,
                ParentRunId = parentRunId,
                RetryOfRunId = retryOfRunId,
                RerunOfRunId = rerunOfRunId,
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

        api.MapGet("/runs/{id}/logs", async (string id, IJobStore store, CancellationToken ct) =>
        {
            var events = await store.GetEventsAsync(id, types: [RunEventType.Log], cancellationToken: ct);
            var logs = new List<object>();
            foreach (var e in events)
            {
                try
                {
                    using var doc = JsonDocument.Parse(e.Payload);
                    var root = doc.RootElement;
                    logs.Add(new
                    {
                        timestamp = root.GetProperty("timestamp").GetString(),
                        level = root.GetProperty("level").GetInt32(),
                        message = root.GetProperty("message").GetString(),
                        category = root.TryGetProperty("category", out var cat) ? cat.GetString() : null
                    });
                }
                catch (JsonException) { /* skip malformed log events */ }
            }
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

        api.MapGet("/nodes", async (IJobStore store, CancellationToken ct) =>
            Results.Ok(await store.GetNodesAsync(ct)));

        api.MapGet("/nodes/{name}", async (string name, IJobStore store, CancellationToken ct) =>
        {
            var node = await store.GetNodeAsync(name, ct);
            return node is null ? Results.NotFound() : Results.Ok(node);
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

        return endpoints;
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
