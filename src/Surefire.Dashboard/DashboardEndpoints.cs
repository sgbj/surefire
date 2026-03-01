using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
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

        api.MapGet("/stats", async (DateTimeOffset? since, int bucketMinutes, IJobStore store, CancellationToken ct) =>
            Results.Ok(await store.GetDashboardStatsAsync(since, bucketMinutes, ct)));

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

            using var doc = await JsonDocument.ParseAsync(request.Body, cancellationToken: ct);
            if (doc.RootElement.TryGetProperty("isEnabled", out var prop))
                await store.SetJobEnabledAsync(name, prop.GetBoolean(), ct);

            job = await store.GetJobAsync(name, ct);
            return Results.Ok(job);
        });

        api.MapPost("/jobs/{name}/trigger", async (string name, HttpRequest request, IJobClient client, CancellationToken ct) =>
        {
            object? args = null;
            DateTimeOffset? notBefore = null;

            if (request.ContentLength is null or > 0)
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

        api.MapGet("/runs", async (string? jobName, JobStatus? status, string? nodeName, string? parentRunId, string? originalRunId, int? skip, int? take, DateTimeOffset? createdAfter, DateTimeOffset? createdBefore, IJobStore store, CancellationToken ct) =>
        {
            var filter = new RunFilter
            {
                JobName = jobName,
                Status = status,
                NodeName = nodeName,
                ParentRunId = parentRunId,
                OriginalRunId = originalRunId,
                Skip = skip ?? 0,
                Take = take ?? 50,
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
            try
            {
                await client.CancelAsync(id, ct);
                return Results.Ok();
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("not found"))
            {
                return Results.NotFound();
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("terminal"))
            {
                return Results.Conflict(new { Error = ex.Message });
            }
        });

        api.MapPost("/runs/{id}/rerun", async (string id, IJobClient client, CancellationToken ct) =>
        {
            try
            {
                var newRunId = await client.RerunAsync(id, ct);
                return Results.Ok(new { RunId = newRunId });
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("not found"))
            {
                return Results.NotFound();
            }
        });

        api.MapGet("/runs/{id}/logs", async (string id, IJobStore store, CancellationToken ct) =>
            Results.Ok(await store.GetRunLogsAsync(id, ct)));

        api.MapGet("/runs/{id}/stream", async (string id, HttpResponse response, INotificationProvider notifications, IJobStore store, SurefireOptions options, CancellationToken ct) =>
        {
            response.ContentType = "text/event-stream";
            response.Headers.CacheControl = "no-cache";
            response.Headers.Connection = "keep-alive";

            var completedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var writeLock = new SemaphoreSlim(1, 1);

            await using var completedSub = await notifications.SubscribeAsync(
                NotificationChannels.RunCompleted(id),
                _ => { completedTcs.TrySetResult(); return Task.CompletedTask; },
                ct);

            await using var logSub = await notifications.SubscribeAsync(
                NotificationChannels.RunLog(id),
                async message =>
                {
                    await writeLock.WaitAsync(ct);
                    try
                    {
                        await response.WriteAsync($"data: {message}\n\n", ct);
                        await response.Body.FlushAsync(ct);
                    }
                    catch (OperationCanceledException) { }
                    finally { writeLock.Release(); }
                },
                ct);

            await using var progressSub = await notifications.SubscribeAsync(
                NotificationChannels.RunProgress(id),
                async message =>
                {
                    await writeLock.WaitAsync(ct);
                    try
                    {
                        await response.WriteAsync($"event: progress\ndata: {message}\n\n", ct);
                        await response.Body.FlushAsync(ct);
                    }
                    catch (OperationCanceledException) { }
                    finally { writeLock.Release(); }
                },
                ct);

            try
            {
                var run = await store.GetRunAsync(id, ct);
                var isTerminal = run is not null && run.Status.IsTerminal;

                await writeLock.WaitAsync(ct);
                try
                {
                    var existingLogs = await store.GetRunLogsAsync(id, ct);
                    foreach (var log in existingLogs)
                    {
                        await response.WriteAsync($"data: {JsonSerializer.Serialize(log, options.SerializerOptions)}\n\n", ct);
                    }
                    await response.Body.FlushAsync(ct);
                }
                finally { writeLock.Release(); }

                if (isTerminal)
                {
                    await response.WriteAsync("event: done\ndata: {}\n\n", ct);
                    return;
                }

                await using var reg = ct.Register(() => completedTcs.TrySetCanceled(ct));
                await completedTcs.Task;
                await response.WriteAsync("event: done\ndata: {}\n\n", CancellationToken.None);
            }
            catch (OperationCanceledException) { }
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
                await context.Response.Body.WriteAsync(indexBytes);
                return;
            }

            context.Response.ContentType = GetContentType(file.Name);
            await using var stream = file.CreateReadStream();
            await stream.CopyToAsync(context.Response.Body);
        });

        return endpoints;
    }

    private static string GetContentType(string fileName) => Path.GetExtension(fileName).ToLowerInvariant() switch
    {
        ".html" => "text/html",
        ".js" => "application/javascript",
        ".css" => "text/css",
        ".json" => "application/json",
        ".svg" => "image/svg+xml",
        ".png" => "image/png",
        ".ico" => "image/x-icon",
        ".woff" => "font/woff",
        ".woff2" => "font/woff2",
        _ => "application/octet-stream"
    };
}
