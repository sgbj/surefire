using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Surefire;
using Surefire.Dashboard;

namespace Surefire.Tests.Dashboard;

public sealed class DashboardEndpointsTests
{
    [Fact]
    public async Task RunsEndpoint_CapsTakeAt500()
    {
        const int requestedTake = 620;

        await using var app = await CreateAppAsync(a => a.AddJob("tests-runs-unbounded", () => "ok"));
        var clientApi = app.Services.GetRequiredService<IJobClient>();

        for (var i = 0; i < requestedTake; i++)
        {
            await clientApi.TriggerAsync("tests-runs-unbounded");
        }

        using var client = app.GetTestClient();
        var page = await client.GetFromJsonAsync<PagedResponse<RunResponse>>(
            $"/surefire/api/runs?jobName=tests-runs-unbounded&exactJobName=true&skip=0&take={requestedTake}");

        Assert.NotNull(page);
        Assert.Equal(500, page.Items.Count);
        Assert.Equal(requestedTake, page.TotalCount);
    }

    [Fact]
    public async Task JobsEndpoint_ReturnsRegisteredJob()
    {
        await using var app = await CreateAppAsync(a => a.AddJob("tests-job", () => "ok"));

        using var client = app.GetTestClient();
        var jobs = await client.GetFromJsonAsync<List<JobResponse>>("/surefire/api/jobs");

        Assert.NotNull(jobs);
        Assert.Contains(jobs, j => j.Name == "tests-job");
    }

    [Fact]
    public async Task JobDetailEndpoint_AutoGeneratesArgumentsSchema_FromHandlerParameters()
    {
        await using var app = await CreateAppAsync(a =>
        {
            a.AddJob("tests-add", (int a, int b) => a + b);
            a.AddJob("tests-service-only", (ILogger<DashboardEndpointsTests> logger) => logger.LogInformation("ok"));
        });

        using var client = app.GetTestClient();

        var addJob = await client.GetFromJsonAsync<JobResponse>("/surefire/api/jobs/tests-add");
        Assert.NotNull(addJob);
        Assert.True(addJob.ArgumentsSchema.HasValue);
        Assert.Equal("object", addJob.ArgumentsSchema.Value.GetProperty("type").GetString());

        var addProps = addJob.ArgumentsSchema.Value.GetProperty("properties");
        Assert.Equal("integer", addProps.GetProperty("a").GetProperty("type").GetString());
        Assert.Equal("integer", addProps.GetProperty("b").GetProperty("type").GetString());

        var required = addJob.ArgumentsSchema.Value.GetProperty("required")
            .EnumerateArray()
            .Select(e => e.GetString())
            .ToHashSet(StringComparer.Ordinal);
        Assert.Contains("a", required);
        Assert.Contains("b", required);

        var serviceOnlyJob = await client.GetFromJsonAsync<JobResponse>("/surefire/api/jobs/tests-service-only");
        Assert.NotNull(serviceOnlyJob);
        Assert.False(serviceOnlyJob.ArgumentsSchema.HasValue);
    }

    [Fact]
    public async Task PatchJobEndpoint_TogglesIsEnabled()
    {
        await using var app = await CreateAppAsync(a => a.AddJob("tests-toggle", () => "ok"));

        using var client = app.GetTestClient();

        var disableResponse = await SendPatchAsync(client, "/surefire/api/jobs/tests-toggle", new { isEnabled = false });
        Assert.Equal(HttpStatusCode.OK, disableResponse.StatusCode);

        var disabled = await disableResponse.Content.ReadFromJsonAsync<JobResponse>();
        Assert.NotNull(disabled);
        Assert.False(disabled.IsEnabled);

        var enableResponse = await SendPatchAsync(client, "/surefire/api/jobs/tests-toggle", new { isEnabled = true });
        Assert.Equal(HttpStatusCode.OK, enableResponse.StatusCode);

        var enabled = await enableResponse.Content.ReadFromJsonAsync<JobResponse>();
        Assert.NotNull(enabled);
        Assert.True(enabled.IsEnabled);
    }

    [Fact]
    public async Task PatchQueueEndpoint_UnknownQueue_ReturnsNotFound()
    {
        await using var app = await CreateAppAsync();
        using var client = app.GetTestClient();

        var response = await SendPatchAsync(client, "/surefire/api/queues/tests-missing-queue", new { isPaused = true });

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task TriggerRunEndpoint_ReturnsRunId()
    {
        await using var app = await CreateAppAsync(a => a.AddJob("tests-trigger", () => "ok"));

        using var client = app.GetTestClient();
        var response = await client.PostAsJsonAsync("/surefire/api/jobs/tests-trigger/trigger", new { args = (object?)null });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var payload = await response.Content.ReadFromJsonAsync<RunIdResponse>();
        Assert.NotNull(payload);
        Assert.False(string.IsNullOrWhiteSpace(payload.RunId));
    }

    [Fact]
    public async Task JobDetailEndpoint_IncludesArgumentsSchema_ForUiFormGeneration()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        await store.UpsertJobAsync(new JobDefinition
        {
            Name = "tests-trigger-schema",
            Queue = "default",
            ArgumentsSchema = "{\"type\":\"object\",\"properties\":{\"count\":{\"type\":\"integer\"}},\"required\":[\"count\"]}"
        });

        using var client = app.GetTestClient();
        var job = await client.GetFromJsonAsync<JobResponse>("/surefire/api/jobs/tests-trigger-schema");

        Assert.NotNull(job);
        Assert.True(job.ArgumentsSchema.HasValue);
        Assert.Equal("object", job.ArgumentsSchema.Value.GetProperty("type").GetString());
        Assert.True(job.ArgumentsSchema.Value.GetProperty("properties").TryGetProperty("count", out _));
    }

    [Fact]
    public async Task JobStatsEndpoint_ReturnsSuccessRateAsPercent()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;

        await store.UpsertJobAsync(new JobDefinition
        {
            Name = "tests-job-stats-percent",
            Queue = "default"
        });

        await store.CreateRunsAsync([
            new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = "tests-job-stats-percent",
                Status = JobStatus.Succeeded,
                CreatedAt = now,
                NotBefore = now,
                StartedAt = now,
                CompletedAt = now,
                Attempt = 1,
                Progress = 1
            }
        ]);

        using var client = app.GetTestClient();
        var stats = await client.GetFromJsonAsync<JobStatsResponse>("/surefire/api/jobs/tests-job-stats-percent/stats");

        Assert.NotNull(stats);
        Assert.Equal(100, stats.SuccessRate);
    }

    [Fact]
    public async Task StatsEndpoint_ReturnsSuccessRateAsPercent()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;

        await store.UpsertJobAsync(new JobDefinition
        {
            Name = "tests-stats-success-rate-ratio",
            Queue = "default"
        });

        await store.CreateRunsAsync([
            new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = "tests-stats-success-rate-ratio",
                Status = JobStatus.Succeeded,
                CreatedAt = now,
                NotBefore = now,
                StartedAt = now,
                CompletedAt = now,
                Attempt = 1,
                Progress = 1
            },
            new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = "tests-stats-success-rate-ratio",
                Status = JobStatus.Failed,
                CreatedAt = now,
                NotBefore = now,
                StartedAt = now,
                CompletedAt = now,
                Attempt = 1,
                Progress = 1,
                Error = "boom"
            }
        ]);

        using var client = app.GetTestClient();
        var stats = await client.GetFromJsonAsync<DashboardStatsResponse>("/surefire/api/stats");

        Assert.NotNull(stats);
        Assert.Equal(50, stats.SuccessRate, 5);
    }

    [Fact]
    public async Task CancelRunEndpoint_MissingRun_ReturnsNotFound()
    {
        await using var app = await CreateAppAsync();
        using var client = app.GetTestClient();

        var response = await client.PostAsync($"/surefire/api/runs/{Guid.CreateVersion7():N}/cancel", null);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task CancelRunEndpoint_TerminalRun_IsNoOp()
    {
        await using var app = await CreateAppAsync(a => a.AddJob("tests-cancel-terminal", () => "ok"));
        var api = app.Services.GetRequiredService<IJobClient>();
        var run = await api.TriggerAsync("tests-cancel-terminal");
        var runId = run.Id;
        await api.WaitAsync(runId);

        using var client = app.GetTestClient();
        var response = await client.PostAsync($"/surefire/api/runs/{runId}/cancel", null);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task RerunEndpoint_MissingRun_ReturnsNotFound()
    {
        await using var app = await CreateAppAsync();
        using var client = app.GetTestClient();

        var response = await client.PostAsync($"/surefire/api/runs/{Guid.CreateVersion7():N}/rerun", null);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task RunTraceEndpoint_ForBatchChild_IncludesSiblings()
    {
        await using var app = await CreateAppAsync(a =>
            a.AddJob("tests-batch", (int n) => n));

        var clientApi = app.Services.GetRequiredService<IJobClient>();
        var batchId = (await clientApi.TriggerBatchAsync("tests-batch", new object?[] { 1, 2, 3 })).Id;

        var store = app.Services.GetRequiredService<IJobStore>();
        var children = await store.GetRunsAsync(new RunFilter { BatchId = batchId }, take: 10);
        var childId = children.Items[0].Id;

        using var client = app.GetTestClient();
        var tracePage = await client.GetFromJsonAsync<PagedResponse<RunResponse>>(
            $"/surefire/api/runs/{childId}/trace?skip=0&take=500");

        Assert.NotNull(tracePage);
        // All 3 batch siblings are included in the trace
        Assert.Equal(3, tracePage.Items.Count);
        Assert.Equal(3, tracePage.TotalCount);
        Assert.DoesNotContain(tracePage.Items, r => r.Id == batchId);
    }

    [Fact]
    public async Task RunTraceEndpoint_PaginatesDescendantsAcrossPages()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        var rootId = Guid.CreateVersion7().ToString("N");
        const int childCount = 1200;

        var root = new JobRun
        {
            Id = rootId,
            JobName = "tests-trace-unbounded",
            Status = JobStatus.Running,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            NodeName = "node-1",
            Attempt = 1,
            Progress = 0
        };

        var children = Enumerable.Range(0, childCount)
            .Select(i =>
            {
                var createdAt = now.AddMilliseconds(-(childCount - i));
                return new JobRun
                {
                    Id = Guid.CreateVersion7().ToString("N"),
                    JobName = "tests-trace-unbounded",
                    Status = JobStatus.Succeeded,
                    ParentRunId = rootId,
                    RootRunId = rootId,
                    CreatedAt = createdAt,
                    NotBefore = createdAt,
                    StartedAt = createdAt,
                    CompletedAt = createdAt,
                    NodeName = "node-1",
                    Attempt = 1,
                    Progress = 1,
                    Result = "{}"
                };
            })
            .ToList();

        await store.CreateRunsAsync([root, ..children]);

        using var client = app.GetTestClient();
        var page1 = await client.GetFromJsonAsync<PagedResponse<RunResponse>>(
            $"/surefire/api/runs/{rootId}/trace?skip=0&take=500");
        var page2 = await client.GetFromJsonAsync<PagedResponse<RunResponse>>(
            $"/surefire/api/runs/{rootId}/trace?skip=500&take=500");
        var page3 = await client.GetFromJsonAsync<PagedResponse<RunResponse>>(
            $"/surefire/api/runs/{rootId}/trace?skip=1000&take=500");

        Assert.NotNull(page1);
        Assert.NotNull(page2);
        Assert.NotNull(page3);

        var combined = page1.Items.Concat(page2.Items).Concat(page3.Items).ToList();

        Assert.Equal(childCount + 1, page1.TotalCount);
        Assert.Equal(childCount + 1, page2.TotalCount);
        Assert.Equal(childCount + 1, page3.TotalCount);
        Assert.Equal(500, page1.Items.Count);
        Assert.Equal(500, page2.Items.Count);
        Assert.Equal(201, page3.Items.Count);
        Assert.Equal(rootId, page1.Items[0].Id);

        var distinctIds = combined.Select(r => r.Id).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(childCount + 1, distinctIds.Count);
        Assert.Equal(childCount, combined.Count(r => r.RootRunId == rootId));
    }

    [Fact]
    public async Task DashboardApiBasePath_ReturnsNotFound()
    {
        await using var app = await CreateAppAsync();
        using var client = app.GetTestClient();

        var response = await client.GetAsync("/surefire/api");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task StreamEndpoint_ResumesFromLastEventId_AndSendsDoneForTerminalRun()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        var jobName = "tests-stream-resume";
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var now = DateTimeOffset.UtcNow;
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Succeeded,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            CompletedAt = now,
            NodeName = "node-1",
            Attempt = 1,
            Progress = 1,
            Result = "{}"
        };

        await store.CreateRunsAsync([run]);
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "101",
                CreatedAt = now,
                Attempt = 1
            },
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "102",
                CreatedAt = now,
                Attempt = 1
            },
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = 1
            },
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.AttemptFailure,
                Payload = "{\"attempt\":1,\"message\":\"boom\",\"exceptionType\":\"System.InvalidOperationException\",\"stackTrace\":\"trace\"}",
                CreatedAt = now,
                Attempt = 1
            }
        ]);

        var events = await store.GetEventsAsync(run.Id, 0);
        Assert.Equal(4, events.Count);

        using var client = app.GetTestClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/surefire/api/runs/{run.Id}/stream");
        request.Headers.TryAddWithoutValidation("Last-Event-ID", events[0].Id.ToString());

        using var response = await client.SendAsync(request);
        var body = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains($"id: {events[1].Id}", body);
        Assert.Contains($"id: {events[2].Id}", body);
        Assert.Contains($"id: {events[3].Id}", body);
        Assert.DoesNotContain($"id: {events[0].Id}", body);
        Assert.Contains("event: done", body);
    }

    [Fact]
    public async Task StreamEndpoint_EmitsInputAndOutputEventTypes()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        var jobName = "tests-stream-event-types";
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var now = DateTimeOffset.UtcNow;
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Succeeded,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            CompletedAt = now,
            NodeName = "node-1",
            Attempt = 1,
            Progress = 1,
            Result = "{}"
        };

        await store.CreateRunsAsync([run]);
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Input,
                Payload = "{\"param\":\"values\",\"value\":1}",
                CreatedAt = now,
                Attempt = 1
            },
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.InputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = 1
            },
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "101",
                CreatedAt = now,
                Attempt = 1
            },
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = 1
            },
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.AttemptFailure,
                Payload = "{\"attempt\":1,\"message\":\"boom\",\"exceptionType\":\"System.InvalidOperationException\",\"stackTrace\":\"trace\"}",
                CreatedAt = now,
                Attempt = 1
            }
        ]);

        using var client = app.GetTestClient();
        using var response = await client.GetAsync($"/surefire/api/runs/{run.Id}/stream");
        var body = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("event: input", body);
        Assert.Contains("event: inputComplete", body);
        Assert.Contains("event: output", body);
        Assert.Contains("event: outputComplete", body);
        Assert.Contains("event: attemptFailure", body);
    }

    [Fact]
    public async Task StreamEndpoint_TerminalRunWithoutEvents_StillSendsDone()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        var jobName = "tests-stream-terminal";
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var now = DateTimeOffset.UtcNow;
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Succeeded,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            CompletedAt = now,
            NodeName = "node-1",
            Attempt = 1,
            Progress = 1,
            Result = "{}"
        };

        await store.CreateRunsAsync([run]);

        using var client = app.GetTestClient();
        var response = await client.GetAsync($"/surefire/api/runs/{run.Id}/stream");
        var body = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("event: done", body);
    }

    [Fact]
    public async Task RunLogsEndpoint_SkipsMalformedLogPayloads()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        var jobName = "tests-logs-malformed";
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var now = DateTimeOffset.UtcNow;
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Succeeded,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            CompletedAt = now,
            NodeName = "node-1",
            Attempt = 1,
            Progress = 1,
            Result = "{}"
        };

        await store.CreateRunsAsync([run]);
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Log,
                Payload = "not-json",
                CreatedAt = now,
                Attempt = 1
            },
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Log,
                Payload = JsonSerializer.Serialize(new { level = "info", message = "ok" }),
                CreatedAt = now,
                Attempt = 1
            }
        ]);

        using var client = app.GetTestClient();
        var response = await client.GetFromJsonAsync<JsonElement>($"/surefire/api/runs/{run.Id}/logs");

        var items = response.GetProperty("items");
        Assert.Equal(1, items.GetArrayLength());
        Assert.Equal("ok", items[0].GetProperty("message").GetString());
    }

    [Fact]
    public async Task StreamEndpoint_DrainsLateEventsBeforeDone()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();
        var notifications = app.Services.GetRequiredService<INotificationProvider>();

        var jobName = "tests-stream-late-events";
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var now = DateTimeOffset.UtcNow;
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Running,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            LastHeartbeatAt = now,
            NodeName = "node-1",
            Attempt = 1,
            Progress = 0.4
        };

        await store.CreateRunsAsync([run]);

        using var client = app.GetTestClient();
        var streamTask = client.GetStringAsync($"/surefire/api/runs/{run.Id}/stream");

        await Task.Delay(40);

        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "1",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ]);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id);

        var transition = RunStatusTransition.RunningToSucceeded(
            run.Id,
            run.Attempt,
            DateTimeOffset.UtcNow,
            run.NotBefore,
            run.NodeName,
            1,
            "[1,2]",
            null,
            run.StartedAt,
            DateTimeOffset.UtcNow);
        Assert.True(await store.TryTransitionRunAsync(transition));
        await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id);

        // Simulate an event appended in the narrow completion window.
        await Task.Delay(5);
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "2",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ]);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id);

        var body = await streamTask;
        Assert.Contains("data: 1", body);
        Assert.Contains("data: 2", body);
        Assert.Contains("event: done", body);
    }

    [Fact]
    public async Task StreamEndpoint_CompletesWhenRunBecomesTerminal_WithoutCompletionNotification()
    {
        await using var app = await CreateAppAsync();

        var store = app.Services.GetRequiredService<IJobStore>();

        var jobName = "tests-stream-terminal-without-completed-notification";
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var now = DateTimeOffset.UtcNow;
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Running,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            LastHeartbeatAt = now,
            NodeName = "node-1",
            Attempt = 1,
            Progress = 0.5
        };

        await store.CreateRunsAsync([run]);

        using var client = app.GetTestClient();
        var streamTask = client.GetStringAsync($"/surefire/api/runs/{run.Id}/stream");

        await Task.Delay(40);

        var completedTransition = RunStatusTransition.RunningToSucceeded(
            run.Id,
            run.Attempt,
            DateTimeOffset.UtcNow,
            run.NotBefore,
            run.NodeName,
            1,
            "{}",
            null,
            run.StartedAt,
            DateTimeOffset.UtcNow);

        Assert.True(await store.TryTransitionRunAsync(completedTransition));

        var body = await streamTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Contains("event: done", body);
    }

    private static async Task<WebApplication> CreateAppAsync(Action<WebApplication>? configure = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();

        builder.Services.AddSurefire(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(10);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
        });

        var app = builder.Build();
    configure?.Invoke(app);
        app.MapSurefireDashboard("/surefire");

        await app.StartAsync();
        return app;
    }

    private static Task<HttpResponseMessage> SendPatchAsync(HttpClient client, string uri, object payload)
    {
        var request = new HttpRequestMessage(HttpMethod.Patch, uri)
        {
            Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
        };

        return client.SendAsync(request);
    }
}
