using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Surefire.Dashboard;

namespace Surefire.Tests.Dashboard;

public sealed class DashboardEndpointsTests
{
    [Fact]
    public async Task RunsEndpoint_CapsTakeAt500()
    {
        var ct = TestContext.Current.CancellationToken;
        const int requestedTake = 620;

        await using var app = await CreateAppAsync(a => a.AddJob("tests-runs-unbounded", () => "ok"), ct);
        var clientApi = app.Services.GetRequiredService<IJobClient>();

        for (var i = 0; i < requestedTake; i++)
        {
            await clientApi.TriggerAsync("tests-runs-unbounded", cancellationToken: ct);
        }

        using var client = app.GetTestClient();
        var page = await client.GetFromJsonAsync<PagedResponse<RunResponse>>(
            $"/surefire/api/runs?jobName=tests-runs-unbounded&exactJobName=true&skip=0&take={requestedTake}", ct);

        Assert.NotNull(page);
        Assert.Equal(500, page.Items.Count);
        Assert.Equal(requestedTake, page.TotalCount);
    }

    [Fact]
    public async Task JobsEndpoint_ReturnsRegisteredJob()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(a => a.AddJob("tests-job", () => "ok"), ct);

        using var client = app.GetTestClient();
        var jobs = await client.GetFromJsonAsync<List<JobResponse>>("/surefire/api/jobs", ct);

        Assert.NotNull(jobs);
        Assert.Contains(jobs, j => j.Name == "tests-job");
    }

    [Fact]
    public async Task JobDetailEndpoint_AutoGeneratesArgumentsSchema_FromHandlerParameters()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(a =>
        {
            a.AddJob("tests-add", (int a, int b) => a + b);
            a.AddJob("tests-service-only", (ILogger<DashboardEndpointsTests> logger) => logger.LogInformation("ok"));
        }, ct);

        using var client = app.GetTestClient();

        var addJob = await client.GetFromJsonAsync<JobResponse>("/surefire/api/jobs/tests-add", ct);
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

        var serviceOnlyJob = await client.GetFromJsonAsync<JobResponse>("/surefire/api/jobs/tests-service-only", ct);
        Assert.NotNull(serviceOnlyJob);
        Assert.False(serviceOnlyJob.ArgumentsSchema.HasValue);
    }

    [Fact]
    public async Task JobsEndpoint_InvalidTimeZone_ReturnsNullNextRunAt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        await store.UpsertJobAsync(new()
        {
            Name = "tests-invalid-timezone",
            Queue = "default",
            CronExpression = "0 9 * * *",
            TimeZoneId = "Invalid/Zone"
        }, ct);

        using var client = app.GetTestClient();
        var job = await client.GetFromJsonAsync<JobResponse>("/surefire/api/jobs/tests-invalid-timezone", ct);

        Assert.NotNull(job);
        Assert.Null(job.NextRunAt);
    }

    [Fact]
    public async Task PatchJobEndpoint_TogglesIsEnabled()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(a => a.AddJob("tests-toggle", () => "ok"), ct);

        using var client = app.GetTestClient();

        var disableResponse =
            await SendPatchAsync(client, "/surefire/api/jobs/tests-toggle", new { isEnabled = false }, ct);
        Assert.Equal(HttpStatusCode.OK, disableResponse.StatusCode);

        var disabled = await disableResponse.Content.ReadFromJsonAsync<JobResponse>(ct);
        Assert.NotNull(disabled);
        Assert.False(disabled.IsEnabled);

        var enableResponse =
            await SendPatchAsync(client, "/surefire/api/jobs/tests-toggle", new { isEnabled = true }, ct);
        Assert.Equal(HttpStatusCode.OK, enableResponse.StatusCode);

        var enabled = await enableResponse.Content.ReadFromJsonAsync<JobResponse>(ct);
        Assert.NotNull(enabled);
        Assert.True(enabled.IsEnabled);
    }

    [Fact]
    public async Task PatchQueueEndpoint_ImplicitQueue_CreatesAndPauses()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);
        using var client = app.GetTestClient();

        var response = await SendPatchAsync(client, "/surefire/api/queues/tests-missing-queue", new { isPaused = true },
            ct);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task TriggerRunEndpoint_ReturnsRunId()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(a => a.AddJob("tests-trigger", () => "ok"), ct);

        using var client = app.GetTestClient();
        var response = await client.PostAsJsonAsync("/surefire/api/jobs/tests-trigger/trigger",
            new { args = (object?)null }, ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var payload = await response.Content.ReadFromJsonAsync<RunIdResponse>(ct);
        Assert.NotNull(payload);
        Assert.False(string.IsNullOrWhiteSpace(payload.RunId));
    }

    [Fact]
    public async Task JobDetailEndpoint_IncludesArgumentsSchema_ForUiFormGeneration()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        await store.UpsertJobAsync(new()
        {
            Name = "tests-trigger-schema",
            Queue = "default",
            ArgumentsSchema =
                "{\"type\":\"object\",\"properties\":{\"count\":{\"type\":\"integer\"}},\"required\":[\"count\"]}"
        }, ct);

        using var client = app.GetTestClient();
        var job = await client.GetFromJsonAsync<JobResponse>("/surefire/api/jobs/tests-trigger-schema", ct);

        Assert.NotNull(job);
        Assert.True(job.ArgumentsSchema.HasValue);
        Assert.Equal("object", job.ArgumentsSchema.Value.GetProperty("type").GetString());
        Assert.True(job.ArgumentsSchema.Value.GetProperty("properties").TryGetProperty("count", out _));
    }

    [Fact]
    public async Task JobStatsEndpoint_ReturnsSuccessRateAsPercent()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;

        await store.UpsertJobAsync(new()
        {
            Name = "tests-job-stats-percent",
            Queue = "default"
        }, ct);

        await store.CreateRunsAsync([
            new()
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
        ], cancellationToken: ct);

        using var client = app.GetTestClient();
        var stats = await client.GetFromJsonAsync<JobStatsResponse>("/surefire/api/jobs/tests-job-stats-percent/stats",
            ct);

        Assert.NotNull(stats);
        Assert.Equal(100, stats.SuccessRate);
    }

    [Fact]
    public async Task StatsEndpoint_ReturnsSuccessRateAsPercent()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;

        await store.UpsertJobAsync(new()
        {
            Name = "tests-stats-success-rate-ratio",
            Queue = "default"
        }, ct);

        await store.CreateRunsAsync([
            new()
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
            new()
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
                Reason = "boom"
            }
        ], cancellationToken: ct);

        using var client = app.GetTestClient();
        var stats = await client.GetFromJsonAsync<DashboardStatsResponse>("/surefire/api/stats", ct);

        Assert.NotNull(stats);
        Assert.Equal(50, stats.SuccessRate, 5);
    }

    [Fact]
    public async Task CancelRunEndpoint_MissingRun_ReturnsNotFound()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);
        using var client = app.GetTestClient();

        var response = await client.PostAsync($"/surefire/api/runs/{Guid.CreateVersion7():N}/cancel", null, ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task CancelRunEndpoint_TerminalRun_IsNoOp()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(a => a.AddJob("tests-cancel-terminal", () => "ok"), ct);
        var api = app.Services.GetRequiredService<IJobClient>();
        var run = await api.TriggerAsync("tests-cancel-terminal", cancellationToken: ct);
        var runId = run.Id;
        await api.WaitAsync(runId, ct);

        using var client = app.GetTestClient();
        var response = await client.PostAsync($"/surefire/api/runs/{runId}/cancel", null, ct);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task RerunEndpoint_MissingRun_ReturnsNotFound()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);
        using var client = app.GetTestClient();

        var response = await client.PostAsync($"/surefire/api/runs/{Guid.CreateVersion7():N}/rerun", null, ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task RunTraceEndpoint_RunWithBothParentAndBatch_UsesParentBranch()
    {
        // A run can have both ParentRunId (hierarchical parent) and BatchId (belongs to a
        // batch triggered by that parent). The parent branch owns the sibling window — the
        // batch branch only applies when ParentRunId is null.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        var batchId = Guid.CreateVersion7().ToString("N");
        var parentId = Guid.CreateVersion7().ToString("N");

        var parent = new JobRun
        {
            Id = parentId,
            JobName = "tests-trace-pb",
            Status = JobStatus.Succeeded,
            CreatedAt = now.AddSeconds(-10),
            NotBefore = now.AddSeconds(-10),
            CompletedAt = now.AddSeconds(-10),
            Attempt = 1,
            Progress = 1
        };

        // Three children all under the same parent AND the same batch.
        var children = Enumerable.Range(0, 3).Select(i =>
        {
            var id = Guid.CreateVersion7().ToString("N");
            var createdAt = now.AddMilliseconds(i);
            return new JobRun
            {
                Id = id,
                JobName = "tests-trace-pb",
                Status = JobStatus.Succeeded,
                ParentRunId = parentId,
                RootRunId = parentId,
                BatchId = batchId,
                CreatedAt = createdAt,
                NotBefore = createdAt,
                CompletedAt = createdAt,
                Attempt = 1,
                Progress = 1
            };
        }).ToList();

        await store.CreateRunsAsync([parent, ..children], cancellationToken: ct);

        using var client = app.GetTestClient();
        var trace = await client.GetFromJsonAsync<RunTraceResponse>(
            $"/surefire/api/runs/{children[1].Id}/trace", ct);

        Assert.NotNull(trace);
        // Parent branch used because ParentRunId is set — siblings populated via direct-children keyset.
        Assert.Single(trace.Ancestors);
        Assert.Equal(parentId, trace.Ancestors[0].Id);
        Assert.Single(trace.SiblingsBefore);
        Assert.Equal(children[0].Id, trace.SiblingsBefore[0].Id);
        Assert.Single(trace.SiblingsAfter);
        Assert.Equal(children[2].Id, trace.SiblingsAfter[0].Id);
    }

    [Fact]
    public async Task RunTraceEndpoint_ForBatchChild_ReturnsFocusWithoutSiblings()
    {
        // Batch children share an atomic CreatedAt across the whole batch; a
        // (createdAt, id) keyset split around the focus is not expressible via RunFilter.
        // The trace endpoint therefore returns no siblings for batch focus — users
        // browse batch siblings via the dedicated batch page. The trace still carries
        // the focus's own identity + ancestors + children so it behaves uniformly.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(a =>
            a.AddJob("tests-batch", (int n) => n), ct);

        var clientApi = app.Services.GetRequiredService<IJobClient>();
        var batchId =
            (await clientApi.TriggerBatchAsync("tests-batch", new object?[] { 1, 2, 3 }, cancellationToken: ct)).Id;

        var store = app.Services.GetRequiredService<IJobStore>();
        var children = await store.GetRunsAsync(
            new() { BatchId = batchId, OrderBy = RunOrderBy.CreatedAt },
            take: 10, cancellationToken: ct);
        var focusId = children.Items[1].Id;

        using var client = app.GetTestClient();
        var trace = await client.GetFromJsonAsync<RunTraceResponse>(
            $"/surefire/api/runs/{focusId}/trace", ct);

        Assert.NotNull(trace);
        Assert.Empty(trace.Ancestors);
        Assert.Equal(focusId, trace.Focus.Id);
        Assert.Empty(trace.SiblingsBefore);
        Assert.Empty(trace.SiblingsAfter);
        Assert.Null(trace.SiblingsCursor);
    }

    [Fact]
    public async Task RunTraceEndpoint_ReturnsAncestorChainAndChildren_WithCursor()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        var rootId = Guid.CreateVersion7().ToString("N");
        var focusId = Guid.CreateVersion7().ToString("N");
        const int childCount = 250;

        var root = new JobRun
        {
            Id = rootId, JobName = "tests-trace", Status = JobStatus.Running,
            CreatedAt = now.AddSeconds(-10), NotBefore = now.AddSeconds(-10),
            StartedAt = now.AddSeconds(-10), NodeName = "node-1", Attempt = 1, Progress = 0.1
        };

        var focus = new JobRun
        {
            Id = focusId, JobName = "tests-trace", Status = JobStatus.Running,
            ParentRunId = rootId, RootRunId = rootId,
            CreatedAt = now.AddSeconds(-5), NotBefore = now.AddSeconds(-5),
            StartedAt = now.AddSeconds(-5), NodeName = "node-1", Attempt = 1, Progress = 0.5
        };

        var children = Enumerable.Range(0, childCount)
            .Select(i =>
            {
                var createdAt = now.AddMilliseconds(i);
                return new JobRun
                {
                    Id = Guid.CreateVersion7().ToString("N"), JobName = "tests-trace",
                    Status = JobStatus.Succeeded, ParentRunId = focusId, RootRunId = rootId,
                    CreatedAt = createdAt, NotBefore = createdAt, StartedAt = createdAt,
                    CompletedAt = createdAt, NodeName = "node-1", Attempt = 1, Progress = 1,
                    Result = "{}"
                };
            })
            .ToList();

        await store.CreateRunsAsync([root, focus, ..children], cancellationToken: ct);

        using var client = app.GetTestClient();
        var trace = await client.GetFromJsonAsync<RunTraceResponse>(
            $"/surefire/api/runs/{focusId}/trace?childrenTake=100", ct);

        Assert.NotNull(trace);
        Assert.Single(trace.Ancestors);
        Assert.Equal(rootId, trace.Ancestors[0].Id);
        Assert.Equal(0, trace.Ancestors[0].Depth);
        Assert.Equal(focusId, trace.Focus.Id);
        Assert.Equal(1, trace.Focus.Depth);
        Assert.Equal(100, trace.Children.Count);
        Assert.All(trace.Children, c => Assert.Equal(2, c.Depth));
        Assert.NotNull(trace.ChildrenCursor);

        // Drain remaining children via /children endpoint.
        var remaining = new List<RunResponse>();
        var cursor = trace.ChildrenCursor;
        while (cursor is { })
        {
            var page = await client.GetFromJsonAsync<RunChildrenResponse>(
                $"/surefire/api/runs/{focusId}/children?afterCursor={Uri.EscapeDataString(cursor)}&take=100", ct);
            Assert.NotNull(page);
            remaining.AddRange(page.Items);
            cursor = page.NextCursor;
        }

        Assert.Equal(childCount - 100, remaining.Count);
    }

    [Fact]
    public async Task DashboardApiBasePath_ReturnsNotFound()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);
        using var client = app.GetTestClient();

        var response = await client.GetAsync("/surefire/api", ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task StreamEndpoint_ResumesFromLastEventId_AndSendsDoneForTerminalRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var jobName = "tests-stream-resume";
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

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

        await store.CreateRunsAsync([run], cancellationToken: ct);
        await store.AppendEventsAsync([
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "101",
                CreatedAt = now,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "102",
                CreatedAt = now,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.AttemptFailure,
                Payload =
                    "{\"attempt\":1,\"message\":\"boom\",\"exceptionType\":\"System.InvalidOperationException\",\"stackTrace\":\"trace\"}",
                CreatedAt = now,
                Attempt = 1
            },
            CreateTerminalStatusEvent(run, now)
        ], ct);

        var events = await store.GetEventsAsync(run.Id, cancellationToken: ct);
        Assert.Equal(5, events.Count);

        using var client = app.GetTestClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/surefire/api/runs/{run.Id}/stream");
        request.Headers.TryAddWithoutValidation("Last-Event-ID", events[0].Id.ToString());

        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);

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
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var jobName = "tests-stream-event-types";
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

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

        await store.CreateRunsAsync([run], cancellationToken: ct);
        await store.AppendEventsAsync([
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Input,
                Payload = "{\"param\":\"values\",\"value\":1}",
                CreatedAt = now,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.InputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "101",
                CreatedAt = now,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.AttemptFailure,
                Payload =
                    "{\"attempt\":1,\"message\":\"boom\",\"exceptionType\":\"System.InvalidOperationException\",\"stackTrace\":\"trace\"}",
                CreatedAt = now,
                Attempt = 1
            },
            CreateTerminalStatusEvent(run, now)
        ], ct);

        using var client = app.GetTestClient();
        using var response = await client.GetAsync($"/surefire/api/runs/{run.Id}/stream", ct);
        var body = await response.Content.ReadAsStringAsync(ct);

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
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var jobName = "tests-stream-terminal";
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

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

        await store.CreateRunsAsync([run], cancellationToken: ct);
        await store.AppendEventsAsync([CreateTerminalStatusEvent(run, now)], ct);

        using var client = app.GetTestClient();
        var response = await client.GetAsync($"/surefire/api/runs/{run.Id}/stream", ct);
        var body = await response.Content.ReadAsStringAsync(ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("event: done", body);
    }

    [Fact]
    public async Task RunLogsEndpoint_SkipsMalformedLogPayloads()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var jobName = "tests-logs-malformed";
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

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

        await store.CreateRunsAsync([run], cancellationToken: ct);
        await store.AppendEventsAsync([
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Log,
                Payload = "not-json",
                CreatedAt = now,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Log,
                Payload = JsonSerializer.Serialize(new { level = "info", message = "ok" }),
                CreatedAt = now,
                Attempt = 1
            }
        ], ct);

        using var client = app.GetTestClient();
        var response = await client.GetFromJsonAsync<JsonElement>($"/surefire/api/runs/{run.Id}/logs", ct);

        var items = response.GetProperty("items");
        Assert.Equal(1, items.GetArrayLength());
        Assert.Equal("ok", items[0].GetProperty("message").GetString());
    }

    [Fact]
    public async Task StreamEndpoint_DrainsLateEventsBeforeDone()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var notifications = app.Services.GetRequiredService<INotificationProvider>();

        var jobName = "tests-stream-late-events";
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

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

        await store.CreateRunsAsync([run], cancellationToken: ct);

        using var client = app.GetTestClient();
        var streamTask = client.GetStringAsync($"/surefire/api/runs/{run.Id}/stream", ct);

        await Task.Delay(40, ct);

        await store.AppendEventsAsync([
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "1",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ], ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, ct);

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
        Assert.True((await store.TryTransitionRunAsync(transition, ct)).Transitioned);
        await store.AppendEventsAsync([
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "2",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            CreateTerminalStatusEvent(run with { Status = JobStatus.Succeeded }, DateTimeOffset.UtcNow)
        ], ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, ct);
        await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id, ct);

        var body = await streamTask;
        Assert.Contains("data: 1", body);
        Assert.Contains("data: 2", body);
        Assert.Contains("event: done", body);
    }

    [Fact]
    public async Task StreamEndpoint_CompletesWhenRunBecomesTerminal_WithoutCompletionNotification()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();

        var jobName = "tests-stream-terminal-without-completed-notification";
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

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

        await store.CreateRunsAsync([run], cancellationToken: ct);

        using var client = app.GetTestClient();
        var streamTask = client.GetStringAsync($"/surefire/api/runs/{run.Id}/stream", ct);

        await Task.Delay(40, ct);

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

        Assert.True((await store.TryTransitionRunAsync(completedTransition, ct)).Transitioned);
        await store.AppendEventsAsync([
            CreateTerminalStatusEvent(run with { Status = JobStatus.Succeeded }, DateTimeOffset.UtcNow)
        ], ct);

        var body = await streamTask.WaitAsync(TimeSpan.FromSeconds(5), ct);
        Assert.Contains("event: done", body);
    }

    private static async Task<WebApplication> CreateAppAsync(Action<WebApplication>? configure = null,
        CancellationToken ct = default)
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
        app.MapSurefireDashboard();

        await app.StartAsync(ct);
        return app;
    }

    private static Task<HttpResponseMessage> SendPatchAsync(HttpClient client, string uri, object payload,
        CancellationToken ct = default)
    {
        var request = new HttpRequestMessage(HttpMethod.Patch, uri)
        {
            Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
        };

        return client.SendAsync(request, ct);
    }

    private static RunEvent CreateTerminalStatusEvent(JobRun run, DateTimeOffset createdAt) => new()
    {
        RunId = run.Id,
        EventType = RunEventType.Status,
        Payload = ((int)run.Status).ToString(),
        CreatedAt = createdAt,
        Attempt = run.Attempt
    };
}