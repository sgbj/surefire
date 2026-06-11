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
            $"/surefire/api/runs?jobName=tests-runs-unbounded&skip=0&take={requestedTake}", ct);

        Assert.NotNull(page);
        Assert.Equal(500, page.Items.Count);
        Assert.Equal(requestedTake, page.TotalCount);
    }

    [Fact]
    public async Task RunsLookupEndpoint_ReturnsFreshRunsById()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        const string jobName = "tests-runs-lookup";
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var running = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Running,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            NodeName = "node-1",
            Attempt = 1,
            Progress = 0.5
        };
        var succeeded = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Succeeded,
            CreatedAt = now.AddMilliseconds(1),
            NotBefore = now.AddMilliseconds(1),
            StartedAt = now.AddMilliseconds(1),
            CompletedAt = now.AddMilliseconds(2),
            Attempt = 1,
            Progress = 1
        };

        await store.CreateRunsAsync([running, succeeded], cancellationToken: ct);

        var transition = RunStatusTransition.RunningToSucceeded(
            running.Id,
            running.LeaseEpoch,
            now.AddSeconds(1),
            running.NotBefore,
            running.NodeName,
            1,
            "{}",
            null,
            running.StartedAt,
            now.AddSeconds(1));
        Assert.True((await store.TryTransitionRunAsync(transition, ct)).Transitioned);

        using var client = app.GetTestClient();
        var response = await client.PostAsJsonAsync("/surefire/api/runs/lookup",
            new { ids = new[] { succeeded.Id, "", running.Id, running.Id, Guid.CreateVersion7().ToString("N") } },
            ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var runs = await response.Content.ReadFromJsonAsync<List<RunResponse>>(ct);
        Assert.NotNull(runs);
        Assert.Equal([succeeded.Id, running.Id], runs.Select(r => r.Id));
        Assert.All(runs, r => Assert.Equal(JobStatus.Succeeded, r.Status));
        Assert.All(runs, r => Assert.NotNull(r.CompletedAt));
    }

    [Fact]
    public async Task RunsLookupEndpoint_DurableUnclaimedRun_ReportsAttemptZero()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        const string jobName = "tests-durable-attempt-zero";
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default", IsDurable = true }], ct);

        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Pending,
            CreatedAt = now,
            NotBefore = now,
            Attempt = 1,
            FailureCount = 0,
            ReplayCount = 0,
            IsDurable = true
        };

        await store.CreateRunsAsync([run], cancellationToken: ct);

        using var client = app.GetTestClient();
        var response = await client.PostAsJsonAsync("/surefire/api/runs/lookup",
            new { ids = new[] { run.Id } },
            ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var runs = await response.Content.ReadFromJsonAsync<List<RunResponse>>(ct);
        var dto = Assert.Single(runs!);
        Assert.Equal(1, dto.Attempt);
        Assert.Equal(0, dto.FailureCount);
        Assert.Equal(0, dto.ReplayCount);
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
    public async Task JobDetailEndpoint_ReturnsCapturedSourceCode()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(a => a.AddJob("tests-source", () => "ok"), ct);

        using var client = app.GetTestClient();
        var job = await client.GetFromJsonAsync<JobResponse>("/surefire/api/jobs/tests-source", ct);

        Assert.NotNull(job);
        Assert.NotNull(job.SourceCode);
        Assert.Contains("AddJob(\"tests-source\"", job.SourceCode);
        Assert.Contains("() => \"ok\"", job.SourceCode);
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
        await store.UpsertJobsAsync([
            new()
            {
                Name = "tests-invalid-timezone",
                Queue = "default",
                CronExpression = "0 9 * * *",
                TimeZoneId = "Invalid/Zone"
            }
        ], ct);

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
    public async Task PatchQueueEndpoint_MissingQueue_ReturnsNotFound()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);
        using var client = app.GetTestClient();

        var response = await SendPatchAsync(client, "/surefire/api/queues/tests-missing-queue", new { isPaused = true },
            ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task PatchQueueEndpoint_DefaultQueue_Pauses_WhenAJobUsesIt()
    {
        // The default queue is upserted by initialization based on registered jobs, not by
        // migration. An app that registers a job with no explicit queue gets a "default" row
        // it can pause from the dashboard.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(a => a.AddJob("tests-default-queue", () => "ok"), ct);
        using var client = app.GetTestClient();

        var response = await SendPatchAsync(client, "/surefire/api/queues/default", new { isPaused = true }, ct);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task PatchQueueEndpoint_DefaultQueue_ReturnsNotFound_WhenNoJobUsesIt()
    {
        // Symmetry: when no job uses the default queue, no row exists, so pausing it is a 404.
        // This is the behavior that lets retention sweep an unused default and keeps it off
        // the dashboard for apps that define their own queues.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);
        using var client = app.GetTestClient();

        var response = await SendPatchAsync(client, "/surefire/api/queues/default", new { isPaused = true }, ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task RunLogsEndpoint_WithOnlyMalformedEntriesInWindow_StillAdvancesCursor()
    {
        // Cursor must advance across rows that can't be parsed; otherwise the client sees
        // hasMore=true with a null cursor and polls the same window forever.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);
        var store = app.Services.GetRequiredService<IJobStore>();

        var run = await SeedRunAsync(store, ct);
        await store.AppendEventsAsync(
        [
            MakeLogEvent(run.Id, "not-json"),
            MakeLogEvent(run.Id, "also-not-json")
        ], ct);

        using var client = app.GetTestClient();
        var page = await client.GetFromJsonAsync<JsonElement>(
            $"/surefire/api/runs/{run.Id}/logs?take=1", ct);

        Assert.True(page.GetProperty("nextCursor").GetInt64() > 0);
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
        await store.UpsertJobsAsync([
            new()
            {
                Name = "tests-trigger-schema",
                Queue = "default",
                ArgumentsSchema =
                    "{\"type\":\"object\",\"properties\":{\"count\":{\"type\":\"integer\"}},\"required\":[\"count\"]}"
            }
        ], ct);

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

        await store.UpsertJobsAsync([
            new()
            {
                Name = "tests-job-stats-percent",
                Queue = "default"
            }
        ], ct);

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

        await store.UpsertJobsAsync([
            new()
            {
                Name = "tests-stats-success-rate-ratio",
                Queue = "default"
            }
        ], ct);

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
    public async Task StatsEndpoint_SerializesSuspendedTimelineBuckets()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        const string jobName = "tests-stats-suspended-timeline";
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default", IsDurable = true }], ct);
        await store.CreateRunsAsync([
            new()
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = jobName,
                Status = JobStatus.Suspended,
                CreatedAt = now,
                NotBefore = now,
                Attempt = 1,
                IsDurable = true
            }
        ], cancellationToken: ct);

        using var client = app.GetTestClient();
        var since = Uri.EscapeDataString(now.AddMinutes(-5).ToString("O"));
        var json = await client.GetStringAsync($"/surefire/api/stats?since={since}&bucketMinutes=60", ct);
        using var doc = JsonDocument.Parse(json);
        var timeline = doc.RootElement.GetProperty("timeline").EnumerateArray().ToArray();

        Assert.Contains(timeline, bucket =>
            bucket.TryGetProperty("suspended", out var suspended) && suspended.GetInt32() == 1);
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
    public async Task RunTreeEndpoint_ReturnsEntireHierarchy_RegardlessOfFocus()
    {
        // The tree endpoint must return the same payload for any focus run in the hierarchy:
        // root, mid-tier, or leaf. Aunts, uncles, cousins, grandchildren: anything reachable
        // through the shared root must appear. This is the key invariant that distinguishes
        // the tree query from the old focus-windowed trace.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;

        //   root
        //   +-- child-a
        //   |    +-- grandchild-a (leaf)
        //   +-- child-b           (grandchild-a's "uncle")
        //        +-- grandchild-b
        var rootId = Guid.CreateVersion7().ToString("N");
        var childAId = Guid.CreateVersion7().ToString("N");
        var childBId = Guid.CreateVersion7().ToString("N");
        var grandAId = Guid.CreateVersion7().ToString("N");
        var grandBId = Guid.CreateVersion7().ToString("N");

        JobRun MakeRun(string id, string? parentId, string? rootRunId, int offsetMs) => new()
        {
            Id = id,
            JobName = "tests-tree",
            Status = JobStatus.Succeeded,
            ParentRunId = parentId,
            RootRunId = rootRunId,
            CreatedAt = now.AddMilliseconds(offsetMs),
            NotBefore = now.AddMilliseconds(offsetMs),
            CompletedAt = now.AddMilliseconds(offsetMs),
            Attempt = 1,
            Progress = 1
        };

        await store.CreateRunsAsync([
            MakeRun(rootId, null, null, 0),
            MakeRun(childAId, rootId, rootId, 1),
            MakeRun(childBId, rootId, rootId, 2),
            MakeRun(grandAId, childAId, rootId, 3),
            MakeRun(grandBId, childBId, rootId, 4)
        ], cancellationToken: ct);

        using var client = app.GetTestClient();

        async Task AssertWholeTreeFrom(string focusId)
        {
            var tree = await client.GetFromJsonAsync<RunTreeResponse>(
                $"/surefire/api/runs/{focusId}/tree", ct);

            Assert.NotNull(tree);
            Assert.Equal(rootId, tree.RootId);
            Assert.False(tree.Truncated);
            Assert.Equal(5, tree.TotalCount);

            var ids = tree.Runs.Select(r => r.Id).ToHashSet();
            Assert.Equal(new() { rootId, childAId, childBId, grandAId, grandBId }, ids);

            var depthById = tree.Runs.ToDictionary(r => r.Id, r => r.Depth);
            Assert.Equal(0, depthById[rootId]);
            Assert.Equal(1, depthById[childAId]);
            Assert.Equal(1, depthById[childBId]);
            Assert.Equal(2, depthById[grandAId]);
            Assert.Equal(2, depthById[grandBId]);
        }

        // Same payload from any focus.
        await AssertWholeTreeFrom(rootId);
        await AssertWholeTreeFrom(childAId);
        await AssertWholeTreeFrom(grandAId);
        await AssertWholeTreeFrom(grandBId);
    }

    [Fact]
    public async Task RunTreeEndpoint_SortsRunsByCreatedAtThenId()
    {
        // Server returns a flat list sorted by (CreatedAt, Id) ASC. The client uses this order
        // to bucket children chronologically without re-sorting.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        var rootId = Guid.CreateVersion7().ToString("N");

        var children = Enumerable.Range(0, 5).Select(i =>
        {
            var id = Guid.CreateVersion7().ToString("N");
            var createdAt = now.AddMilliseconds(i);
            return new JobRun
            {
                Id = id,
                JobName = "tests-tree-order",
                Status = JobStatus.Succeeded,
                ParentRunId = rootId,
                RootRunId = rootId,
                CreatedAt = createdAt,
                NotBefore = createdAt,
                CompletedAt = createdAt,
                Attempt = 1,
                Progress = 1
            };
        }).ToList();

        var root = new JobRun
        {
            Id = rootId,
            JobName = "tests-tree-order",
            Status = JobStatus.Succeeded,
            CreatedAt = now.AddSeconds(-1),
            NotBefore = now.AddSeconds(-1),
            CompletedAt = now.AddSeconds(-1),
            Attempt = 1,
            Progress = 1
        };

        await store.CreateRunsAsync([root, ..children], cancellationToken: ct);

        using var client = app.GetTestClient();
        var tree = await client.GetFromJsonAsync<RunTreeResponse>(
            $"/surefire/api/runs/{rootId}/tree", ct);

        Assert.NotNull(tree);
        Assert.Equal(6, tree.Runs.Count);
        for (var i = 1; i < tree.Runs.Count; i++)
        {
            var prev = tree.Runs[i - 1];
            var curr = tree.Runs[i];
            Assert.True(
                prev.CreatedAt < curr.CreatedAt ||
                (prev.CreatedAt == curr.CreatedAt &&
                 string.CompareOrdinal(prev.Id, curr.Id) <= 0),
                $"Runs not sorted at index {i}");
        }
    }

    [Fact]
    public async Task RunTreeEndpoint_ForRootRun_ReturnsItself()
    {
        // A root has RootRunId == null and is identified by its own id; the endpoint must
        // return it as the single run when it has no descendants.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        var rootId = Guid.CreateVersion7().ToString("N");
        var root = new JobRun
        {
            Id = rootId,
            JobName = "tests-tree-solo",
            Status = JobStatus.Succeeded,
            CreatedAt = now,
            NotBefore = now,
            CompletedAt = now,
            Attempt = 1,
            Progress = 1
        };
        await store.CreateRunsAsync([root], cancellationToken: ct);

        using var client = app.GetTestClient();
        var tree = await client.GetFromJsonAsync<RunTreeResponse>(
            $"/surefire/api/runs/{rootId}/tree", ct);

        Assert.NotNull(tree);
        Assert.Equal(rootId, tree.RootId);
        Assert.Single(tree.Runs);
        Assert.Equal(rootId, tree.Runs[0].Id);
        Assert.Equal(0, tree.Runs[0].Depth);
        Assert.Equal(1, tree.TotalCount);
        Assert.False(tree.Truncated);
    }

    [Fact]
    public async Task RunTreeEndpoint_MissingRun_ReturnsNotFound()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        using var client = app.GetTestClient();
        var response = await client.GetAsync(
            $"/surefire/api/runs/{Guid.CreateVersion7():N}/tree", ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task BuildRunTreeAsync_Truncated_PreservesFocusLineageAndDropsOrphans()
    {
        // With a cap smaller than the tree, BuildRunTreeAsync must:
        //   - always include the focus and its full ancestor chain (so depth resolves),
        //   - include reachable descendants of those rows up to the cap,
        //   - drop any rows whose parent isn't visible (so nothing renders at wrong depth).
        // Ascending order in the descendants query is what makes truncation surface root-down,
        // so the BFS can reach as much of the subtree as the budget allows.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;

        // root
        // +-- midA
        // |    +-- leafA1
        // |    +-- leafA2
        // +-- midB
        //      +-- leafB1
        //      +-- leafB2
        var rootId = Guid.CreateVersion7().ToString("N");
        var midAId = Guid.CreateVersion7().ToString("N");
        var midBId = Guid.CreateVersion7().ToString("N");
        var leafA1Id = Guid.CreateVersion7().ToString("N");
        var leafA2Id = Guid.CreateVersion7().ToString("N");
        var leafB1Id = Guid.CreateVersion7().ToString("N");
        var leafB2Id = Guid.CreateVersion7().ToString("N");

        JobRun MakeRun(string id, string? parentId, string? rootRunId, int offsetMs) => new()
        {
            Id = id,
            JobName = "tests-tree-trunc",
            Status = JobStatus.Succeeded,
            ParentRunId = parentId,
            RootRunId = rootRunId,
            CreatedAt = now.AddMilliseconds(offsetMs),
            NotBefore = now.AddMilliseconds(offsetMs),
            CompletedAt = now.AddMilliseconds(offsetMs),
            Attempt = 1,
            Progress = 1
        };

        await store.CreateRunsAsync([
            MakeRun(rootId, null, null, 0),
            MakeRun(midAId, rootId, rootId, 1),
            MakeRun(midBId, rootId, rootId, 2),
            MakeRun(leafA1Id, midAId, rootId, 3),
            MakeRun(leafA2Id, midAId, rootId, 4),
            MakeRun(leafB1Id, midBId, rootId, 5),
            MakeRun(leafB2Id, midBId, rootId, 6)
        ], cancellationToken: ct);

        var focus = (await store.GetRunAsync(leafA1Id, ct))!;

        // Cap = 3: focus lineage is root, midA, leafA1 (exactly fits). With ASC ordering the
        // descendants page contains [midA, midB] (oldest first), so midA is reachable from
        // root and gets added. midB never reaches the result because we've already included
        // the lineage. leafA1 is in lineage; leafA2/leafB* drop off the cap.
        var tree3 = await DashboardEndpoints.BuildRunTreeAsync(store, focus, 3, ct);

        Assert.Equal(rootId, tree3.RootId);
        Assert.True(tree3.Truncated);
        Assert.Equal(7, tree3.TotalCount);

        var ids3 = tree3.Runs.Select(r => r.Id).ToHashSet();
        Assert.Contains(rootId, ids3);
        Assert.Contains(midAId, ids3);
        Assert.Contains(leafA1Id, ids3);

        // Every returned run has its parent in the result (no orphans rendering at depth 0).
        foreach (var r in tree3.Runs)
        {
            if (r.ParentRunId is null)
            {
                continue;
            }

            Assert.Contains(r.ParentRunId, ids3);
        }

        var depthById3 = tree3.Runs.ToDictionary(r => r.Id, r => r.Depth);
        Assert.Equal(0, depthById3[rootId]);
        Assert.Equal(1, depthById3[midAId]);
        Assert.Equal(2, depthById3[leafA1Id]);

        // Cap = 5: budget grows; ASC order brings in midA/midB before the leaves, then the
        // BFS reaches the focus subtree's siblings. Lineage still guaranteed.
        var tree5 = await DashboardEndpoints.BuildRunTreeAsync(store, focus, 5, ct);

        Assert.True(tree5.Truncated);
        Assert.Equal(7, tree5.TotalCount);
        var ids5 = tree5.Runs.Select(r => r.Id).ToHashSet();
        Assert.Contains(rootId, ids5);
        Assert.Contains(midAId, ids5);
        Assert.Contains(leafA1Id, ids5);
        // No orphans at higher cap either.
        foreach (var r in tree5.Runs)
        {
            if (r.ParentRunId is null)
            {
                continue;
            }

            Assert.Contains(r.ParentRunId, ids5);
        }

        // Cap = 100: room for everything; no truncation.
        var treeFull = await DashboardEndpoints.BuildRunTreeAsync(store, focus, 100, ct);
        Assert.False(treeFull.Truncated);
        Assert.Equal(7, treeFull.TotalCount);
        Assert.Equal(7, treeFull.Runs.Count);
        var depthByIdFull = treeFull.Runs.ToDictionary(r => r.Id, r => r.Depth);
        Assert.Equal(0, depthByIdFull[rootId]);
        Assert.Equal(1, depthByIdFull[midAId]);
        Assert.Equal(1, depthByIdFull[midBId]);
        Assert.Equal(2, depthByIdFull[leafA1Id]);
        Assert.Equal(2, depthByIdFull[leafA2Id]);
        Assert.Equal(2, depthByIdFull[leafB1Id]);
        Assert.Equal(2, depthByIdFull[leafB2Id]);
    }

    [Fact]
    public async Task BuildRunTreeAsync_Truncated_FocusAtRoot_ShowsImmediateChildren()
    {
        // When the focus is the root itself, the lineage is just the root. ASC ordering of
        // the descendants page must surface direct children first so the BFS can find them.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        var rootId = Guid.CreateVersion7().ToString("N");

        // Root + 3 direct children + 3 grandchildren (children of child A).
        var childAId = Guid.CreateVersion7().ToString("N");
        var childBId = Guid.CreateVersion7().ToString("N");
        var childCId = Guid.CreateVersion7().ToString("N");
        var grand1Id = Guid.CreateVersion7().ToString("N");
        var grand2Id = Guid.CreateVersion7().ToString("N");
        var grand3Id = Guid.CreateVersion7().ToString("N");

        JobRun MakeRun(string id, string? parentId, string? rootRunId, int offsetMs) => new()
        {
            Id = id,
            JobName = "tests-tree-root-focus",
            Status = JobStatus.Succeeded,
            ParentRunId = parentId,
            RootRunId = rootRunId,
            CreatedAt = now.AddMilliseconds(offsetMs),
            NotBefore = now.AddMilliseconds(offsetMs),
            CompletedAt = now.AddMilliseconds(offsetMs),
            Attempt = 1,
            Progress = 1
        };

        await store.CreateRunsAsync([
            MakeRun(rootId, null, null, 0),
            MakeRun(childAId, rootId, rootId, 1),
            MakeRun(childBId, rootId, rootId, 2),
            MakeRun(childCId, rootId, rootId, 3),
            MakeRun(grand1Id, childAId, rootId, 4),
            MakeRun(grand2Id, childAId, rootId, 5),
            MakeRun(grand3Id, childAId, rootId, 6)
        ], cancellationToken: ct);

        var root = (await store.GetRunAsync(rootId, ct))!;

        // Cap = 2: lineage is just [root], so one slot left. BFS adds child A (oldest).
        var tree = await DashboardEndpoints.BuildRunTreeAsync(store, root, 2, ct);

        Assert.True(tree.Truncated);
        Assert.Equal(7, tree.TotalCount);
        var ids = tree.Runs.Select(r => r.Id).ToHashSet();
        Assert.Contains(rootId, ids);
        Assert.Contains(childAId, ids);
        // No orphans.
        foreach (var r in tree.Runs)
        {
            if (r.ParentRunId is null)
            {
                continue;
            }

            Assert.Contains(r.ParentRunId, ids);
        }
    }

    [Fact]
    public async Task RunTreeEndpoint_HonorsConfiguredMaxTreeRuns()
    {
        // SurefireDashboardOptions.MaxTreeRuns is the operator-tunable cap. Configure it small
        // and confirm the endpoint truncates accordingly. This is the only way to exercise
        // truncation in a test without seeding 50k+ rows.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(
            ct: ct,
            configureDashboard: o => o.MaxTreeRuns = 2);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        var rootId = Guid.CreateVersion7().ToString("N");
        var children = Enumerable.Range(0, 5).Select(i => new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "tests-tree-cap",
            Status = JobStatus.Succeeded,
            ParentRunId = rootId,
            RootRunId = rootId,
            CreatedAt = now.AddMilliseconds(i),
            NotBefore = now.AddMilliseconds(i),
            CompletedAt = now.AddMilliseconds(i),
            Attempt = 1,
            Progress = 1
        }).ToList();
        var root = new JobRun
        {
            Id = rootId,
            JobName = "tests-tree-cap",
            Status = JobStatus.Succeeded,
            CreatedAt = now.AddSeconds(-1),
            NotBefore = now.AddSeconds(-1),
            CompletedAt = now.AddSeconds(-1),
            Attempt = 1,
            Progress = 1
        };
        await store.CreateRunsAsync([root, ..children], cancellationToken: ct);

        using var client = app.GetTestClient();
        var tree = await client.GetFromJsonAsync<RunTreeResponse>(
            $"/surefire/api/runs/{rootId}/tree", ct);

        Assert.NotNull(tree);
        Assert.True(tree.Truncated);
        Assert.Equal(6, tree.TotalCount);
        // Truncated payload: at most one extra "headroom" beyond the cap for the lineage row
        // (which the contract pins in regardless of cap). Either way it's strictly less than 6.
        Assert.InRange(tree.Runs.Count, 1, 3);
    }

    [Fact]
    public async Task BuildRunTreeAsync_Truncated_BatchRoot_ShowsBatchSiblings()
    {
        // Top-level batch case: focus.ParentRunId == null, but RootRunId points at a batch
        // record (no run with that id). BFS should still surface batch siblings via the
        // parent-group seeding even though there's no shared root row.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAppAsync(ct: ct);

        var store = app.Services.GetRequiredService<IJobStore>();
        var now = DateTimeOffset.UtcNow;
        var batchId = Guid.CreateVersion7().ToString("N");

        var siblings = Enumerable.Range(0, 5).Select(i => new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "tests-tree-batch",
            Status = JobStatus.Succeeded,
            ParentRunId = null,
            RootRunId = batchId,
            BatchId = batchId,
            CreatedAt = now.AddMilliseconds(i),
            NotBefore = now.AddMilliseconds(i),
            CompletedAt = now.AddMilliseconds(i),
            Attempt = 1,
            Progress = 1
        }).ToList();

        await store.CreateRunsAsync(siblings, cancellationToken: ct);

        var focus = siblings[2];
        var tree = await DashboardEndpoints.BuildRunTreeAsync(store, focus, 3, ct);

        Assert.Equal(batchId, tree.RootId);
        Assert.True(tree.Truncated);
        Assert.Equal(5, tree.TotalCount);
        Assert.Contains(focus.Id, tree.Runs.Select(r => r.Id));
        Assert.All(tree.Runs, r => Assert.Equal(0, r.Depth));
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
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

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
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

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
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

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
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

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
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

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
            run.LeaseEpoch,
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
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

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
            run.LeaseEpoch,
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
        CancellationToken ct = default,
        Action<SurefireDashboardOptions>? configureDashboard = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();

        builder.Services.AddSurefire(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(10);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
        });
        builder.Services.AddSurefireDashboard(o =>
        {
            o.AuthMode = DashboardAuthMode.Unsecured;
            configureDashboard?.Invoke(o);
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

    private static async Task<JobRun> SeedRunAsync(IJobStore store, CancellationToken ct)
    {
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "tests-seed",
            Status = JobStatus.Succeeded,
            CreatedAt = DateTimeOffset.UtcNow,
            NotBefore = DateTimeOffset.UtcNow,
            Attempt = 1
        };
        await store.CreateRunsAsync([run], null, ct);
        return run;
    }

    private static RunEvent MakeLogEvent(string runId, string payload) => new()
    {
        RunId = runId,
        EventType = RunEventType.Log,
        Payload = payload,
        CreatedAt = DateTimeOffset.UtcNow,
        Attempt = 1
    };

    private static RunEvent CreateTerminalStatusEvent(JobRun run, DateTimeOffset createdAt) => new()
    {
        RunId = run.Id,
        EventType = RunEventType.Status,
        Payload = ((int)run.Status).ToString(),
        CreatedAt = createdAt,
        Attempt = run.Attempt
    };
}
