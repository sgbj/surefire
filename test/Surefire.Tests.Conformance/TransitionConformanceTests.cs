using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

public abstract class TransitionConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task TryTransitionRun_Success_WhenExpectedMatches()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        run = run with
        {
            Status = JobStatus.Running, NodeName = "node-1", StartedAt = DateTimeOffset.UtcNow,
            LastHeartbeatAt = DateTimeOffset.UtcNow
        };
        var result = await Store.TryTransitionRunAsync(Transition(run, JobStatus.Pending), ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Running, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Fails_WhenExpectedMismatches()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Running };
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Pending), ct);

        Assert.False(result.Transitioned);
    }

    [Fact]
    public async Task TryTransitionRun_Running_To_Completed()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Succeeded, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Running_To_Pending_Retry()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Pending, NotBefore = DateTimeOffset.UtcNow };
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Pending, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_TerminalToAnything_Fails()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        var ok = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);
        Assert.True(ok.Transitioned);

        claimed = claimed with { Status = JobStatus.Running };
        var result = await Store.TryTransitionRunAsync(InvalidTransition(claimed, JobStatus.Succeeded), ct);

        Assert.False(result.Transitioned);
    }

    [Fact]
    public async Task TryTransitionRun_AttemptFencing()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        Assert.Equal(1, claimed.Attempt);

        // Create a stale copy with wrong attempt
        var stale = new JobRun
        {
            Id = claimed.Id,
            JobName = claimed.JobName,
            Status = JobStatus.Succeeded,
            Attempt = 0, // stale attempt
            CreatedAt = claimed.CreatedAt,
            NotBefore = claimed.NotBefore,
            CompletedAt = DateTimeOffset.UtcNow
        };

        var result = await Store.TryTransitionRunAsync(Transition(stale, JobStatus.Running), ct);

        Assert.False(result.Transitioned);
    }

    [Fact]
    public async Task TryTransitionRun_PreservesTimestamps_WhenPayloadOmitsThem()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        Assert.NotNull(claimed.StartedAt);

        var expectedStartedAt = claimed.StartedAt;

        var update = new JobRun
        {
            Id = claimed.Id,
            JobName = claimed.JobName,
            Status = JobStatus.Pending,
            Attempt = claimed.Attempt,
            CreatedAt = claimed.CreatedAt,
            NotBefore = DateTimeOffset.UtcNow.AddSeconds(1),
            Progress = claimed.Progress,
            Reason = claimed.Reason,
            Result = claimed.Result,
            NodeName = claimed.NodeName
        };

        var ok = await Store.TryTransitionRunAsync(Transition(update, JobStatus.Running), ct);

        Assert.True(ok.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Pending, stored.Status);
        Assert.Equal(expectedStartedAt, stored.StartedAt);
    }

    [Fact]
    public async Task TryTransitionStatus_InvalidTransition_IsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        run = run with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };

        var ok = await Store.TryTransitionRunAsync(InvalidTransition(run, JobStatus.Pending), ct);

        Assert.False(ok.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Pending, stored.Status);
        Assert.Null(stored.CompletedAt);
    }

    [Fact]
    public async Task TryTransitionStatus_CompletedWithoutCompletedAt_IsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = null };

        var ok = await Store.TryTransitionRunAsync(InvalidTransition(claimed, JobStatus.Running), ct);

        Assert.False(ok.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Running, stored.Status);
    }

    [Fact]
    public async Task TryTransitionStatus_RunningWithoutHeartbeat_IsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        run = run with
        {
            Status = JobStatus.Running, Attempt = 0, StartedAt = DateTimeOffset.UtcNow, LastHeartbeatAt = null,
            NodeName = "node-1"
        };

        var ok = await Store.TryTransitionRunAsync(InvalidTransition(run, JobStatus.Pending), ct);

        Assert.False(ok.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Pending, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Concurrent_OnlyOneWins()
    {
        var ct = TestContext.Current.CancellationToken;
        for (var trial = 0; trial < 10; trial++)
        {
            var jobName = "ConcurrentCas_" + Guid.CreateVersion7().ToString("N");
            var job = CreateJob(jobName);
            await Store.UpsertJobsAsync([job], ct);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run], cancellationToken: ct);

            var claimed = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
            Assert.NotNull(claimed);

            var results = new ConcurrentBag<bool>();

            var tasks = Enumerable.Range(0, 10).Select(i => Task.Run(async () =>
            {
                await Task.Delay(1);

                var attempt = new JobRun
                {
                    Id = claimed.Id,
                    JobName = claimed.JobName,
                    Status = i < 5 ? JobStatus.Succeeded : JobStatus.Cancelled,
                    Attempt = claimed.Attempt,
                    CreatedAt = claimed.CreatedAt,
                    NotBefore = claimed.NotBefore,
                    CompletedAt = DateTimeOffset.UtcNow,
                    CancelledAt = i >= 5 ? DateTimeOffset.UtcNow : null
                };

                var ok = await Store.TryTransitionRunAsync(Transition(attempt, JobStatus.Running));
                results.Add(ok.Transitioned);
            }));

            await Task.WhenAll(tasks);

            Assert.Equal(1, results.Count(r => r));
        }
    }

    [Fact]
    public async Task TryTransitionRun_Running_To_Cancelled()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with
        {
            Status = JobStatus.Cancelled, CancelledAt = DateTimeOffset.UtcNow, CompletedAt = DateTimeOffset.UtcNow
        };
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Running_To_DeadLetter()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Failed, CompletedAt = DateTimeOffset.UtcNow };
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Failed, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Pending_To_Cancelled()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        run = run with
        {
            Status = JobStatus.Cancelled, CancelledAt = DateTimeOffset.UtcNow, CompletedAt = DateTimeOffset.UtcNow
        };
        var result = await Store.TryTransitionRunAsync(Transition(run, JobStatus.Pending), ct);

        Assert.True(result.Transitioned);
    }
}