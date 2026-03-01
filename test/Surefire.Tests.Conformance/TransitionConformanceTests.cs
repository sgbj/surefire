using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

public abstract class TransitionConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task TryTransitionRun_Success_WhenExpectedMatches()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        run.Status = JobStatus.Running;
        run.NodeName = "node-1";
        run.StartedAt = DateTimeOffset.UtcNow;
        run.LastHeartbeatAt = DateTimeOffset.UtcNow;
        var result = await Store.TryTransitionRunAsync(Transition(run, JobStatus.Pending));

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Running, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Fails_WhenExpectedMismatches()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Running;
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Pending));

        Assert.False(result);
    }

    [Fact]
    public async Task TryTransitionRun_Running_To_Completed()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Completed;
        claimed.CompletedAt = DateTimeOffset.UtcNow;
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Completed, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Running_To_Retrying()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Retrying;
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Retrying, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Retrying_To_Pending()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Retrying;
        var ok = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));
        Assert.True(ok);

        claimed.Status = JobStatus.Pending;
        claimed.NotBefore = DateTimeOffset.UtcNow;
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Retrying));

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Pending, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_TerminalToAnything_Fails()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Completed;
        claimed.CompletedAt = DateTimeOffset.UtcNow;
        var ok = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));
        Assert.True(ok);

        claimed.Status = JobStatus.Running;
        var result = await Store.TryTransitionRunAsync(InvalidTransition(claimed, JobStatus.Completed));

        Assert.False(result);
    }

    [Fact]
    public async Task TryTransitionRun_AttemptFencing()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);
        Assert.Equal(1, claimed.Attempt);

        // Create a stale copy with wrong attempt
        var stale = new JobRun
        {
            Id = claimed.Id,
            JobName = claimed.JobName,
            Status = JobStatus.Completed,
            Attempt = 0, // stale attempt
            CreatedAt = claimed.CreatedAt,
            NotBefore = claimed.NotBefore,
            CompletedAt = DateTimeOffset.UtcNow
        };

        var result = await Store.TryTransitionRunAsync(Transition(stale, JobStatus.Running));

        Assert.False(result);
    }

    [Fact]
    public async Task TryTransitionRun_PreservesTimestamps_WhenPayloadOmitsThem()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);
        Assert.NotNull(claimed.StartedAt);

        var expectedStartedAt = claimed.StartedAt;

        var update = new JobRun
        {
            Id = claimed.Id,
            JobName = claimed.JobName,
            Status = JobStatus.Retrying,
            Attempt = claimed.Attempt,
            CreatedAt = claimed.CreatedAt,
            NotBefore = DateTimeOffset.UtcNow.AddSeconds(1),
            Progress = claimed.Progress,
            Error = claimed.Error,
            Result = claimed.Result,
            NodeName = claimed.NodeName
        };

        var ok = await Store.TryTransitionRunAsync(Transition(update, JobStatus.Running));

        Assert.True(ok);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Retrying, stored.Status);
        Assert.Equal(expectedStartedAt, stored.StartedAt);
    }

    [Fact]
    public async Task TryTransitionStatus_InvalidTransition_IsRejected()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        run.Status = JobStatus.Completed;
        run.CompletedAt = DateTimeOffset.UtcNow;

        var ok = await Store.TryTransitionRunAsync(InvalidTransition(run, JobStatus.Pending));

        Assert.False(ok);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Pending, stored.Status);
        Assert.Null(stored.CompletedAt);
    }

    [Fact]
    public async Task TryTransitionStatus_CompletedWithoutCompletedAt_IsRejected()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Completed;
        claimed.CompletedAt = null;

        var ok = await Store.TryTransitionRunAsync(InvalidTransition(claimed, JobStatus.Running));

        Assert.False(ok);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Running, stored.Status);
    }

    [Fact]
    public async Task TryTransitionStatus_RunningWithoutHeartbeat_IsRejected()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        run.Status = JobStatus.Running;
        run.Attempt = 0;
        run.StartedAt = DateTimeOffset.UtcNow;
        run.LastHeartbeatAt = null;
        run.NodeName = "node-1";

        var ok = await Store.TryTransitionRunAsync(InvalidTransition(run, JobStatus.Pending));

        Assert.False(ok);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Pending, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Concurrent_OnlyOneWins()
    {
        for (var trial = 0; trial < 10; trial++)
        {
            var jobName = "ConcurrentCas_" + Guid.CreateVersion7().ToString("N");
            var job = CreateJob(jobName);
            await Store.UpsertJobAsync(job);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run]);

            var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
            Assert.NotNull(claimed);

            var results = new ConcurrentBag<bool>();

            var tasks = Enumerable.Range(0, 10).Select(i => Task.Run(async () =>
            {
                await Task.Delay(1);

                var attempt = new JobRun
                {
                    Id = claimed.Id,
                    JobName = claimed.JobName,
                    Status = i < 5 ? JobStatus.Completed : JobStatus.Cancelled,
                    Attempt = claimed.Attempt,
                    CreatedAt = claimed.CreatedAt,
                    NotBefore = claimed.NotBefore,
                    CompletedAt = DateTimeOffset.UtcNow,
                    CancelledAt = i >= 5 ? DateTimeOffset.UtcNow : null
                };

                var ok = await Store.TryTransitionRunAsync(Transition(attempt, JobStatus.Running));
                results.Add(ok);
            }));

            await Task.WhenAll(tasks);

            Assert.Equal(1, results.Count(r => r));
        }
    }

    [Fact]
    public async Task TryTransitionRun_Running_To_Cancelled()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Cancelled;
        claimed.CancelledAt = DateTimeOffset.UtcNow;
        claimed.CompletedAt = DateTimeOffset.UtcNow;
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Running_To_DeadLetter()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.DeadLetter;
        claimed.CompletedAt = DateTimeOffset.UtcNow;
        var result = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.DeadLetter, stored.Status);
    }

    [Fact]
    public async Task TryTransitionRun_Pending_To_Cancelled()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        run.Status = JobStatus.Cancelled;
        run.CancelledAt = DateTimeOffset.UtcNow;
        run.CompletedAt = DateTimeOffset.UtcNow;
        var result = await Store.TryTransitionRunAsync(Transition(run, JobStatus.Pending));

        Assert.True(result);
    }
}