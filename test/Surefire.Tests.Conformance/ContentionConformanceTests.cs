using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

public abstract class ContentionConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task Concurrent_CreateClaimTransition_OnlyThrowsTransientExceptions()
    {
        var ct = TestContext.Current.CancellationToken;

        var jobA = $"ContentionA_{Guid.CreateVersion7():N}";
        var jobB = $"ContentionB_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([
            new() { Name = jobA, Queue = "default" },
            new() { Name = jobB, Queue = "default" }
        ], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var rootRunId = Guid.CreateVersion7().ToString("N");
        await Store.CreateRunsAsync([
            new()
            {
                Id = rootRunId,
                JobName = jobA,
                Status = JobStatus.Pending,
                Attempt = 0,
                CreatedAt = DateTimeOffset.UtcNow,
                NotBefore = DateTimeOffset.UtcNow.AddSeconds(-1),
                Priority = 0,
                RootRunId = rootRunId
            }
        ], cancellationToken: ct);

        var failures = new ConcurrentBag<Exception>();
        var completed = 0;
        using var stop = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stop.Token, ct);

        var creators = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            while (!linked.Token.IsCancellationRequested)
            {
                try
                {
                    var childId = Guid.CreateVersion7().ToString("N");
                    await Store.TryCreateRunAsync(new()
                    {
                        Id = childId,
                        JobName = jobB,
                        Status = JobStatus.Pending,
                        Attempt = 0,
                        CreatedAt = DateTimeOffset.UtcNow,
                        NotBefore = DateTimeOffset.UtcNow.AddSeconds(-1),
                        Priority = 0,
                        ParentRunId = rootRunId,
                        RootRunId = rootRunId
                    }, cancellationToken: linked.Token);
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) { failures.Add(ex); }
            }
        }, linked.Token)).ToArray();

        var claimers = Enumerable.Range(0, 4).Select(i => Task.Run(async () =>
        {
            var node = $"node-{i}";
            while (!linked.Token.IsCancellationRequested)
            {
                try
                {
                    var claimed = await Store.ClaimRunsAsync(node, [jobA, jobB], ["default"], 16, linked.Token);
                    foreach (var run in claimed)
                    {
                        var ok = await Store.TryTransitionRunAsync(new()
                        {
                            RunId = run.Id,
                            ExpectedStatus = JobStatus.Running,
                            ExpectedAttempt = run.Attempt,
                            NewStatus = JobStatus.Succeeded,
                            Progress = 1,
                            Result = "{}",
                            CompletedAt = DateTimeOffset.UtcNow,
                            NotBefore = run.NotBefore,
                            NodeName = node
                        }, linked.Token);
                        if (ok.Transitioned) Interlocked.Increment(ref completed);
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) { failures.Add(ex); }
            }
        }, linked.Token)).ToArray();

        await Task.Delay(TimeSpan.FromSeconds(5), ct);
        await stop.CancelAsync();
        await Task.WhenAll(creators.Concat(claimers));

        var nonTransient = failures.Where(ex => !Store.IsTransientException(ex)).ToList();
        Assert.True(nonTransient.Count == 0,
            "Non-transient exceptions surfaced under contention: " +
            string.Join("; ", nonTransient.Select(e => e.GetType().Name + ": " + e.Message)));
        Assert.True(completed > 0,
            "No runs completed under contention; store made no forward progress.");
    }
}
