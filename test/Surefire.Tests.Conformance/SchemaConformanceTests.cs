namespace Surefire.Tests.Conformance;

public abstract class SchemaConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task MigrateAsync_CalledTwice_IsIdempotent()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"MigrateJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        await Store.MigrateAsync(ct);

        var loaded = await Store.GetJobAsync(jobName, ct);
        Assert.NotNull(loaded);
        Assert.Equal(jobName, loaded.Name);

        var loadedRun = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loadedRun);
    }

    [Fact]
    public async Task UpsertJobsAsync_RoundTripsSourceCode()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"SourceJob_{Guid.CreateVersion7():N}";
        const string sourceCode = "static string Handler() => \"ok\";";
        var job = CreateJob(jobName);
        job.SourceCode = sourceCode;

        await Store.UpsertJobsAsync([job], ct);

        var loaded = await Store.GetJobAsync(jobName, ct);
        Assert.NotNull(loaded);
        Assert.Equal(sourceCode, loaded.SourceCode);
    }
}
