namespace Surefire.Tests.Conformance;

public abstract class SchemaConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task MigrateAsync_CalledTwice_IsIdempotent()
    {
        var jobName = $"MigrateJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));
        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        await Store.MigrateAsync();

        var loaded = await Store.GetJobAsync(jobName);
        Assert.NotNull(loaded);
        Assert.Equal(jobName, loaded.Name);

        var loadedRun = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loadedRun);
    }
}