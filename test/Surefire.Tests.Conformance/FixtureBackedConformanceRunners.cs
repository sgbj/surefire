namespace Surefire.Tests.Conformance;

public interface IStoreTestFixture
{
    Task<IJobStore> CreateStoreAsync();
    Task CleanAsync();
}

public abstract class FixtureBackedJobConformanceTests<TFixture>(TFixture fixture) : JobConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedRunCrudConformanceTests<TFixture>(TFixture fixture) : RunCrudConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedTransitionConformanceTests<TFixture>(TFixture fixture)
    : TransitionConformanceTests where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedCancelConformanceTests<TFixture>(TFixture fixture) : CancelConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedClaimConformanceTests<TFixture>(TFixture fixture) : ClaimConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedBatchConformanceTests<TFixture>(TFixture fixture) : BatchConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedEventConformanceTests<TFixture>(TFixture fixture) : EventConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedNodeConformanceTests<TFixture>(TFixture fixture) : NodeConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedQueueConformanceTests<TFixture>(TFixture fixture) : QueueConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedRateLimitConformanceTests<TFixture>(TFixture fixture)
    : RateLimitConformanceTests where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedMaintenanceConformanceTests<TFixture>(TFixture fixture)
    : MaintenanceConformanceTests where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedPurgeConformanceTests<TFixture>(TFixture fixture) : PurgeConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedStatsConformanceTests<TFixture>(TFixture fixture) : StatsConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedSchemaConformanceTests<TFixture>(TFixture fixture) : SchemaConformanceTests
    where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedStoreFixConformanceTests<TFixture>(TFixture fixture)
    : StoreFixConformanceTests where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedQueueStatsParityConformanceTests<TFixture>(TFixture fixture)
    : QueueStatsParityConformanceTests where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

public abstract class FixtureBackedRuntimeReliabilityConformanceTests<TFixture>(TFixture fixture)
    : RuntimeReliabilityConformanceTests where TFixture : IStoreTestFixture
{
    protected override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}