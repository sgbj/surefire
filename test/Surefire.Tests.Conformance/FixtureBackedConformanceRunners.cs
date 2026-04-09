namespace Surefire.Tests.Conformance;

internal interface IStoreTestFixture
{
    Task<IJobStore> CreateStoreAsync();
    Task CleanAsync();
}

internal abstract class FixtureBackedJobConformanceTests<TFixture>(TFixture fixture) : JobConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedRunCrudConformanceTests<TFixture>(TFixture fixture) : RunCrudConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedTransitionConformanceTests<TFixture>(TFixture fixture)
    : TransitionConformanceTests where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedCancelConformanceTests<TFixture>(TFixture fixture) : CancelConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedClaimConformanceTests<TFixture>(TFixture fixture) : ClaimConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedBatchConformanceTests<TFixture>(TFixture fixture) : BatchConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedEventConformanceTests<TFixture>(TFixture fixture) : EventConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedNodeConformanceTests<TFixture>(TFixture fixture) : NodeConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedQueueConformanceTests<TFixture>(TFixture fixture) : QueueConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedRateLimitConformanceTests<TFixture>(TFixture fixture)
    : RateLimitConformanceTests where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedMaintenanceConformanceTests<TFixture>(TFixture fixture)
    : MaintenanceConformanceTests where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedPurgeConformanceTests<TFixture>(TFixture fixture) : PurgeConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedStatsConformanceTests<TFixture>(TFixture fixture) : StatsConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedSchemaConformanceTests<TFixture>(TFixture fixture) : SchemaConformanceTests
    where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedStoreFixConformanceTests<TFixture>(TFixture fixture)
    : StoreFixConformanceTests where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedQueueStatsParityConformanceTests<TFixture>(TFixture fixture)
    : QueueStatsParityConformanceTests where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}

internal abstract class FixtureBackedRuntimeReliabilityConformanceTests<TFixture>(TFixture fixture)
    : RuntimeReliabilityConformanceTests where TFixture : IStoreTestFixture
{
    internal override Task<IJobStore> CreateStoreAsync() => fixture.CreateStoreAsync();
    public override Task DisposeAsync() => fixture.CleanAsync();
}
