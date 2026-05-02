namespace Surefire.Tests.Conformance;

internal interface IStoreTestFixture
{
    Task<IJobStore> CreateStoreAsync();
    Task CleanAsync();
}

public abstract class FixtureBackedJobConformanceTests<TFixture>(TFixture fixture) : JobConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedRunCrudConformanceTests<TFixture>(TFixture fixture) : RunCrudConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedTransitionConformanceTests<TFixture>(TFixture fixture)
    : TransitionConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedCancelConformanceTests<TFixture>(TFixture fixture) : CancelConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedClaimConformanceTests<TFixture>(TFixture fixture) : ClaimConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedBatchConformanceTests<TFixture>(TFixture fixture) : BatchConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedEventConformanceTests<TFixture>(TFixture fixture) : EventConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedNodeConformanceTests<TFixture>(TFixture fixture) : NodeConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedQueueConformanceTests<TFixture>(TFixture fixture) : QueueConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedRateLimitConformanceTests<TFixture>(TFixture fixture)
    : RateLimitConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedMaintenanceConformanceTests<TFixture>(TFixture fixture)
    : MaintenanceConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedPurgeConformanceTests<TFixture>(TFixture fixture) : PurgeConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedStatsConformanceTests<TFixture>(TFixture fixture) : StatsConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedSchemaConformanceTests<TFixture>(TFixture fixture) : SchemaConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedQueueStatsParityConformanceTests<TFixture>(TFixture fixture)
    : QueueStatsParityConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedRuntimeReliabilityConformanceTests<TFixture>(TFixture fixture)
    : RuntimeReliabilityConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedTraceConformanceTests<TFixture>(TFixture fixture) : TraceConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}

public abstract class FixtureBackedContentionConformanceTests<TFixture>(TFixture fixture)
    : ContentionConformanceTests
{
    internal override Task<IJobStore> CreateStoreAsync() => ((IStoreTestFixture)fixture!).CreateStoreAsync();
    public override ValueTask DisposeAsync() => new(((IStoreTestFixture)fixture!).CleanAsync());
}
