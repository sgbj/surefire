using Surefire.Tests.Conformance;

namespace Surefire.Tests.InMemory;

[CollectionDefinition("InMemory")]
public class InMemoryCollection : ICollectionFixture<InMemoryFixture>;

[Collection("InMemory")]
public sealed class InMemoryJobTests(InMemoryFixture fixture)
    : FixtureBackedJobConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryRunCrudTests(InMemoryFixture fixture)
    : FixtureBackedRunCrudConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryTransitionTests(InMemoryFixture fixture)
    : FixtureBackedTransitionConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryCancelTests(InMemoryFixture fixture)
    : FixtureBackedCancelConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryClaimTests(InMemoryFixture fixture)
    : FixtureBackedClaimConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryBatchTests(InMemoryFixture fixture)
    : FixtureBackedBatchConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryEventTests(InMemoryFixture fixture)
    : FixtureBackedEventConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryNodeTests(InMemoryFixture fixture)
    : FixtureBackedNodeConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryQueueTests(InMemoryFixture fixture)
    : FixtureBackedQueueConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryRateLimitTests(InMemoryFixture fixture)
    : FixtureBackedRateLimitConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryMaintenanceTests(InMemoryFixture fixture)
    : FixtureBackedMaintenanceConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryPurgeTests(InMemoryFixture fixture)
    : FixtureBackedPurgeConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryStatsTests(InMemoryFixture fixture)
    : FixtureBackedStatsConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemorySchemaTests(InMemoryFixture fixture)
    : FixtureBackedSchemaConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryStoreFixTests(InMemoryFixture fixture)
    : FixtureBackedStoreFixConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryQueueStatsParityTests(InMemoryFixture fixture)
    : FixtureBackedQueueStatsParityConformanceTests<InMemoryFixture>(fixture);

[Collection("InMemory")]
public sealed class InMemoryRuntimeReliabilityTests(InMemoryFixture fixture)
    : FixtureBackedRuntimeReliabilityConformanceTests<InMemoryFixture>(fixture);