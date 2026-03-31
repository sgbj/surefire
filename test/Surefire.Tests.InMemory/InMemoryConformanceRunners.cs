using Surefire.Tests.Conformance;

namespace Surefire.Tests.InMemory;

public sealed class InMemoryJobTests(InMemoryFixture fixture)
    : FixtureBackedJobConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryRunCrudTests(InMemoryFixture fixture)
    : FixtureBackedRunCrudConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryTransitionTests(InMemoryFixture fixture)
    : FixtureBackedTransitionConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryCancelTests(InMemoryFixture fixture)
    : FixtureBackedCancelConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryClaimTests(InMemoryFixture fixture)
    : FixtureBackedClaimConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryBatchTests(InMemoryFixture fixture)
    : FixtureBackedBatchConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryEventTests(InMemoryFixture fixture)
    : FixtureBackedEventConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryNodeTests(InMemoryFixture fixture)
    : FixtureBackedNodeConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryQueueTests(InMemoryFixture fixture)
    : FixtureBackedQueueConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryRateLimitTests(InMemoryFixture fixture)
    : FixtureBackedRateLimitConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryMaintenanceTests(InMemoryFixture fixture)
    : FixtureBackedMaintenanceConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryPurgeTests(InMemoryFixture fixture)
    : FixtureBackedPurgeConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryStatsTests(InMemoryFixture fixture)
    : FixtureBackedStatsConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemorySchemaTests(InMemoryFixture fixture)
    : FixtureBackedSchemaConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryStoreFixTests(InMemoryFixture fixture)
    : FixtureBackedStoreFixConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryQueueStatsParityTests(InMemoryFixture fixture)
    : FixtureBackedQueueStatsParityConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

public sealed class InMemoryRuntimeReliabilityTests(InMemoryFixture fixture)
    : FixtureBackedRuntimeReliabilityConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;