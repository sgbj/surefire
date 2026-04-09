using Surefire.Tests.Conformance;

namespace Surefire.Tests.InMemory;

internal sealed class InMemoryJobTests(InMemoryFixture fixture)
    : FixtureBackedJobConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryRunCrudTests(InMemoryFixture fixture)
    : FixtureBackedRunCrudConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryTransitionTests(InMemoryFixture fixture)
    : FixtureBackedTransitionConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryCancelTests(InMemoryFixture fixture)
    : FixtureBackedCancelConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryClaimTests(InMemoryFixture fixture)
    : FixtureBackedClaimConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryBatchTests(InMemoryFixture fixture)
    : FixtureBackedBatchConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryEventTests(InMemoryFixture fixture)
    : FixtureBackedEventConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryNodeTests(InMemoryFixture fixture)
    : FixtureBackedNodeConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryQueueTests(InMemoryFixture fixture)
    : FixtureBackedQueueConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryRateLimitTests(InMemoryFixture fixture)
    : FixtureBackedRateLimitConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryMaintenanceTests(InMemoryFixture fixture)
    : FixtureBackedMaintenanceConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryPurgeTests(InMemoryFixture fixture)
    : FixtureBackedPurgeConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryStatsTests(InMemoryFixture fixture)
    : FixtureBackedStatsConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemorySchemaTests(InMemoryFixture fixture)
    : FixtureBackedSchemaConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryStoreFixTests(InMemoryFixture fixture)
    : FixtureBackedStoreFixConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryQueueStatsParityTests(InMemoryFixture fixture)
    : FixtureBackedQueueStatsParityConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;

internal sealed class InMemoryRuntimeReliabilityTests(InMemoryFixture fixture)
    : FixtureBackedRuntimeReliabilityConformanceTests<InMemoryFixture>(fixture), IClassFixture<InMemoryFixture>;