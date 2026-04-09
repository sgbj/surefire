using Surefire.Tests.Conformance;

namespace Surefire.Tests.Redis;

[CollectionDefinition("Redis")]
public class RedisCollection : ICollectionFixture<RedisFixture>;

[Collection("Redis")]
internal sealed class RedisJobTests(RedisFixture fixture)
    : FixtureBackedJobConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisRunCrudTests(RedisFixture fixture)
    : FixtureBackedRunCrudConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisTransitionTests(RedisFixture fixture)
    : FixtureBackedTransitionConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisCancelTests(RedisFixture fixture)
    : FixtureBackedCancelConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisClaimTests(RedisFixture fixture)
    : FixtureBackedClaimConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisBatchTests(RedisFixture fixture)
    : FixtureBackedBatchConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisEventTests(RedisFixture fixture)
    : FixtureBackedEventConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisNodeTests(RedisFixture fixture)
    : FixtureBackedNodeConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisQueueTests(RedisFixture fixture)
    : FixtureBackedQueueConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisRateLimitTests(RedisFixture fixture)
    : FixtureBackedRateLimitConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisMaintenanceTests(RedisFixture fixture)
    : FixtureBackedMaintenanceConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisPurgeTests(RedisFixture fixture)
    : FixtureBackedPurgeConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisStatsTests(RedisFixture fixture)
    : FixtureBackedStatsConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisSchemaTests(RedisFixture fixture)
    : FixtureBackedSchemaConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisStoreFixTests(RedisFixture fixture)
    : FixtureBackedStoreFixConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisQueueStatsParityTests(RedisFixture fixture)
    : FixtureBackedQueueStatsParityConformanceTests<RedisFixture>(fixture);

[Collection("Redis")]
internal sealed class RedisRuntimeReliabilityTests(RedisFixture fixture)
    : FixtureBackedRuntimeReliabilityConformanceTests<RedisFixture>(fixture);