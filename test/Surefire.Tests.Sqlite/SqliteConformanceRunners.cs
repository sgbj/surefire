using Surefire.Tests.Conformance;

namespace Surefire.Tests.Sqlite;

[CollectionDefinition("Sqlite")]
public class SqliteCollection : ICollectionFixture<SqliteFixture>;

[Collection("Sqlite")]
public sealed class SqliteJobTests(SqliteFixture fixture)
    : FixtureBackedJobConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteRunCrudTests(SqliteFixture fixture)
    : FixtureBackedRunCrudConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteTransitionTests(SqliteFixture fixture)
    : FixtureBackedTransitionConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteCancelTests(SqliteFixture fixture)
    : FixtureBackedCancelConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteClaimTests(SqliteFixture fixture)
    : FixtureBackedClaimConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteBatchTests(SqliteFixture fixture)
    : FixtureBackedBatchConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteEventTests(SqliteFixture fixture)
    : FixtureBackedEventConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteNodeTests(SqliteFixture fixture)
    : FixtureBackedNodeConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteQueueTests(SqliteFixture fixture)
    : FixtureBackedQueueConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteRateLimitTests(SqliteFixture fixture)
    : FixtureBackedRateLimitConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteMaintenanceTests(SqliteFixture fixture)
    : FixtureBackedMaintenanceConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqlitePurgeTests(SqliteFixture fixture)
    : FixtureBackedPurgeConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteStatsTests(SqliteFixture fixture)
    : FixtureBackedStatsConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteSchemaTests(SqliteFixture fixture)
    : FixtureBackedSchemaConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteStoreFixTests(SqliteFixture fixture)
    : FixtureBackedStoreFixConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteQueueStatsParityTests(SqliteFixture fixture)
    : FixtureBackedQueueStatsParityConformanceTests<SqliteFixture>(fixture);

[Collection("Sqlite")]
public sealed class SqliteRuntimeReliabilityTests(SqliteFixture fixture)
    : FixtureBackedRuntimeReliabilityConformanceTests<SqliteFixture>(fixture);