using Surefire.Tests.Conformance;

namespace Surefire.Tests.SqlServer;

[CollectionDefinition("SqlServer")]
public class SqlServerCollection : ICollectionFixture<SqlServerFixture>;

[Collection("SqlServer")]
public sealed class SqlServerJobTests(SqlServerFixture fixture)
    : FixtureBackedJobConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerRunCrudTests(SqlServerFixture fixture)
    : FixtureBackedRunCrudConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerTransitionTests(SqlServerFixture fixture)
    : FixtureBackedTransitionConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerCancelTests(SqlServerFixture fixture)
    : FixtureBackedCancelConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerClaimTests(SqlServerFixture fixture)
    : FixtureBackedClaimConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerBatchTests(SqlServerFixture fixture)
    : FixtureBackedBatchConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerEventTests(SqlServerFixture fixture)
    : FixtureBackedEventConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerNodeTests(SqlServerFixture fixture)
    : FixtureBackedNodeConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerQueueTests(SqlServerFixture fixture)
    : FixtureBackedQueueConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerRateLimitTests(SqlServerFixture fixture)
    : FixtureBackedRateLimitConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerMaintenanceTests(SqlServerFixture fixture)
    : FixtureBackedMaintenanceConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerPurgeTests(SqlServerFixture fixture)
    : FixtureBackedPurgeConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerStatsTests(SqlServerFixture fixture)
    : FixtureBackedStatsConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerSchemaTests(SqlServerFixture fixture)
    : FixtureBackedSchemaConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerStoreFixTests(SqlServerFixture fixture)
    : FixtureBackedStoreFixConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerQueueStatsParityTests(SqlServerFixture fixture)
    : FixtureBackedQueueStatsParityConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
public sealed class SqlServerRuntimeReliabilityTests(SqlServerFixture fixture)
    : FixtureBackedRuntimeReliabilityConformanceTests<SqlServerFixture>(fixture);