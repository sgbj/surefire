using Surefire.Tests.Conformance;

namespace Surefire.Tests.SqlServer;

[CollectionDefinition("SqlServer")]
public class SqlServerCollection : ICollectionFixture<SqlServerFixture>;

[Collection("SqlServer")]
internal sealed class SqlServerJobTests(SqlServerFixture fixture)
    : FixtureBackedJobConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerRunCrudTests(SqlServerFixture fixture)
    : FixtureBackedRunCrudConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerTransitionTests(SqlServerFixture fixture)
    : FixtureBackedTransitionConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerCancelTests(SqlServerFixture fixture)
    : FixtureBackedCancelConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerClaimTests(SqlServerFixture fixture)
    : FixtureBackedClaimConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerBatchTests(SqlServerFixture fixture)
    : FixtureBackedBatchConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerEventTests(SqlServerFixture fixture)
    : FixtureBackedEventConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerNodeTests(SqlServerFixture fixture)
    : FixtureBackedNodeConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerQueueTests(SqlServerFixture fixture)
    : FixtureBackedQueueConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerRateLimitTests(SqlServerFixture fixture)
    : FixtureBackedRateLimitConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerMaintenanceTests(SqlServerFixture fixture)
    : FixtureBackedMaintenanceConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerPurgeTests(SqlServerFixture fixture)
    : FixtureBackedPurgeConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerStatsTests(SqlServerFixture fixture)
    : FixtureBackedStatsConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerSchemaTests(SqlServerFixture fixture)
    : FixtureBackedSchemaConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerStoreFixTests(SqlServerFixture fixture)
    : FixtureBackedStoreFixConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerQueueStatsParityTests(SqlServerFixture fixture)
    : FixtureBackedQueueStatsParityConformanceTests<SqlServerFixture>(fixture);

[Collection("SqlServer")]
internal sealed class SqlServerRuntimeReliabilityTests(SqlServerFixture fixture)
    : FixtureBackedRuntimeReliabilityConformanceTests<SqlServerFixture>(fixture);