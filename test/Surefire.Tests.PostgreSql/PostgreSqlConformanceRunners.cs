using Surefire.Tests.Conformance;

namespace Surefire.Tests.PostgreSql;

[CollectionDefinition("PostgreSql", DisableParallelization = true)]
public class PostgreSqlCollection : ICollectionFixture<PostgreSqlFixture>;

[Collection("PostgreSql")]
public sealed class PostgreSqlJobTests(PostgreSqlFixture fixture)
    : FixtureBackedJobConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlRunCrudTests(PostgreSqlFixture fixture)
    : FixtureBackedRunCrudConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlTransitionTests(PostgreSqlFixture fixture)
    : FixtureBackedTransitionConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlCancelTests(PostgreSqlFixture fixture)
    : FixtureBackedCancelConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlClaimTests(PostgreSqlFixture fixture)
    : FixtureBackedClaimConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlBatchTests(PostgreSqlFixture fixture)
    : FixtureBackedBatchConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlEventTests(PostgreSqlFixture fixture)
    : FixtureBackedEventConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlNodeTests(PostgreSqlFixture fixture)
    : FixtureBackedNodeConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlQueueTests(PostgreSqlFixture fixture)
    : FixtureBackedQueueConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlRateLimitTests(PostgreSqlFixture fixture)
    : FixtureBackedRateLimitConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlMaintenanceTests(PostgreSqlFixture fixture)
    : FixtureBackedMaintenanceConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlPurgeTests(PostgreSqlFixture fixture)
    : FixtureBackedPurgeConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlStatsTests(PostgreSqlFixture fixture)
    : FixtureBackedStatsConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlSchemaTests(PostgreSqlFixture fixture)
    : FixtureBackedSchemaConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlStoreFixTests(PostgreSqlFixture fixture)
    : FixtureBackedStoreFixConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlQueueStatsParityTests(PostgreSqlFixture fixture)
    : FixtureBackedQueueStatsParityConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlRuntimeReliabilityTests(PostgreSqlFixture fixture)
    : FixtureBackedRuntimeReliabilityConformanceTests<PostgreSqlFixture>(fixture);

[Collection("PostgreSql")]
public sealed class PostgreSqlTraceTests(PostgreSqlFixture fixture)
    : FixtureBackedTraceConformanceTests<PostgreSqlFixture>(fixture);