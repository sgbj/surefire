using System.Reflection;

namespace Surefire.Tests.Conformance;

public static class ConformanceRunnerParity
{
    private static readonly (string Suffix, Type BaseType)[] Contracts =
    [
        ("JobTests", typeof(JobConformanceTests)),
        ("RunCrudTests", typeof(RunCrudConformanceTests)),
        ("TransitionTests", typeof(TransitionConformanceTests)),
        ("CancelTests", typeof(CancelConformanceTests)),
        ("ClaimTests", typeof(ClaimConformanceTests)),
        ("BatchTests", typeof(BatchConformanceTests)),
        ("EventTests", typeof(EventConformanceTests)),
        ("NodeTests", typeof(NodeConformanceTests)),
        ("QueueTests", typeof(QueueConformanceTests)),
        ("RateLimitTests", typeof(RateLimitConformanceTests)),
        ("MaintenanceTests", typeof(MaintenanceConformanceTests)),
        ("PurgeTests", typeof(PurgeConformanceTests)),
        ("StatsTests", typeof(StatsConformanceTests)),
        ("SchemaTests", typeof(SchemaConformanceTests)),
        ("StoreFixTests", typeof(StoreFixConformanceTests)),
        ("QueueStatsParityTests", typeof(QueueStatsParityConformanceTests)),
        ("RuntimeReliabilityTests", typeof(RuntimeReliabilityConformanceTests)),
        ("TraceTests", typeof(TraceConformanceTests))
    ];

    public static void AssertProviderCoverage(Assembly providerTestAssembly, string providerPrefix)
    {
        var providerTypes = providerTestAssembly.GetTypes();

        foreach (var (suffix, baseType) in Contracts)
        {
            var expectedTypeName = providerPrefix + suffix;
            var runnerType =
                providerTypes.SingleOrDefault(t => string.Equals(t.Name, expectedTypeName, StringComparison.Ordinal));

            Assert.True(runnerType is { }, $"Missing conformance runner '{expectedTypeName}'.");
            Assert.True(baseType.IsAssignableFrom(runnerType),
                $"Runner '{expectedTypeName}' must derive from '{baseType.Name}'.");
        }
    }
}