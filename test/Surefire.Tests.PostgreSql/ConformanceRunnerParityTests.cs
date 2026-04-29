using Surefire.Tests.Conformance;

namespace Surefire.Tests.PostgreSql;

public sealed class ConformanceRunnerParityTests
{
    [Fact]
    public void PostgreSql_Covers_All_Conformance_Runners()
    {
        ConformanceRunnerParity.AssertProviderCoverage(typeof(ConformanceRunnerParityTests).Assembly, "PostgreSql");
    }
}
