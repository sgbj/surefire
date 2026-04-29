using Surefire.Tests.Conformance;

namespace Surefire.Tests.SqlServer;

public sealed class ConformanceRunnerParityTests
{
    [Fact]
    public void SqlServer_Covers_All_Conformance_Runners()
    {
        ConformanceRunnerParity.AssertProviderCoverage(typeof(ConformanceRunnerParityTests).Assembly, "SqlServer");
    }
}
