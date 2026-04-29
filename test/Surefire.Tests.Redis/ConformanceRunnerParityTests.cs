using Surefire.Tests.Conformance;

namespace Surefire.Tests.Redis;

public sealed class ConformanceRunnerParityTests
{
    [Fact]
    public void Redis_Covers_All_Conformance_Runners()
    {
        ConformanceRunnerParity.AssertProviderCoverage(typeof(ConformanceRunnerParityTests).Assembly, "Redis");
    }
}
