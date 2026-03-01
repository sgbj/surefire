using Surefire.Tests.Conformance;

namespace Surefire.Tests.InMemory;

public sealed class ConformanceRunnerParityTests
{
    [Fact]
    public void InMemory_Covers_All_Conformance_Runners()
    {
        ConformanceRunnerParity.AssertProviderCoverage(typeof(ConformanceRunnerParityTests).Assembly, "InMemory");
    }
}