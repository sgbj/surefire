using Surefire.Tests.Conformance;

namespace Surefire.Tests.Sqlite;

public sealed class ConformanceRunnerParityTests
{
    [Fact]
    public void Sqlite_Covers_All_Conformance_Runners()
    {
        ConformanceRunnerParity.AssertProviderCoverage(typeof(ConformanceRunnerParityTests).Assembly, "Sqlite");
    }
}
