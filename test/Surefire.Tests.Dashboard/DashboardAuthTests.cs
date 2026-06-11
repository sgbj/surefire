using Surefire.Dashboard;

namespace Surefire.Tests.Dashboard;

public sealed class DashboardAuthOptionsTests
{
    [Fact]
    public void AuthMode_Defaults_To_BrowserToken()
    {
        var options = new SurefireDashboardOptions();
        Assert.Equal(DashboardAuthMode.BrowserToken, options.AuthMode);
    }

    [Fact]
    public void BrowserToken_Defaults_To_Null()
    {
        var options = new SurefireDashboardOptions();
        Assert.Null(options.BrowserToken);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void BrowserToken_Rejects_Empty_And_Whitespace(string value)
    {
        var options = new SurefireDashboardOptions();
        Assert.Throws<ArgumentException>(() => options.BrowserToken = value);
    }

    [Fact]
    public void BrowserToken_Accepts_Explicit_Value_And_Null_Reset()
    {
        var options = new SurefireDashboardOptions { BrowserToken = "my-token" };
        Assert.Equal("my-token", options.BrowserToken);
        options.BrowserToken = null;
        Assert.Null(options.BrowserToken);
    }
}
