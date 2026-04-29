using System.Text.Json;

namespace Surefire.Tests;

public sealed class JobArgumentsSchemaBuilderTests
{
    private static bool NoServices(Type _) => false;

    [Fact]
    public void Build_NoParams_ReturnsNull()
    {
        Delegate handler = () => { };
        Assert.Null(JobArgumentsSchemaBuilder.Build(handler, NoServices));
    }

    [Fact]
    public void Build_IntParam_SchemaHasIntegerProperty()
    {
        Delegate handler = (int count) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, NoServices);

        Assert.NotNull(schema);
        var doc = JsonDocument.Parse(schema);
        var prop = doc.RootElement.GetProperty("properties").GetProperty("count");
        Assert.Equal("integer", prop.GetProperty("type").GetString());
    }

    [Fact]
    public void Build_StringParam_SchemaHasStringProperty()
    {
        Delegate handler = (string name) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, NoServices);

        Assert.NotNull(schema);
        var doc = JsonDocument.Parse(schema);
        // String maps to {"type": "string"} or {"type": ["string","null"]} depending on schema options
        Assert.True(doc.RootElement.GetProperty("properties").TryGetProperty("name", out _));
    }

    [Fact]
    public void Build_NonNullableReferenceParam_IsRequired()
    {
        Delegate handler = (string name) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, NoServices)!;

        var doc = JsonDocument.Parse(schema);
        var required = doc.RootElement.GetProperty("required");
        Assert.Contains("name", required.EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void Build_NullableReferenceParam_NotRequired()
    {
        Delegate handler = (string? name) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, NoServices)!;

        var doc = JsonDocument.Parse(schema);
        var hasRequired = doc.RootElement.TryGetProperty("required", out var required);
        if (hasRequired)
        {
            Assert.DoesNotContain("name", required.EnumerateArray().Select(e => e.GetString()));
        }
    }

    [Fact]
    public void Build_NullableValueTypeParam_NotRequired()
    {
        Delegate handler = (int? count) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, NoServices)!;

        var doc = JsonDocument.Parse(schema);
        var hasRequired = doc.RootElement.TryGetProperty("required", out var required);
        if (hasRequired)
        {
            Assert.DoesNotContain("count", required.EnumerateArray().Select(e => e.GetString()));
        }
    }

    [Fact]
    public void Build_DefaultValueParam_NotRequired()
    {
        Delegate handler = (int count = 5) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, NoServices)!;

        var doc = JsonDocument.Parse(schema);
        var hasRequired = doc.RootElement.TryGetProperty("required", out var required);
        if (hasRequired)
        {
            Assert.DoesNotContain("count", required.EnumerateArray().Select(e => e.GetString()));
        }
    }

    [Fact]
    public void Build_JobContextOnly_ReturnsNull()
    {
        Delegate handler = (JobContext ctx) => { };
        Assert.Null(JobArgumentsSchemaBuilder.Build(handler, NoServices));
    }

    [Fact]
    public void Build_CancellationTokenOnly_ReturnsNull()
    {
        Delegate handler = (CancellationToken ct) => { };
        Assert.Null(JobArgumentsSchemaBuilder.Build(handler, NoServices));
    }

    [Fact]
    public void Build_JobContextAndCancellationTokenWithParams_ExcludesBoth()
    {
        Delegate handler = (int count, JobContext ctx, CancellationToken ct) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, NoServices)!;

        var doc = JsonDocument.Parse(schema);
        var props = doc.RootElement.GetProperty("properties");
        Assert.True(props.TryGetProperty("count", out _));
        Assert.False(props.TryGetProperty("ctx", out _));
        Assert.False(props.TryGetProperty("ct", out _));
    }

    [Fact]
    public void Build_ServiceTypeParam_Excluded()
    {
        Delegate handler = (int count, MyService svc) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, t => t == typeof(MyService))!;

        var doc = JsonDocument.Parse(schema);
        var props = doc.RootElement.GetProperty("properties");
        Assert.True(props.TryGetProperty("count", out _));
        Assert.False(props.TryGetProperty("svc", out _));
    }

    [Fact]
    public void Build_AllParamsAreServices_ReturnsNull()
    {
        Delegate handler = (MyService svc) => { };
        Assert.Null(JobArgumentsSchemaBuilder.Build(handler, t => t == typeof(MyService)));
    }

    [Fact]
    public void Build_MultipleParams_AllAppearInProperties()
    {
        Delegate handler = (string name, int age) => { };
        var schema = JobArgumentsSchemaBuilder.Build(handler, NoServices)!;

        var doc = JsonDocument.Parse(schema);
        var props = doc.RootElement.GetProperty("properties");
        Assert.True(props.TryGetProperty("name", out _));
        Assert.True(props.TryGetProperty("age", out _));
    }

    private sealed class MyService;
}
