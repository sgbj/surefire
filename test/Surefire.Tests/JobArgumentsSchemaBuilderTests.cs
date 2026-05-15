using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire.Tests;

public sealed class JobArgumentsSchemaBuilderTests
{
    private static string? Build(Delegate handler, Action<IServiceCollection>? registerServices = null)
    {
        var descriptor = ReflectionDescriptorBuilder.BuildJob("Test", handler);
        var collection = new ServiceCollection();
        registerServices?.Invoke(collection);
        var services = collection.BuildServiceProvider();
        var options = new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
        return JobArgumentsSchemaBuilder.BuildFromDescriptor(
            descriptor.Parameters,
            descriptor.ParameterJsonTypeInfoFactories,
            options,
            services);
    }

    [Fact]
    public void Build_NoParams_ReturnsNull()
    {
        Delegate handler = () => { };
        Assert.Null(Build(handler));
    }

    [Fact]
    public void Build_IntParam_SchemaHasIntegerProperty()
    {
        Delegate handler = (int count) => { };
        var schema = Build(handler);

        Assert.NotNull(schema);
        using var doc = JsonDocument.Parse(schema!);
        var prop = doc.RootElement.GetProperty("properties").GetProperty("count");
        Assert.Equal("integer", prop.GetProperty("type").GetString());
    }

    [Fact]
    public void Build_StringParam_SchemaHasStringProperty()
    {
        Delegate handler = (string name) => { };
        var schema = Build(handler);

        Assert.NotNull(schema);
        using var doc = JsonDocument.Parse(schema!);
        Assert.True(doc.RootElement.GetProperty("properties").TryGetProperty("name", out _));
    }

    [Fact]
    public void Build_NonNullableReferenceParam_IsRequired()
    {
        Delegate handler = (string name) => { };
        var schema = Build(handler)!;

        using var doc = JsonDocument.Parse(schema);
        var required = doc.RootElement.GetProperty("required");
        Assert.Contains("name", required.EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void Build_NullableReferenceParam_NotRequired()
    {
        Delegate handler = (string? name) => { };
        var schema = Build(handler)!;

        using var doc = JsonDocument.Parse(schema);
        if (doc.RootElement.TryGetProperty("required", out var required))
        {
            Assert.DoesNotContain("name", required.EnumerateArray().Select(e => e.GetString()));
        }
    }

    [Fact]
    public void Build_NullableValueTypeParam_NotRequired()
    {
        Delegate handler = (int? count) => { };
        var schema = Build(handler)!;

        using var doc = JsonDocument.Parse(schema);
        if (doc.RootElement.TryGetProperty("required", out var required))
        {
            Assert.DoesNotContain("count", required.EnumerateArray().Select(e => e.GetString()));
        }
    }

    [Fact]
    public void Build_DefaultValueParam_NotRequired()
    {
        Delegate handler = (int count = 5) => { };
        var schema = Build(handler)!;

        using var doc = JsonDocument.Parse(schema);
        if (doc.RootElement.TryGetProperty("required", out var required))
        {
            Assert.DoesNotContain("count", required.EnumerateArray().Select(e => e.GetString()));
        }
    }

    [Fact]
    public void Build_JobContextOnly_ReturnsNull()
    {
        Delegate handler = (JobContext ctx) => { };
        Assert.Null(Build(handler));
    }

    [Fact]
    public void Build_CancellationTokenOnly_ReturnsNull()
    {
        Delegate handler = (CancellationToken ct) => { };
        Assert.Null(Build(handler));
    }

    [Fact]
    public void Build_JobContextAndCancellationTokenWithParams_ExcludesBoth()
    {
        Delegate handler = (int count, JobContext ctx, CancellationToken ct) => { };
        var schema = Build(handler)!;

        using var doc = JsonDocument.Parse(schema);
        var props = doc.RootElement.GetProperty("properties");
        Assert.True(props.TryGetProperty("count", out _));
        Assert.False(props.TryGetProperty("ctx", out _));
        Assert.False(props.TryGetProperty("ct", out _));
    }

    [Fact]
    public void Build_ServiceTypeParam_Excluded()
    {
        Delegate handler = (int count, MyService svc) => { };
        var schema = Build(handler, s => s.AddSingleton<MyService>())!;

        using var doc = JsonDocument.Parse(schema);
        var props = doc.RootElement.GetProperty("properties");
        Assert.True(props.TryGetProperty("count", out _));
        Assert.False(props.TryGetProperty("svc", out _));
    }

    [Fact]
    public void Build_AllParamsAreServices_ReturnsNull()
    {
        Delegate handler = (MyService svc) => { };
        Assert.Null(Build(handler, s => s.AddSingleton<MyService>()));
    }

    [Fact]
    public void Build_MultipleParams_AllAppearInProperties()
    {
        Delegate handler = (string name, int age) => { };
        var schema = Build(handler)!;

        using var doc = JsonDocument.Parse(schema);
        var props = doc.RootElement.GetProperty("properties");
        Assert.True(props.TryGetProperty("name", out _));
        Assert.True(props.TryGetProperty("age", out _));
    }

    [Fact]
    public void Build_ListParam_ExcludedFromSchema()
    {
        // Stream-shaped parameters (List, Array, IAsyncEnumerable) are bound either from a
        // declared input stream or from inline JSON; the schema describes only inline-JSON
        // arguments, so stream params are excluded.
        Delegate handler = (List<int> values) => { };
        Assert.Null(Build(handler));
    }

    private sealed class MyService;
}
