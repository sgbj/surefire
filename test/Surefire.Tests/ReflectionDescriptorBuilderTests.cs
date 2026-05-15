using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Surefire.Tests;

public sealed class ReflectionDescriptorBuilderTests
{
    [Fact]
    public void BuildJob_Scalar_PopulatesParametersAndReturnInfo()
    {
        var handler = (int a, int b) => a + b;

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Add", handler);

        Assert.Equal("Add", descriptor.Name);
        Assert.Same(handler, descriptor.Handler);
        Assert.Equal(ReturnKind.Scalar, descriptor.ReturnKind);
        Assert.Equal(typeof(int), descriptor.ReturnType);
        Assert.Equal(2, descriptor.Parameters.Count);
        Assert.All(descriptor.Parameters, p => Assert.Equal(ParameterKind.ServiceOrJson, p.Kind));
        Assert.Equal("a", descriptor.Parameters[0].Name);
    }

    [Fact]
    public void BuildJob_Invoke_PropagatesHandlerExceptionsUnwrapped()
    {
        Action handler = () => throw new InvalidOperationException("boom");

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Bang", handler);

        var ex = Assert.Throws<InvalidOperationException>(() => descriptor.Invoke([], descriptor.Handler));
        Assert.Equal("boom", ex.Message);
    }

    [Fact]
    public void BuildJob_TaskOfT_PopulatesExtractTaskResult()
    {
        var handler = (int x) => Task.FromResult(x * 2);

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Double", handler);

        Assert.Equal(ReturnKind.TaskOfT, descriptor.ReturnKind);
        Assert.NotNull(descriptor.ExtractTaskResult);
        var result = descriptor.ExtractTaskResult!(Task.FromResult(21));
        Assert.Equal(21, result);
    }

    [Fact]
    public void BuildJob_ValueTaskOfT_PopulatesAsTaskAndExtract()
    {
        var handler = (int x) => new ValueTask<int>(x + 1);

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Plus", handler);

        Assert.Equal(ReturnKind.ValueTaskOfT, descriptor.ReturnKind);
        Assert.NotNull(descriptor.AsTask);
        Assert.NotNull(descriptor.ExtractTaskResult);
        var task = descriptor.AsTask!(new ValueTask<int>(42));
        Assert.Equal(42, descriptor.ExtractTaskResult!(task));
    }

    [Fact]
    public void BuildJob_AsyncEnumerable_PopulatesMaterializerAndElementType()
    {
        var handler = () => StreamInts();

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Stream", handler);

        Assert.Equal(ReturnKind.AsyncEnumerable, descriptor.ReturnKind);
        Assert.Equal(typeof(int), descriptor.AsyncEnumerableElementType);
        Assert.NotNull(descriptor.Materializer);

        static async IAsyncEnumerable<int> StreamInts()
        {
            yield return 1;
            await Task.Yield();
        }
    }

    [Fact]
    public void BuildJob_FrameworkParameters_ClassifiedCorrectly()
    {
        var handler = (JobContext ctx, CancellationToken ct, IServiceProvider sp, int value) => ctx.JobName;

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Mixed", handler);

        Assert.Equal(ParameterKind.JobContext, descriptor.Parameters[0].Kind);
        Assert.Equal(ParameterKind.CancellationToken, descriptor.Parameters[1].Kind);
        Assert.Equal(ParameterKind.ServiceProvider, descriptor.Parameters[2].Kind);
        Assert.Equal(ParameterKind.ServiceOrJson, descriptor.Parameters[3].Kind);
    }

    [Fact]
    public void BuildJob_AsyncEnumerableParameter_ClassifiedAsStream()
    {
        var handler = (IAsyncEnumerable<int> input) => input;

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Pass", handler);

        Assert.Equal(ParameterKind.Stream, descriptor.Parameters[0].Kind);
        Assert.Equal(typeof(int), descriptor.Parameters[0].StreamElementType);
        Assert.Equal(StreamShape.AsyncEnumerable, descriptor.Parameters[0].StreamShape);
        Assert.NotNull(descriptor.StreamBinders);
        Assert.NotNull(descriptor.StreamBinders![0]);
        Assert.NotNull(descriptor.InputJsonBinders);
        Assert.NotNull(descriptor.InputJsonBinders![0]);
    }

    [Fact]
    public void BuildJob_ArrayParameter_ClassifiedAsStreamArray()
    {
        var handler = (int[] values) => values.Length;

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Sum", handler);

        Assert.Equal(ParameterKind.Stream, descriptor.Parameters[0].Kind);
        Assert.Equal(typeof(int), descriptor.Parameters[0].StreamElementType);
        Assert.Equal(StreamShape.Array, descriptor.Parameters[0].StreamShape);
    }

    [Fact]
    public void BuildJob_ListParameter_ClassifiedAsStreamList()
    {
        var handler = (List<int> values) => values.Count;

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Count", handler);

        Assert.Equal(ParameterKind.Stream, descriptor.Parameters[0].Kind);
        Assert.Equal(typeof(int), descriptor.Parameters[0].StreamElementType);
        Assert.Equal(StreamShape.List, descriptor.Parameters[0].StreamShape);
    }

    [Fact]
    public void BuildJob_ResultJsonTypeInfo_ResolvesAgainstSerializer()
    {
        var handler = (int x) => x.ToString();
        var options = new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

        var descriptor = ReflectionDescriptorBuilder.BuildJob("ToString", handler);

        Assert.NotNull(descriptor.ResultJsonTypeInfoFactory);
        var typeInfo = descriptor.ResultJsonTypeInfoFactory!(options);
        Assert.NotNull(typeInfo);
        Assert.Equal(typeof(string), typeInfo!.Type);
    }

    [Fact]
    public void BuildCallback_VoidWithJobContext_PopulatesShape()
    {
        var handler = (JobContext ctx) => { };

        var descriptor = ReflectionDescriptorBuilder.BuildCallback(handler);

        Assert.Equal(ReturnKind.Void, descriptor.ReturnKind);
        Assert.Single(descriptor.Parameters);
        Assert.Equal(ParameterKind.JobContext, descriptor.Parameters[0].Kind);
    }

    [Fact]
    public void BuildCallback_ValueTaskOfT_PopulatesAsTask()
    {
        var handler = () => new ValueTask<int>(0);

        var descriptor = ReflectionDescriptorBuilder.BuildCallback(handler);

        Assert.Equal(ReturnKind.ValueTaskOfT, descriptor.ReturnKind);
        Assert.NotNull(descriptor.AsTask);
    }

    [Fact]
    public void BuildJob_ValueTask_ClassifiedAsTask()
    {
        var handler = () => ValueTask.CompletedTask;

        var descriptor = ReflectionDescriptorBuilder.BuildJob("V", handler);

        Assert.Equal(ReturnKind.ValueTask, descriptor.ReturnKind);
    }

    [Fact]
    public void BuildJob_TaskOfAsyncEnumerable_UnwrapsToAsyncEnumerable()
    {
        var handler = () => Task.FromResult(AsyncEnumerable.Empty<int>());

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Wrapped", handler);

        Assert.Equal(ReturnKind.AsyncEnumerable, descriptor.ReturnKind);
        Assert.Equal(typeof(int), descriptor.AsyncEnumerableElementType);
        Assert.NotNull(descriptor.Materializer);
    }

    [Fact]
    public void BuildJob_ValueTaskOfAsyncEnumerable_UnwrapsToAsyncEnumerable()
    {
        var handler = () => new ValueTask<IAsyncEnumerable<int>>(AsyncEnumerable.Empty<int>());

        var descriptor = ReflectionDescriptorBuilder.BuildJob("VWrapped", handler);

        Assert.Equal(ReturnKind.AsyncEnumerable, descriptor.ReturnKind);
        Assert.Equal(typeof(int), descriptor.AsyncEnumerableElementType);
    }

    [Fact]
    public void BuildJob_Invoke_PassesArgsInOrder()
    {
        var handler = (int a, string b, double c) => $"{a}-{b}-{c}";

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Concat", handler);
        var result = descriptor.Invoke([1, "x", 2.5], descriptor.Handler);

        Assert.Equal("1-x-2.5", result);
    }

    [Fact]
    public void BuildJob_Invoke_BoxesValueTypeResults()
    {
        var handler = () => 42;

        var descriptor = ReflectionDescriptorBuilder.BuildJob("Forty", handler);
        var result = descriptor.Invoke([], descriptor.Handler);

        Assert.IsType<int>(result);
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task BuildJob_Invoke_AwaitsTask()
    {
        var ran = false;
        var handler = async () =>
        {
            await Task.Yield();
            ran = true;
        };

        var descriptor = ReflectionDescriptorBuilder.BuildJob("AsyncVoid", handler);
        await (Task)descriptor.Invoke([], descriptor.Handler)!;

        Assert.True(ran);
    }

    [Fact]
    public async Task BuildJob_Invoke_AwaitsValueTask()
    {
        var ran = false;
        Func<ValueTask> handler = async () =>
        {
            await Task.Yield();
            ran = true;
        };

        var descriptor = ReflectionDescriptorBuilder.BuildJob("AsyncVT", handler);
        await (ValueTask)descriptor.Invoke([], descriptor.Handler)!;

        Assert.True(ran);
    }

    [Fact]
    public void BuildCallback_TaskOfT_PopulatesReturnKind()
    {
        var handler = () => Task.FromResult(1);

        var descriptor = ReflectionDescriptorBuilder.BuildCallback(handler);

        Assert.Equal(ReturnKind.TaskOfT, descriptor.ReturnKind);
    }

    [Fact]
    public void BuildCallback_AsyncEnumerable_PopulatesReturnKind()
    {
        var handler = () => AsyncEnumerable.Empty<int>();

        var descriptor = ReflectionDescriptorBuilder.BuildCallback(handler);

        Assert.Equal(ReturnKind.AsyncEnumerable, descriptor.ReturnKind);
    }

    [Fact]
    public void BuildCallback_Scalar_PopulatesReturnKind()
    {
        var handler = () => 1;

        var descriptor = ReflectionDescriptorBuilder.BuildCallback(handler);

        Assert.Equal(ReturnKind.Scalar, descriptor.ReturnKind);
    }
}
