using System.Reflection;

namespace Surefire.Tests;

public sealed class HandlerMetadataTests
{
    // ── Invoke ────────────────────────────────────────────────────────────────

    [Fact]
    public void Invoke_CallsDelegate_WithCompiledExpression()
    {
        int captured = 0;
        Delegate handler = (int x) => { captured = x; };
        var meta = HandlerMetadata.Build(handler);

        meta.Invoke([42]);

        Assert.Equal(42, captured);
    }

    // ── HasResult ─────────────────────────────────────────────────────────────

    [Fact]
    public void HasResult_False_ForVoidHandler()
    {
        Delegate handler = () => { };
        var meta = HandlerMetadata.Build(handler);
        Assert.False(meta.HasResult);
    }

    [Fact]
    public void HasResult_False_ForTaskHandler()
    {
        Delegate handler = () => Task.CompletedTask;
        var meta = HandlerMetadata.Build(handler);
        Assert.False(meta.HasResult);
    }

    [Fact]
    public void HasResult_False_ForValueTaskHandler()
    {
        Delegate handler = () => ValueTask.CompletedTask;
        var meta = HandlerMetadata.Build(handler);
        Assert.False(meta.HasResult);
    }

    [Fact]
    public void HasResult_True_ForTaskOfT()
    {
        Delegate handler = () => Task.FromResult(99);
        var meta = HandlerMetadata.Build(handler);
        Assert.True(meta.HasResult);
    }

    [Fact]
    public void HasResult_True_ForValueTaskOfT()
    {
        Delegate handler = () => ValueTask.FromResult("hello");
        var meta = HandlerMetadata.Build(handler);
        Assert.True(meta.HasResult);
    }

    // ── ExtractTaskResult ─────────────────────────────────────────────────────

    [Fact]
    public async Task ExtractTaskResult_ReturnsBoxedResult_ForTaskOfInt()
    {
        Delegate handler = () => Task.FromResult(7);
        var meta = HandlerMetadata.Build(handler);
        Assert.NotNull(meta.ExtractTaskResult);

        var task = Task.FromResult(7);
        await task;
        var result = meta.ExtractTaskResult!(task);
        Assert.Equal(7, result);
    }

    [Fact]
    public void ExtractTaskResult_Null_ForPlainTask()
    {
        Delegate handler = () => Task.CompletedTask;
        var meta = HandlerMetadata.Build(handler);
        Assert.Null(meta.ExtractTaskResult);
    }

    // ── AsTaskDelegate ────────────────────────────────────────────────────────

    [Fact]
    public async Task AsTaskDelegate_ConvertsValueTaskOfT_ToTask()
    {
        Delegate handler = () => ValueTask.FromResult(42);
        var meta = HandlerMetadata.Build(handler);
        Assert.NotNull(meta.AsTaskDelegate);

        var vt = ValueTask.FromResult(42);
        var task = meta.AsTaskDelegate!(vt);
        var result = await (Task<int>)task;
        Assert.Equal(42, result);
    }

    [Fact]
    public void AsTaskDelegate_Null_ForTaskOfT()
    {
        Delegate handler = () => Task.FromResult(1);
        var meta = HandlerMetadata.Build(handler);
        Assert.Null(meta.AsTaskDelegate);
    }

    // ── Parameters ────────────────────────────────────────────────────────────

    [Fact]
    public void Parameters_CachesParameterInfoArray()
    {
        Delegate handler = (int a, string b) => Task.CompletedTask;
        var meta = HandlerMetadata.Build(handler);

        Assert.Equal(2, meta.Parameters.Length);
        Assert.Same(meta.Parameters, meta.Parameters); // reference-stable
    }

    // ── StreamBinders ─────────────────────────────────────────────────────────

    [Fact]
    public void StreamBinders_NullForNonStreamParameters()
    {
        Delegate handler = (int x, string y) => Task.CompletedTask;
        var meta = HandlerMetadata.Build(handler);

        Assert.Equal(2, meta.StreamBinders.Count);
        Assert.Null(meta.StreamBinders[0]);
        Assert.Null(meta.StreamBinders[1]);
    }

    [Fact]
    public void StreamBinders_NonNullForIAsyncEnumerableParameter()
    {
        Delegate handler = (IAsyncEnumerable<string> stream) => Task.CompletedTask;
        var meta = HandlerMetadata.Build(handler);

        Assert.Single(meta.StreamBinders);
        Assert.NotNull(meta.StreamBinders[0]);
    }

    [Fact]
    public void StreamBinders_NonNullForListParameter()
    {
        Delegate handler = (List<int> items) => Task.CompletedTask;
        var meta = HandlerMetadata.Build(handler);

        Assert.Single(meta.StreamBinders);
        Assert.NotNull(meta.StreamBinders[0]);
    }

    [Fact]
    public void StreamBinders_NonNullForArrayParameter()
    {
        Delegate handler = (int[] items) => Task.CompletedTask;
        var meta = HandlerMetadata.Build(handler);

        Assert.Single(meta.StreamBinders);
        Assert.NotNull(meta.StreamBinders[0]);
    }

    // ── AsyncEnumerableElementType / Materializer ─────────────────────────────

    [Fact]
    public void AsyncEnumerableElementType_Null_ForNonStreamReturn()
    {
        Delegate handler = () => Task.FromResult(1);
        var meta = HandlerMetadata.Build(handler);
        Assert.Null(meta.AsyncEnumerableElementType);
        Assert.Null(meta.Materializer);
    }

    [Fact]
    public void AsyncEnumerableElementType_Set_ForIAsyncEnumerableReturn()
    {
        Delegate handler = () => AsyncEnumerable.Empty<string>();
        var meta = HandlerMetadata.Build(handler);

        Assert.Equal(typeof(string), meta.AsyncEnumerableElementType);
        Assert.NotNull(meta.Materializer);
    }
}
