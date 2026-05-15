using Microsoft.CodeAnalysis;

namespace Surefire.Tests.SourceGeneration;

/// <summary>
///     Verifies that the source generator emits expected interceptor scaffolding for common
///     handler and call-site shapes. These tests assert on the shape of the generated code, not
///     on exact byte-for-byte output, to stay robust as the emitter evolves.
/// </summary>
public sealed class EmissionTests
{
    [Fact]
    public void AddJob_SimpleScalarHandler_EmitsInterceptorAndDescriptor()
    {
        var source = """
                     using Microsoft.Extensions.Hosting;

                     internal static class Caller
                     {
                         public static void Wire(IHost host) => host.AddJob("Add", (int a, int b) => a + b);
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("InterceptsLocation", generated);
        Assert.Contains("AddJob_", generated);
        Assert.Contains("BuildDescriptor_", generated);
        Assert.Contains("new global::Surefire.ParameterDescriptor", generated);
        Assert.Contains("ResultJsonTypeInfoFactory", generated);
    }

    [Fact]
    public void AddJob_AsyncEnumerableHandler_EmitsMaterializer()
    {
        var source = """
                     using System.Collections.Generic;
                     using Microsoft.Extensions.Hosting;

                     internal static class Caller
                     {
                         public static void Wire(IHost host) => host.AddJob("Stream", () => Produce());

                         private static async IAsyncEnumerable<int> Produce()
                         {
                             yield return 1;
                             await System.Threading.Tasks.Task.CompletedTask;
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("Materializer = static", generated);
        Assert.Contains("WriteOutputStreamAsync", generated);
    }

    [Fact]
    public void AddJob_AsyncEnumerableInput_EmitsStreamBinderAndJsonBinder()
    {
        var source = """
                     using System.Collections.Generic;
                     using Microsoft.Extensions.Hosting;

                     internal static class Caller
                     {
                         public static void Wire(IHost host) =>
                             host.AddJob("Sum", async (IAsyncEnumerable<int> items) =>
                             {
                                 var total = 0;
                                 await foreach (var v in items) total += v;
                                 return total;
                             });
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("StreamBinders", generated);
        Assert.Contains("ReadInputStreamAsync", generated);
        Assert.Contains("InputJsonBinders", generated);
    }

    [Fact]
    public void IJobClient_RunAsync_AnonymousArgs_EmitsPerCallSerializer()
    {
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.RunAsync<int>("Add", new { a = 1, b = 2 });
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("RunAsync_Client_", generated);
        Assert.Contains("WriteJson", generated);
        Assert.Contains("WritePropertyName(\"a\")", generated);
        Assert.Contains("WritePropertyName(\"b\")", generated);
    }

    [Fact]
    public void IJobClient_RunAsync_LiteralNull_BindsAotSafeOverloadDirectly()
    {
        // A bare `null` literal binds to the more specific `RunArguments?` overload during
        // C# overload resolution, so the generator has nothing to rewrite. The call already hits
        // the AOT-safe path. Verify no interception was emitted.
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.RunAsync<int>("Add", null);
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        Assert.DoesNotContain("RunAsync_Client_", result.GeneratedSource());
    }

    [Fact]
    public void IJobClient_RunBatchAsync_AnonymousElement_EmitsMapIteratorAndForwards()
    {
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.RunBatchAsync<int>("Add", new[] { new { a = 1, b = 2 } });
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out var compilation);
        var generated = result.GeneratedSource();

        Assert.Contains("RunBatchAsync_Client_", generated);
        Assert.Contains("MapBatchArgs_", generated);
        Assert.Contains("yield return BuildArgs_", generated);
        // Forwards to the IEnumerable<RunArguments?> AOT-safe overload.
        Assert.Contains("client.RunBatchAsync<TResult>(job, MapBatchArgs_", generated);
        var errors = compilation.GetDiagnostics(TestContext.Current.CancellationToken)
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Select(d => d.ToString())
            .ToArray();
        Assert.True(errors.Length == 0, string.Join(Environment.NewLine, errors));
    }

    [Fact]
    public void IJobClient_RunBatchAsync_NonGeneric_AnonymousElement_EmitsTaskForwarder()
    {
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.RunBatchAsync("Add", new[] { new { a = 1, b = 2 } });
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("RunBatchAsync_Client_", generated);
        // No <TResult> in the synthesized signature for the non-generic overload.
        Assert.DoesNotContain("RunBatchAsync_Client_0<TResult>", generated);
        Assert.Contains("client.RunBatchAsync(job, MapBatchArgs_", generated);
    }

    [Fact]
    public void IJobClient_TriggerBatchAsync_AnonymousElement_EmitsForwarder()
    {
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.TriggerBatchAsync("Add", new[] { new { a = 1 } });
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("TriggerBatchAsync_Client_", generated);
        Assert.Contains("client.TriggerBatchAsync(job, MapBatchArgs_", generated);
    }

    [Fact]
    public void IJobClient_RunBatchAsync_RunArgumentsElement_BindsAotSafeOverloadDirectly()
    {
        // RunArguments?[] picks the IEnumerable<RunArguments?> overload via overload resolution,
        // so the generator has nothing to rewrite. Verify no interception was emitted.
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.RunBatchAsync<int>("Add",
                                 new[] { new RunArguments { Json = "{\"a\":1}" } });
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        Assert.DoesNotContain("RunBatchAsync_Client_", result.GeneratedSource());
    }

    [Fact]
    public void BatchItem_Create_AnonymousArgs_EmitsConstructorForwarder()
    {
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static BatchItem Build() => BatchItem.Create("Add", new { a = 1, b = 2 });
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out var compilation);
        var generated = result.GeneratedSource();

        Assert.Contains("Create_0", generated);
        Assert.Contains("new global::Surefire.BatchItem(jobName, runArgs, options)", generated);
        Assert.Contains("BuildArgs_0", generated);
        var errors = compilation.GetDiagnostics(TestContext.Current.CancellationToken)
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Select(d => d.ToString())
            .ToArray();
        Assert.True(errors.Length == 0, string.Join(Environment.NewLine, errors));
    }

    [Fact]
    public void BatchItem_Create_NullArgs_EmitsNullPath()
    {
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static BatchItem Build() => BatchItem.Create("Add", null);
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("Create_0", generated);
        Assert.Contains("global::Surefire.RunArguments? runArgs = null;", generated);
        Assert.DoesNotContain("BuildArgs_0", generated);
    }

    [Fact]
    public void IJobClient_RunAsync_NamedTypeArgs_EmitsWholeObjectSerializer()
    {
        var source = """
                     using Surefire;

                     internal sealed record AddArgs(int A, int B);

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.RunAsync<int>("Add", new AddArgs(1, 2));
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out var compilation);
        var generated = result.GeneratedSource();

        Assert.Contains("RunAsync_Client_", generated);
        Assert.Contains("BuildArgs_0", generated);
        // Named types serialize the whole object via JsonTypeInfo<T>, not per property.
        Assert.Contains("JsonTypeInfo<global::AddArgs>", generated);
        Assert.Contains("JsonSerializer.Serialize(writer, typed, ti)", generated);
        Assert.DoesNotContain("WriteStartObject", generated);
        var errors = compilation.GetDiagnostics(TestContext.Current.CancellationToken)
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Select(d => d.ToString())
            .ToArray();
        Assert.True(errors.Length == 0, string.Join(Environment.NewLine, errors));
    }

    [Fact]
    public void IJobClient_RunAsync_NonGeneric_AnonymousArgs_EmitsTaskForwarder()
    {
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.RunAsync("Notify", new { user = "u1" });
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out var compilation);
        var generated = result.GeneratedSource();

        Assert.Contains("RunAsync_Client_", generated);
        Assert.DoesNotContain("RunAsync_Client_0<TResult>", generated);
        Assert.Contains("client.RunAsync(job, runArgs, options, cancellationToken)", generated);
        var errors = compilation.GetDiagnostics(TestContext.Current.CancellationToken)
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Select(d => d.ToString())
            .ToArray();
        Assert.True(errors.Length == 0, string.Join(Environment.NewLine, errors));
    }

    [Fact]
    public void IJobClient_RunBatchAsync_NamedTypeElement_EmitsMapIteratorAndWholeObject()
    {
        var source = """
                     using Surefire;

                     internal sealed record AddArgs(int A, int B);

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client)
                         {
                             await client.RunBatchAsync<int>("Add",
                                 new[] { new AddArgs(1, 2), new AddArgs(3, 4) });
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("RunBatchAsync_Client_", generated);
        Assert.Contains("MapBatchArgs_", generated);
        Assert.Contains("JsonTypeInfo<global::AddArgs>", generated);
        Assert.Contains("JsonSerializer.Serialize(writer, typed, ti)", generated);
    }

    [Fact]
    public void BatchItem_Create_NamedTypeArgs_EmitsConstructorForwarder()
    {
        var source = """
                     using Surefire;

                     internal sealed record AddArgs(int A, int B);

                     internal static class Caller
                     {
                         public static BatchItem Build() => BatchItem.Create("Add", new AddArgs(1, 2));
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out var compilation);
        var generated = result.GeneratedSource();

        Assert.Contains("Create_0", generated);
        Assert.Contains("new global::Surefire.BatchItem(jobName, runArgs, options)", generated);
        Assert.Contains("JsonTypeInfo<global::AddArgs>", generated);
        var errors = compilation.GetDiagnostics(TestContext.Current.CancellationToken)
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Select(d => d.ToString())
            .ToArray();
        Assert.True(errors.Length == 0, string.Join(Environment.NewLine, errors));
    }

    [Fact]
    public void IJobClient_RunAsync_ObjectTypedVariable_BindsRequiresOverload()
    {
        // object-typed args have no static identity, so source gen can't emit a typed serializer
        // and the call must fall through to the [Requires*] overload.
        var source = """
                     using Surefire;

                     internal static class Caller
                     {
                         public static async System.Threading.Tasks.Task Run(IJobClient client, object payload)
                         {
                             await client.RunAsync<int>("Add", payload);
                         }
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        Assert.DoesNotContain("RunAsync_Client_", result.GeneratedSource());
    }

    [Fact]
    public void Callback_OnSuccess_EmitsDescriptor()
    {
        var source = """
                     using Surefire;
                     using Microsoft.Extensions.Hosting;

                     internal static class Caller
                     {
                         public static void Wire(IHost host) =>
                             host.AddJob("Add", (int a, int b) => a + b)
                                 .OnSuccess((JobContext ctx) => System.Console.WriteLine(ctx.JobName));
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out _);
        var generated = result.GeneratedSource();

        Assert.Contains("OnSuccess_Cb_", generated);
        Assert.Contains("BuildCallbackDescriptor_", generated);
        Assert.Contains("Surefire.CallbackDescriptor", generated);
    }
}
