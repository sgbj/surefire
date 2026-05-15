using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Surefire.Tests.SourceGeneration;

public sealed class GeneratorCoverageTests
{
    [Theory]
    [InlineData("(ref int x) => x")]
    [InlineData("(in int x) => x")]
    [InlineData("(out int x) => { x = 0; }")]
    public void AddJob_ByRefParameter_DoesNotEmitInterceptor(string handler)
    {
        var source = $$"""
                       using Microsoft.Extensions.Hosting;

                       internal static class Caller
                       {
                           public static void Wire(IHost host) => host.AddJob("Ref", {{handler}});
                       }
                       """;

        var result = GeneratorDriverHarness.Run(source, out _);
        Assert.DoesNotContain("BuildDescriptor_", result.GeneratedSource());
    }

    [Fact]
    public void AddJob_ConditionalAccessOnHost_EmitsInterceptorAndCompilesClean()
    {
        var source = """
                     using Microsoft.Extensions.Hosting;

                     internal static class Caller
                     {
                         public static void Wire(IHost? host) => host?.AddJob("Add", (int a, int b) => a + b);
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out var compilation);
        var generated = result.GeneratedSource();

        Assert.Contains("BuildDescriptor_", generated);
        Assert.Contains("InterceptsLocation", generated);
        AssertNoCompilationErrors(compilation);
    }

    [Fact]
    public void AddJob_DefaultValuedParameter_EmitsHasDefaultAndLiteral()
    {
        var source = """
                     using Microsoft.Extensions.Hosting;

                     internal static class Caller
                     {
                         public static void Wire(IHost host) =>
                             host.AddJob("Greet", (string name = "world") => name);
                     }
                     """;

        var result = GeneratorDriverHarness.Run(source, out var compilation);
        var generated = result.GeneratedSource();

        Assert.Contains("HasDefault: true", generated);
        Assert.Contains("DefaultValue: \"world\"", generated);
        AssertNoCompilationErrors(compilation);
    }

    [Fact]
    public void GeneratedSource_AlwaysCompilesClean()
    {
        var source = """
                     using System.Collections.Generic;
                     using Surefire;
                     using Microsoft.Extensions.Hosting;

                     internal static class Caller
                     {
                         public static void Wire(IHost host) =>
                             host.AddJob("Sum", async (IAsyncEnumerable<int> values) =>
                             {
                                 var total = 0;
                                 await foreach (var v in values) total += v;
                                 return total;
                             });

                         public static async System.Threading.Tasks.Task Trigger(IJobClient client)
                         {
                             await client.RunAsync<int>("Sum", new { values = new[] { 1, 2, 3 } });
                         }
                     }
                     """;

        GeneratorDriverHarness.Run(source, out var compilation);
        AssertNoCompilationErrors(compilation);
    }

    [Fact]
    public void Generator_IdenticalInput_FinalOutputUnchanged()
    {
        var source = """
                     using Microsoft.Extensions.Hosting;

                     internal static class Caller
                     {
                         public static void Wire(IHost host) => host.AddJob("Add", (int a, int b) => a + b);
                     }
                     """;

        // Fresh Compilation each pass, so Roslyn's input nodes (Compilation, ParseOptions, etc.)
        // always report Modified. What matters is the generator's own outputs: these flow
        // through Select/Where/Collect/Combine and must yield value-equal results so the final
        // source emission is reused. A regression that breaks record equality on AddJobCall,
        // HandlerSignature / EquatableArray would flip these outputs back to Modified.
        var (_, second) = GeneratorDriverHarness.RunIncremental(source, source);

        var rerunOutputs = second.Results
            .SelectMany(r => r.TrackedOutputSteps)
            .SelectMany(kvp => kvp.Value.SelectMany(step => step.Outputs.Select(o => (kvp.Key, o.Reason))))
            .Where(o => o.Reason is not IncrementalStepRunReason.Cached
                and not IncrementalStepRunReason.Unchanged)
            .ToArray();

        Assert.True(rerunOutputs.Length == 0,
            "Expected generator output steps Cached/Unchanged on identical re-run, but got: "
            + string.Join(", ", rerunOutputs.Select(o => $"{o.Key}={o.Reason}")));
    }

    private static void AssertNoCompilationErrors(CSharpCompilation compilation)
    {
        var errors = compilation.GetDiagnostics(TestContext.Current.CancellationToken)
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Select(d => d.ToString())
            .ToArray();
        Assert.True(errors.Length == 0, string.Join(Environment.NewLine, errors));
    }
}
