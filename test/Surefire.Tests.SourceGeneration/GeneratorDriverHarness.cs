using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Surefire.SourceGeneration;

namespace Surefire.Tests.SourceGeneration;

/// <summary>
///     Compiles a source string against the Surefire library and runs the
///     <see cref="Surefire.SourceGeneration.SurefireGenerator" /> over it. Tests inspect the
///     resulting <see cref="GeneratorDriverRunResult" /> for emitted source, diagnostics, and
///     end-to-end <see cref="CSharpCompilation.GetDiagnostics" />.
/// </summary>
internal static class GeneratorDriverHarness
{
    private static readonly IReadOnlyList<MetadataReference> ReferenceAssemblies = BuildReferenceAssemblies();

    public static GeneratorDriverRunResult Run(string source, out CSharpCompilation compilation)
    {
        var inputCompilation = CreateCompilation(source);
        var driver = CreateDriver();
        driver = (CSharpGeneratorDriver)driver.RunGeneratorsAndUpdateCompilation(inputCompilation,
            out var outputCompilation, out _);
        compilation = (CSharpCompilation)outputCompilation;
        return driver.GetRunResult();
    }

    /// <summary>
    ///     Runs the same driver instance over two compilations so callers can inspect which
    ///     incremental pipeline steps reran versus reused their cached outputs on the second pass.
    /// </summary>
    public static (GeneratorDriverRunResult First, GeneratorDriverRunResult Second) RunIncremental(
        string firstSource, string secondSource)
    {
        var driver = CreateDriver();
        driver = (CSharpGeneratorDriver)driver.RunGenerators(CreateCompilation(firstSource));
        var first = driver.GetRunResult();
        driver = (CSharpGeneratorDriver)driver.RunGenerators(CreateCompilation(secondSource));
        return (first, driver.GetRunResult());
    }

    private static CSharpParseOptions CreateParseOptions() =>
        new CSharpParseOptions(LanguageVersion.Preview)
            .WithFeatures([new("InterceptorsNamespaces", "Surefire.Generated")]);

    private static CSharpCompilation CreateCompilation(string source)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(source, CreateParseOptions());
        return CSharpCompilation.Create(
            "Surefire.GeneratorHostTest",
            [syntaxTree],
            ReferenceAssemblies,
            new(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
    }

    private static CSharpGeneratorDriver CreateDriver()
    {
        var generator = new SurefireGenerator().AsSourceGenerator();
        return CSharpGeneratorDriver.Create(
            [generator],
            parseOptions: CreateParseOptions(),
            optionsProvider: new HostAnalyzerConfigOptionsProvider(new Dictionary<string, string>
            {
                ["build_property.SurefireSourceGenerationEnabled"] = "true"
            }),
            driverOptions: new(IncrementalGeneratorOutputKind.None, true));
    }

    /// <summary>Returns the concatenated generated source for inspection.</summary>
    public static string GeneratedSource(this GeneratorDriverRunResult result) =>
        string.Join(Environment.NewLine,
            result.Results.SelectMany(r => r.GeneratedSources).Select(s => s.SourceText.ToString()));

    private static IReadOnlyList<MetadataReference> BuildReferenceAssemblies()
    {
        var trustedAssembliesPaths = (AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") as string
                                      ?? throw new InvalidOperationException(
                                          "Generator harness requires the host runtime's TRUSTED_PLATFORM_ASSEMBLIES list."))
            .Split(Path.PathSeparator);

        var allowedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // Core BCL + System types Surefire depends on
            "System.Runtime",
            "System.Private.CoreLib",
            "System.Console",
            "System.Collections",
            "System.Collections.Concurrent",
            "System.Collections.Immutable",
            "System.Linq",
            "System.Linq.Async",
            "System.Threading",
            "System.Threading.Channels",
            "System.Threading.Tasks",
            "System.Threading.Tasks.Extensions",
            "System.Runtime.Extensions",
            "System.ObjectModel",
            "System.ComponentModel",
            "System.Text.Json",
            "System.Text.Encodings.Web",
            "System.Diagnostics.DiagnosticSource",
            "System.Memory",
            "Microsoft.Extensions.Hosting.Abstractions",
            "Microsoft.Extensions.DependencyInjection.Abstractions",
            "Microsoft.Extensions.DependencyInjection",
            "Microsoft.Extensions.Logging.Abstractions",
            "Microsoft.Extensions.Options",
            "Microsoft.Extensions.Configuration.Abstractions",
            "netstandard"
        };

        var references = new List<MetadataReference>(allowedNames.Count + 1);
        foreach (var path in trustedAssembliesPaths)
        {
            var name = Path.GetFileNameWithoutExtension(path);
            if (allowedNames.Contains(name))
            {
                references.Add(MetadataReference.CreateFromFile(path));
            }
        }

        // Surefire itself: load it via the test runtime so the source generator sees the real
        // public surface (IJobClient, RunArguments, AddJob, etc.).
        references.Add(MetadataReference.CreateFromFile(typeof(IJobClient).Assembly.Location));
        return references;
    }

    private sealed class HostAnalyzerConfigOptionsProvider : AnalyzerConfigOptionsProvider
    {
        public HostAnalyzerConfigOptionsProvider(IReadOnlyDictionary<string, string> globalOptions) =>
            GlobalOptions = new SimpleAnalyzerConfigOptions(globalOptions);

        public override AnalyzerConfigOptions GlobalOptions { get; }

        public override AnalyzerConfigOptions GetOptions(SyntaxTree tree) => GlobalOptions;

        public override AnalyzerConfigOptions GetOptions(AdditionalText textFile) => GlobalOptions;

        private sealed class SimpleAnalyzerConfigOptions(IReadOnlyDictionary<string, string> options)
            : AnalyzerConfigOptions
        {
            public override bool TryGetValue(string key, out string value)
            {
                if (options.TryGetValue(key, out var v))
                {
                    value = v;
                    return true;
                }

                value = string.Empty;
                return false;
            }
        }
    }
}
