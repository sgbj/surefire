using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Surefire;

/// <summary>Represents a single item in a batch job submission.</summary>
/// <param name="JobName">The name of the job to run.</param>
/// <param name="Args">Pre-serialized arguments, or <c>null</c> for none.</param>
/// <param name="Options">Optional per-item run options.</param>
public record BatchItem(string JobName, RunArguments? Args = null, BatchRunOptions? Options = null)
{
    /// <summary>
    ///     Builds a <see cref="BatchItem" /> from an arbitrary <paramref name="args" /> object,
    ///     serializing it through the runtime's configured <see cref="JsonSerializerOptions" />.
    /// </summary>
    /// <remarks>
    ///     The Surefire source generator intercepts this factory and replaces it with a strongly
    ///     typed construction when <paramref name="args" /> is statically known to be <c>null</c>,
    ///     a <see cref="RunArguments" /> instance, or an anonymous type. Other shapes fall back to
    ///     a reflective serializer at trigger time, which is why this overload carries
    ///     <see cref="RequiresUnreferencedCodeAttribute" />. For native AOT or trim-published apps,
    ///     either let the generator handle the call or construct <see cref="BatchItem" /> directly
    ///     with a pre-built <see cref="RunArguments" /> payload.
    /// </remarks>
    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public static BatchItem Create(string jobName, object? args, BatchRunOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobName);

        if (args is null)
        {
            return new(jobName, null, options);
        }

        if (args is RunArguments preBuilt)
        {
            return new(jobName, preBuilt, options);
        }

        var capturedArgs = args;
        var capturedType = args.GetType();
        return new(jobName, new()
        {
            WriteJson = (opts, writer) =>
            {
                var typeInfo = opts.GetTypeInfo(capturedType);
                JsonSerializer.Serialize(writer, capturedArgs, typeInfo);
            }
        }, options);
    }
}
