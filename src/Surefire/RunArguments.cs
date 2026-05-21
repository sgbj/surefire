using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Surefire;

/// <summary>
///     Arguments for a job run, prepared for transport to the executor.
/// </summary>
/// <remarks>
///     <para>
///         Either <see cref="Json" /> or <see cref="WriteJson" /> describes the non-stream
///         arguments. <see cref="Json" /> is set when the caller already has a serialized payload;
///         <see cref="WriteJson" /> defers serialization until the runtime supplies its configured
///         <see cref="JsonSerializerOptions" />.
///     </para>
///     <para>
///         <see cref="Streams" /> enumerates each <see cref="IAsyncEnumerable{T}" /> argument as a
///         <see cref="RunArgumentStream" /> whose items are serialized lazily.
///     </para>
/// </remarks>
public sealed class RunArguments
{
    /// <summary>An empty argument set: no JSON payload, no streams.</summary>
    public static RunArguments Empty { get; } = new();

    /// <summary>
    ///     The pre-serialized JSON payload for non-stream arguments, or <c>null</c> when none.
    ///     Ignored when <see cref="WriteJson" /> is non-null.
    /// </summary>
    [StringSyntax(StringSyntaxAttribute.Json)]
    public string? Json { get; init; }

    /// <summary>
    ///     A deferred JSON writer invoked with the runtime's <see cref="JsonSerializerOptions" />
    ///     and a <see cref="Utf8JsonWriter" />. The callback writes the complete JSON value
    ///     (including its own <c>WriteStartObject</c>/<c>WriteEndObject</c>) and is used in place
    ///     of <see cref="Json" /> when serialization needs the runtime's resolver chain.
    /// </summary>
    public Action<JsonSerializerOptions, Utf8JsonWriter>? WriteJson { get; init; }

    /// <summary>The streaming arguments declared on this run. Empty when there are none.</summary>
    public IReadOnlyList<RunArgumentStream> Streams { get; init; } = [];
}

/// <summary>
///     A single streaming argument on a run, materialized lazily once the runtime supplies its
///     configured <see cref="JsonSerializerOptions" />.
/// </summary>
public sealed class RunArgumentStream
{
    /// <summary>
    ///     The argument name this stream binds to, matching the corresponding handler parameter
    ///     (or <c>"$root"</c> when the entire args object is itself an
    ///     <see cref="IAsyncEnumerable{T}" />).
    /// </summary>
    public required string ArgumentName { get; init; }

    /// <summary>
    ///     Produces the serialized item stream once invoked with the runtime's configured
    ///     <see cref="JsonSerializerOptions" />. Each yielded element is a JSON-encoded
    ///     representation of one item from the source <see cref="IAsyncEnumerable{T}" />.
    ///     Mirrors <see cref="RunArguments.WriteJson" />'s deferred-options pattern so callers
    ///     don't need to capture serializer state at trigger time.
    /// </summary>
    public required Func<JsonSerializerOptions, IAsyncEnumerable<string>> SerializeItems { get; init; }

    // Replay resume marker (internal): when a durable run is replayed and its prior pump didn't
    // emit InputComplete for this argument, the client wraps the original stream and supplies
    // the recorded LastSequence here so the pump skips already-written items and continues
    // numbering past LastSequence. Zero on first-trigger.
    internal long ResumeFromSequence { get; init; }
}
