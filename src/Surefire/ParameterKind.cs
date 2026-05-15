namespace Surefire;

/// <summary>
///     Describes how a job handler parameter is sourced at run time. The runtime uses this to bind
///     arguments without inspecting <see cref="System.Reflection.ParameterInfo" /> on each
///     invocation.
/// </summary>
public enum ParameterKind
{
    /// <summary>The parameter is bound to the <see cref="JobContext" /> for the current run.</summary>
    JobContext,

    /// <summary>The parameter is bound to the run's <see cref="System.Threading.CancellationToken" />.</summary>
    CancellationToken,

    /// <summary>The parameter is bound to the executing scope's <see cref="System.IServiceProvider" />.</summary>
    ServiceProvider,

    /// <summary>
    ///     The parameter is bound from the dependency-injection scope. The generator could prove the
    ///     type is DI-registered at compile time, so the runtime resolves it without consulting
    ///     <see cref="Microsoft.Extensions.DependencyInjection.IServiceProviderIsService" />.
    /// </summary>
    Service,

    /// <summary>
    ///     The parameter's binding source is decided at run time by
    ///     <see cref="Microsoft.Extensions.DependencyInjection.IServiceProviderIsService" />: if the
    ///     type is a registered service, it is resolved from DI; otherwise it is deserialized from the
    ///     run's argument JSON. Mirrors the Minimal-APIs unattributed-parameter fallback.
    /// </summary>
    ServiceOrJson,

    /// <summary>
    ///     The parameter is deserialized from the run's argument JSON using a
    ///     <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo" /> resolved through
    ///     <see cref="SurefireOptions.SerializerOptions" />.
    /// </summary>
    Json,

    /// <summary>
    ///     The parameter consumes an input stream declared on the run. Supported shapes are
    ///     <see cref="System.Collections.Generic.IAsyncEnumerable{T}" />,
    ///     <see cref="System.Collections.Generic.List{T}" />, and <c>T[]</c>.
    /// </summary>
    Stream
}
