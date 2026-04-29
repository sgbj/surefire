using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Surefire;

namespace Microsoft.Extensions.Hosting;

/// <summary>
///     Extension methods for registering jobs on a built host.
/// </summary>
public static class HostExtensions
{
    /// <summary>
    ///     Registers or updates a named job and returns a fluent <see cref="JobBuilder" />.
    /// </summary>
    /// <typeparam name="TDelegate">The handler delegate type.</typeparam>
    /// <param name="host">The host instance.</param>
    /// <param name="name">The job name.</param>
    /// <param name="handler">The job handler delegate.</param>
    /// <returns>A fluent job builder for additional configuration.</returns>
    /// <remarks>
    ///     The handler delegate is reflected over and an Expression is compiled at registration
    ///     time. A planned source generator will remove this reflection requirement.
    /// </remarks>
    [RequiresUnreferencedCode("Reflects over a user-supplied handler delegate to build job metadata.")]
    [RequiresDynamicCode("Reflects over a user-supplied handler delegate to build job metadata.")]
    public static JobBuilder AddJob<TDelegate>(this IHost host, string name, TDelegate handler)
        where TDelegate : Delegate
    {
        var registry = host.Services.GetRequiredService<JobRegistry>();
        return registry.AddOrUpdate(name, handler, host.Services);
    }
}
