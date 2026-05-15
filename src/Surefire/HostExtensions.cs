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
    [RequiresUnreferencedCode(
        "Registering a job from a Delegate inspects its parameters and return type via reflection. " +
        "Build a JobRegistrationDescriptor instead when publishing with trimming or Native AOT.")]
    [RequiresDynamicCode(
        "Registering a job from a Delegate creates closed-generic invocation glue at run time. " +
        "Build a JobRegistrationDescriptor instead when publishing with Native AOT.")]
    public static JobBuilder AddJob<TDelegate>(this IHost host, string name, TDelegate handler)
        where TDelegate : Delegate
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(handler);
        return host.AddJob(ReflectionDescriptorBuilder.BuildJob(name, handler));
    }

    /// <summary>
    ///     Registers or updates a named job from a pre-built <see cref="JobRegistrationDescriptor" />
    ///     and returns a fluent <see cref="JobBuilder" />.
    /// </summary>
    /// <param name="host">The host instance.</param>
    /// <param name="descriptor">The pre-built handler descriptor.</param>
    /// <returns>A fluent job builder for additional configuration.</returns>
    public static JobBuilder AddJob(this IHost host, JobRegistrationDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        var registry = host.Services.GetRequiredService<JobRegistry>();
        return registry.AddOrUpdate(descriptor, host.Services);
    }
}
