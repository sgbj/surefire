using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Surefire;

public static class HostExtensions
{
    public static JobBuilder AddJob(this IHost host, string name, Delegate handler)
    {
        var registry = host.Services.GetRequiredService<JobRegistry>();
        var options = host.Services.GetRequiredService<SurefireOptions>();

        var (compiledHandler, hasReturnValue) = ParameterBinder.Compile(handler, options.SerializerOptions);

        var registeredJob = new RegisteredJob
        {
            Definition = new JobDefinition { Name = name },
            CompiledHandler = compiledHandler,
            HasReturnValue = hasReturnValue
        };

        registry.Register(registeredJob);
        return new JobBuilder(registeredJob);
    }
}
