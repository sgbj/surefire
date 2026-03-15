using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Surefire;

public static class HostExtensions
{
    public static JobBuilder AddJob(this IHost host, string name, Delegate handler)
    {
        var registry = host.Services.GetRequiredService<JobRegistry>();
        var options = host.Services.GetRequiredService<SurefireOptions>();

        var result = ParameterBinder.Compile(handler, options.SerializerOptions);
        var schema = ArgumentSchemaGenerator.Generate(handler, host.Services, options.SerializerOptions);

        var registeredJob = new RegisteredJob
        {
            Definition = new JobDefinition { Name = name, ArgumentsSchema = schema },
            CompiledHandler = result.Handler,
            HasReturnValue = result.HasReturnValue
        };

        registry.Register(registeredJob);
        return new JobBuilder(registeredJob);
    }

    public static JobBuilder AddPlan(this IHost host, string name, Delegate planBuilder)
    {
        var registry = host.Services.GetRequiredService<JobRegistry>();
        var options = host.Services.GetRequiredService<SurefireOptions>();

        // Build the plan graph at registration time
        var graph = BuildPlanGraph(planBuilder, options.SerializerOptions);
        var graphJson = JsonSerializer.Serialize(graph, options.SerializerOptions);
        var schema = ArgumentSchemaGenerator.GenerateForPlan(planBuilder, options.SerializerOptions);

        // Create a no-op handler since plan execution is driven by the engine, not a handler
        var registeredJob = new RegisteredJob
        {
            Definition = new JobDefinition
            {
                Name = name,
                IsPlan = true,
                PlanGraph = graphJson,
                ArgumentsSchema = schema
            },
            CompiledHandler = (_, _, _) => Task.FromResult<object?>(null),
            HasReturnValue = false
        };

        registry.Register(registeredJob);
        return new JobBuilder(registeredJob);
    }

    private static PlanGraph BuildPlanGraph(Delegate planBuilder, JsonSerializerOptions serializerOptions)
    {
        var method = planBuilder.Method;
        var parameters = method.GetParameters();
        var target = planBuilder.Target;

        var plan = new PlanBuilder();
        var args = new object?[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var param = parameters[i];
            var paramType = param.ParameterType;

            if (paramType == typeof(PlanBuilder))
            {
                args[i] = plan;
            }
            else if (IsStepType(paramType, out var itemType, out var isStream))
            {
                // Create an InputStep for this parameter
                var inputStep = new InputStep
                {
                    Id = $"input_{param.Name}",
                    ParamName = param.Name!
                };
                plan.AddStep(inputStep);

                // Create the matching step handle type (Step<T> or StreamStep<T>)
                var stepType = (isStream ? typeof(StreamStep<>) : typeof(Step<>)).MakeGenericType(itemType!);
                args[i] = Activator.CreateInstance(
                    stepType,
                    BindingFlags.NonPublic | BindingFlags.Instance,
                    null,
                    [plan, inputStep],
                    null);
            }
            else
            {
                throw new InvalidOperationException(
                    $"AddPlan delegate parameter '{param.Name}' has unsupported type '{paramType.Name}'. " +
                    "Only PlanBuilder, Step<T>, and StreamStep<T> parameters are supported.");
            }
        }

        var result = method.Invoke(target, args);

        // If the delegate returns a Step, that's the output step
        string? outputStepId = null;
        if (result is Step outputStep)
            outputStepId = outputStep.InternalStep.Id;

        return plan.Build(outputStepId);
    }

    internal static bool IsStepType(Type type, out Type? itemType, out bool isStream)
    {
        itemType = null;
        isStream = false;

        if (!type.IsGenericType) return false;

        var genericDef = type.GetGenericTypeDefinition();
        if (genericDef == typeof(Step<>))
        {
            itemType = type.GetGenericArguments()[0];
            return true;
        }
        if (genericDef == typeof(StreamStep<>))
        {
            itemType = type.GetGenericArguments()[0];
            isStream = true;
            return true;
        }

        return false;
    }
}
