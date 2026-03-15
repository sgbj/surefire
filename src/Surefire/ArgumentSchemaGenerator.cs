using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Schema;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

internal static class ArgumentSchemaGenerator
{
    private static readonly JsonSchemaExporterOptions ExporterOptions = new()
    {
        TreatNullObliviousAsNonNullable = true
    };

    /// <summary>
    /// Generates a JSON Schema object describing the argument parameters of a job handler.
    /// Returns null if the handler has no argument parameters (only DI services, JobContext, CancellationToken, etc.).
    /// </summary>
    public static string? Generate(Delegate handler, IServiceProvider serviceProvider, JsonSerializerOptions serializerOptions)
    {
        var method = handler.Method;
        var parameters = method.GetParameters();
        var isService = serviceProvider.GetRequiredService<IServiceProviderIsService>();

        var properties = new JsonObject();
        var required = new JsonArray();

        foreach (var param in parameters)
        {
            var type = param.ParameterType;

            // Skip infrastructure parameters
            if (type == typeof(CancellationToken)) continue;
            if (type == typeof(JobContext)) continue;
            if (isService.IsService(type)) continue;
            if (StreamingHelper.IsAsyncEnumerable(type)) continue;

            properties[param.Name!] = JsonSchemaExporter.GetJsonSchemaAsNode(serializerOptions, type, ExporterOptions);

            if (!param.HasDefaultValue)
                required.Add(param.Name!);
        }

        return BuildSchema(properties, required, serializerOptions);
    }

    /// <summary>
    /// Generates a JSON Schema object describing the input parameters of a plan (Step&lt;T&gt; and StreamStep&lt;T&gt; params).
    /// Returns null if the plan has no input parameters.
    /// </summary>
    public static string? GenerateForPlan(Delegate planBuilder, JsonSerializerOptions serializerOptions)
    {
        var parameters = planBuilder.Method.GetParameters();

        var properties = new JsonObject();
        var required = new JsonArray();

        foreach (var param in parameters)
        {
            var paramType = param.ParameterType;

            // Skip PlanBuilder — it's infrastructure, not an input
            if (paramType == typeof(PlanBuilder)) continue;

            // Extract the T from Step<T> or StreamStep<T>
            if (!HostExtensions.IsStepType(paramType, out var itemType, out _) || itemType is null)
                continue;

            properties[param.Name!] = JsonSchemaExporter.GetJsonSchemaAsNode(serializerOptions, itemType, ExporterOptions);
            required.Add(param.Name!);
        }

        return BuildSchema(properties, required, serializerOptions);
    }

    private static string? BuildSchema(JsonObject properties, JsonArray required, JsonSerializerOptions serializerOptions)
    {
        if (properties.Count == 0)
            return null;

        var schema = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = properties
        };

        if (required.Count > 0)
            schema["required"] = required;

        schema["additionalProperties"] = false;

        return schema.ToJsonString(serializerOptions);
    }
}
