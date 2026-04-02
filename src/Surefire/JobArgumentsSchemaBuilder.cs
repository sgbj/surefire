using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Schema;
using System.Text.Json.Serialization.Metadata;

namespace Surefire;

internal static class JobArgumentsSchemaBuilder
{
    private static readonly JsonSerializerOptions SchemaSerializerOptions = new()
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver()
    };

    public static string? Build(Delegate handler, Func<Type, bool> isServiceType)
    {
        var nullabilityContext = new NullabilityInfoContext();
        var method = handler.Method;
        var parameters = method.GetParameters()
            .Where(p => !IsFrameworkParameter(p.ParameterType) && !isServiceType(p.ParameterType))
            .ToArray();

        if (parameters.Length == 0)
        {
            return null;
        }

        var properties = new JsonObject();
        var required = new List<string>();

        foreach (var parameter in parameters)
        {
            var name = parameter.Name;
            if (string.IsNullOrWhiteSpace(name))
            {
                continue;
            }

            properties[name] = BuildSchemaForType(parameter.ParameterType);
            if (IsRequiredParameter(parameter, nullabilityContext))
            {
                required.Add(name);
            }
        }

        if (properties.Count == 0)
        {
            return null;
        }

        var schema = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = properties
        };

        if (required.Count > 0)
        {
            var requiredArray = new JsonArray();
            foreach (var name in required)
            {
                requiredArray.Add(name);
            }

            schema["required"] = requiredArray;
        }

        return schema.ToJsonString();
    }

    private static bool IsFrameworkParameter(Type type) =>
        type == typeof(JobContext) || type == typeof(CancellationToken);

    private static bool IsRequiredParameter(ParameterInfo parameter, NullabilityInfoContext nullabilityContext)
    {
        if (parameter.HasDefaultValue)
        {
            return false;
        }

        var type = parameter.ParameterType;
        if (!type.IsValueType)
        {
            return nullabilityContext
                .Create(parameter)
                .WriteState == NullabilityState.NotNull;
        }

        return Nullable.GetUnderlyingType(type) is null;
    }

    private static JsonNode BuildSchemaForType(Type type)
    {
        var targetType = Nullable.GetUnderlyingType(type) ?? type;
        return SchemaSerializerOptions.GetJsonSchemaAsNode(targetType,
            new());
    }
}