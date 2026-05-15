using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Schema;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

internal static class JobArgumentsSchemaBuilder
{
    /// <summary>
    ///     Builds the JSON schema from a descriptor's per-parameter <see cref="JsonTypeInfo" /> via
    ///     <see cref="JsonSchemaExporter.GetJsonSchemaAsNode(JsonTypeInfo, JsonSchemaExporterOptions?)" />.
    ///     AOT-safe when the descriptor's factories return type info from a <c>JsonSerializerContext</c>.
    /// </summary>
    public static string? BuildFromDescriptor(
        IReadOnlyList<ParameterDescriptor> parameters,
        IReadOnlyList<JsonTypeInfoFactory?>? parameterTypeInfoFactories,
        JsonSerializerOptions serializerOptions,
        IServiceProvider services)
    {
        var serviceChecker = services.GetService<IServiceProviderIsService>();
        var properties = new JsonObject();
        var required = new List<string>();

        // Schema describes the structural shape of arguments, not deserialization leniency.
        // Strip NumberHandling.AllowReadingFromString so an int parameter exports as
        // `{"type":"integer"}` rather than `{"type":["integer","string"]}` even when the runtime
        // accepts both.
        var schemaOptions = new JsonSerializerOptions(serializerOptions)
        {
            NumberHandling = JsonNumberHandling.Strict
        };

        for (var i = 0; i < parameters.Count; i++)
        {
            var parameter = parameters[i];
            if (!ShouldIncludeInSchema(parameter, serviceChecker))
            {
                continue;
            }

            var typeInfo = parameterTypeInfoFactories?[i]?.Invoke(schemaOptions);
            if (typeInfo is null)
            {
                continue;
            }

            properties[parameter.Name] = typeInfo.GetJsonSchemaAsNode();
            if (!parameter.HasDefault && !parameter.IsNullable)
            {
                required.Add(parameter.Name);
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
                // Cast picks the non-generic Add(JsonNode?) overload, which is AOT-safe; the
                // generic Add<T>(T) overload would be picked otherwise and is annotated
                // RequiresUnreferencedCode for non-primitive T.
                requiredArray.Add((JsonNode?)JsonValue.Create(name));
            }

            schema["required"] = requiredArray;
        }

        return schema.ToJsonString();
    }

    private static bool ShouldIncludeInSchema(
        ParameterDescriptor parameter,
        IServiceProviderIsService? serviceChecker)
    {
        switch (parameter.Kind)
        {
            case ParameterKind.JobContext:
            case ParameterKind.CancellationToken:
            case ParameterKind.ServiceProvider:
            case ParameterKind.Service:
            case ParameterKind.Stream:
                return false;
            case ParameterKind.ServiceOrJson:
                // Generator classified the type as DI-or-JSON because it couldn't statically
                // distinguish; ask the container at registration time which it is, matching the
                // Minimal-APIs unattributed-parameter contract.
                return !(serviceChecker?.IsService(parameter.Type) ?? false);
            case ParameterKind.Json:
            default:
                return true;
        }
    }
}
