using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Surefire;

internal static class ResultSerializer
{
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public static T Deserialize<T>(string json, JsonSerializerOptions? serializerOptions) =>
        JsonSerializer.Deserialize<T>(json, serializerOptions)!;
}
