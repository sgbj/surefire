using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>
///     Source-generated JSON context for Surefire's internal wire-format types. Keeps
///     internal serialization AOT-safe without relying on the user-configurable
///     <c>SerializerOptions</c>. User payload serialization (job arguments, results)
///     remains reflection-based behind <c>[RequiresUnreferencedCode]</c>.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(LogEventPayload))]
[JsonSerializable(typeof(RunFailureEnvelope))]
[JsonSerializable(typeof(InputDeclarationEnvelope))]
[JsonSerializable(typeof(InputEnvelope))]
internal sealed partial class SurefireJsonContext : JsonSerializerContext;