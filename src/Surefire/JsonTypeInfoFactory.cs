using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Surefire;

/// <summary>
///     Resolves a <see cref="JsonTypeInfo" /> against the runtime's
///     <see cref="JsonSerializerOptions" />. Used by descriptors to defer JSON metadata lookup
///     until <see cref="SurefireOptions.SerializerOptions" /> has been frozen with the resolver
///     chain in its final form.
/// </summary>
public delegate JsonTypeInfo? JsonTypeInfoFactory(JsonSerializerOptions options);
