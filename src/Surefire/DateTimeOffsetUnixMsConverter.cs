using System.Text.Json;
using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>
///     Serializes <see cref="DateTimeOffset" /> as unix milliseconds (UTC-normalized) so every
///     store persists a compact integer that Lua scripts, SQL JSON primitives, and date-math
///     comparisons can use directly without string parsing.
/// </summary>
internal sealed class DateTimeOffsetUnixMsConverter : JsonConverter<DateTimeOffset>
{
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options) =>
        DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64());

    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options) =>
        writer.WriteNumberValue(value.ToUnixTimeMilliseconds());
}
