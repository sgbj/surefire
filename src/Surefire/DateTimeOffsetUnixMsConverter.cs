using System.Text.Json;
using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>
///     Serializes <see cref="DateTimeOffset" /> as unix milliseconds. Used on
///     <see cref="SurefireJsonContext" /> so every store persists timestamps as a compact
///     integer that Lua scripts, SQL JSON primitives, and date-math comparisons can use
///     directly without string parsing. All <see cref="DateTimeOffset" /> values round-tripped
///     through the context are UTC-normalized with millisecond precision — matching the
///     resolution every persistence layer already stores.
/// </summary>
internal sealed class DateTimeOffsetUnixMsConverter : JsonConverter<DateTimeOffset>
{
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options) =>
        DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64());

    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options) =>
        writer.WriteNumberValue(value.ToUnixTimeMilliseconds());
}