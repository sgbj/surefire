using System.Text.Json;
using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>
///     Serializes <see cref="TimeSpan" /> as a JSON number of ticks. Used on
///     <see cref="SurefireJsonContext" /> so every store persists durations as a compact,
///     lossless integer and the JSON payloads remain legible in Lua / SQL JSON primitives
///     that don't understand ISO-8601 strings.
/// </summary>
internal sealed class TimeSpanTicksConverter : JsonConverter<TimeSpan>
{
    public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        TimeSpan.FromTicks(reader.GetInt64());

    public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options) =>
        writer.WriteNumberValue(value.Ticks);
}