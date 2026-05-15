using System.Text.Json;
using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>
///     Serializes <see cref="TimeSpan" /> as a JSON number of ticks.
/// </summary>
internal sealed class TimeSpanTicksConverter : JsonConverter<TimeSpan>
{
    public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        TimeSpan.FromTicks(reader.GetInt64());

    public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options) =>
        writer.WriteNumberValue(value.Ticks);
}
