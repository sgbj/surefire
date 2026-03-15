using System.Text.Json;
using System.Text.Json.Serialization;

namespace Surefire;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(StepRefValue), "ref")]
[JsonDerivedType(typeof(StreamRefValue), "stream")]
[JsonDerivedType(typeof(ItemValue), "item")]
[JsonDerivedType(typeof(ConstantValue), "const")]
public abstract class StepValue { }

public sealed class StepRefValue : StepValue
{
    public required string StepId { get; set; }
}

public sealed class StreamRefValue : StepValue
{
    public required string StepId { get; set; }
}

public sealed class ItemValue : StepValue { }

public sealed class ConstantValue : StepValue
{
    public required JsonElement Value { get; set; }
}
