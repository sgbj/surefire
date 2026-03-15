using System.Text.Json.Serialization;

namespace Surefire;

public sealed class PlanGraph
{
    public string Version { get; set; } = "1";
    public required List<PlanStep> Steps { get; set; }
    public string? OutputStepId { get; set; }
    public StepFailurePolicy FailurePolicy { get; set; } = StepFailurePolicy.CancelDependents;
}

[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(InputStep), "input")]
[JsonDerivedType(typeof(RunStep), "run")]
[JsonDerivedType(typeof(StreamStep), "stream")]
[JsonDerivedType(typeof(ForEachStep), "forEach")]
[JsonDerivedType(typeof(WhenAllStep), "whenAll")]
[JsonDerivedType(typeof(IfStep), "if")]
[JsonDerivedType(typeof(SwitchStep), "switch")]
[JsonDerivedType(typeof(SignalStep), "signal")]
[JsonDerivedType(typeof(OperatorStep), "operator")]
public abstract class PlanStep
{
    public required string Id { get; set; }
    public string? Name { get; set; }
    public List<string> DependsOn { get; set; } = [];
    public TriggerRule TriggerRule { get; set; } = TriggerRule.AllSuccess;
    public bool IsOptional { get; set; }
}

public sealed class InputStep : PlanStep
{
    public required string ParamName { get; set; }
}

public abstract class JobStep : PlanStep
{
    public required string JobName { get; set; }
    public Dictionary<string, StepValue>? Arguments { get; set; }
    public StepValue? PriorityOverride { get; set; }
    public StepValue? NotBeforeOverride { get; set; }
    public StepValue? DeduplicationIdOverride { get; set; }
}

public sealed class RunStep : JobStep
{
    public string? FallbackJobName { get; set; }
}

/// <summary>
/// A step that runs a streaming job. Distinct from <see cref="StreamStep{T}"/> (the user-facing handle type)
/// by generic arity — this is the graph model type with zero type parameters.
/// </summary>
public sealed class StreamStep : JobStep
{
}

public sealed class ForEachStep : PlanStep
{
    public required string SourceStepId { get; set; }
    public required PlanGraph Template { get; set; }
    public bool IsStreaming { get; set; }
}

public sealed class WhenAllStep : PlanStep
{
    public required List<string> SourceStepIds { get; set; }
}

public sealed class IfStep : PlanStep
{
    public required List<ConditionalBranch> Branches { get; set; }
    public PlanGraph? ElseBranch { get; set; }
}

public sealed class ConditionalBranch
{
    public required string ConditionStepId { get; set; }
    public required PlanGraph Branch { get; set; }
}

public sealed class SwitchStep : PlanStep
{
    public required string KeyStepId { get; set; }
    public required List<SwitchCase> Cases { get; set; }
    public PlanGraph? DefaultBranch { get; set; }
}

public sealed class SwitchCase
{
    public required string Value { get; set; }
    public required PlanGraph Branch { get; set; }
}

public sealed class SignalStep : PlanStep
{
    public required string SignalName { get; set; }
}

public sealed class OperatorStep : PlanStep
{
    public required string OperatorType { get; set; }
    public required string SourceStepId { get; set; }
    public OperatorExpression? Expression { get; set; }
}
