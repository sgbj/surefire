using System.Reflection;
using System.Text.Json;

namespace Surefire;

public sealed class PlanBuilder
{
    private readonly List<PlanStep> _steps = [];
    private int _stepCounter;
    private StepFailurePolicy _failurePolicy = StepFailurePolicy.CancelDependents;

    internal string NextStepId() => $"step_{_stepCounter++}";

    internal void AddStep(PlanStep step) => _steps.Add(step);

    public Step<T> Run<T>(string jobName, object? args = null)
    {
        var planStep = AddJobStep(jobName, args, (id, name, arguments) => new RunStep { Id = id, JobName = name, Arguments = arguments });
        return new Step<T>(this, planStep);
    }

    public Step Run(string jobName, object? args = null)
    {
        var planStep = AddJobStep(jobName, args, (id, name, arguments) => new RunStep { Id = id, JobName = name, Arguments = arguments });
        return new Step(this, planStep);
    }

    public StreamStep<T> Stream<T>(string jobName, object? args = null)
    {
        var planStep = AddJobStep(jobName, args, (id, name, arguments) => new Surefire.StreamStep { Id = id, JobName = name, Arguments = arguments });
        return new StreamStep<T>(this, planStep);
    }

    public Step<T[]> WhenAll<T>(params Step<T>[] steps)
    {
        var sourceIds = steps.Select(s => s.InternalStep.Id).ToList();
        var planStep = new WhenAllStep
        {
            Id = NextStepId(),
            SourceStepIds = sourceIds
        };
        planStep.DependsOn.AddRange(sourceIds);

        AddStep(planStep);
        return new Step<T[]>(this, planStep);
    }

    public Step<T[]> WhenAll<T>(IEnumerable<Step<T>> steps)
        => WhenAll(steps.ToArray());

    // -- If/Switch --

    public IfBuilder If(Step<bool> condition, Action<PlanBuilder> body)
        => new(this, condition, body);

    public IfBuilder<T> If<T>(Step<bool> condition, Func<PlanBuilder, Step<T>> body)
        => new(this, condition, body);

    public SwitchBuilder Switch(Step<string> key)
        => new(this, key);

    public SwitchBuilder<T> Switch<T>(Step<string> key)
        => new(this, key);

    // -- Signals --

    public Step<T> WaitForSignal<T>(string signalName)
    {
        var signalStep = new SignalStep
        {
            Id = NextStepId(),
            SignalName = signalName
        };
        AddStep(signalStep);
        return new Step<T>(this, signalStep);
    }

    public void WithFailurePolicy(StepFailurePolicy policy)
        => _failurePolicy = policy;

    internal PlanGraph Build(string? outputStepId = null)
    {
        var hasV2Features = _steps.Any(s => s is IfStep or SwitchStep or SignalStep or OperatorStep);
        return new PlanGraph
        {
            Version = hasV2Features ? "2" : "1",
            Steps = [.. _steps],
            OutputStepId = outputStepId,
            FailurePolicy = _failurePolicy
        };
    }

    private TStep AddJobStep<TStep>(string jobName, object? args,
        Func<string, string, Dictionary<string, StepValue>?, TStep> factory) where TStep : JobStep
    {
        var (arguments, deps) = ExtractArguments(args);
        var planStep = factory(NextStepId(), jobName, arguments);
        foreach (var dep in deps)
        {
            if (!planStep.DependsOn.Contains(dep))
                planStep.DependsOn.Add(dep);
        }
        _steps.Add(planStep);
        return planStep;
    }

    // Argument extraction — detects Step<T> handles and creates StepValue references

    internal static (Dictionary<string, StepValue>? arguments, List<string> deps) ExtractArguments(object? args)
    {
        if (args is null) return (null, []);

        var arguments = new Dictionary<string, StepValue>();
        var deps = new List<string>();

        foreach (var prop in args.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var value = prop.GetValue(args);
            var propName = prop.Name;

            if (value is Step stepHandle)
            {
                if (stepHandle.IsItem)
                {
                    arguments[propName] = new ItemValue();
                }
                else if (stepHandle.IsStream)
                {
                    arguments[propName] = new StreamRefValue { StepId = stepHandle.InternalStep.Id };
                    deps.Add(stepHandle.InternalStep.Id);
                }
                else
                {
                    arguments[propName] = new StepRefValue { StepId = stepHandle.InternalStep.Id };
                    deps.Add(stepHandle.InternalStep.Id);
                }
            }
            else
            {
                var jsonValue = JsonSerializer.SerializeToElement(value);
                arguments[propName] = new ConstantValue { Value = jsonValue };
            }
        }

        return (arguments.Count > 0 ? arguments : null, deps);
    }
}
