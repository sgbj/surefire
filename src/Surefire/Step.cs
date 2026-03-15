using System.Text.Json;

namespace Surefire;

public class Step
{
    internal PlanStep InternalStep { get; }
    internal PlanBuilder Builder { get; }
    internal virtual bool IsStream => false;
    internal bool IsItem { get; init; }

    internal Step(PlanBuilder builder, PlanStep planStep)
    {
        Builder = builder;
        InternalStep = planStep;
    }

    public virtual Step DependsOn(params Step[] steps)
    {
        foreach (var s in steps)
        {
            if (!InternalStep.DependsOn.Contains(s.InternalStep.Id))
                InternalStep.DependsOn.Add(s.InternalStep.Id);
        }
        return this;
    }

    public virtual Step WithName(string name)
    {
        InternalStep.Name = name;
        return this;
    }

    public virtual Step WithTriggerRule(TriggerRule rule)
    {
        InternalStep.TriggerRule = rule;
        return this;
    }

    public virtual Step Optional()
    {
        InternalStep.IsOptional = true;
        return this;
    }

    public virtual Step WithPriority(int priority)
    {
        SetOverride(new ConstantValue { Value = JsonSerializer.SerializeToElement(priority) }, "priority");
        return this;
    }

    public virtual Step WithPriority(Step<int> priorityStep)
    {
        SetOverride(new StepRefValue { StepId = priorityStep.InternalStep.Id }, "priority");
        AddDepIfMissing(priorityStep);
        return this;
    }

    public virtual Step NotBefore(DateTimeOffset notBefore)
    {
        SetOverride(new ConstantValue { Value = JsonSerializer.SerializeToElement(notBefore) }, "notBefore");
        return this;
    }

    public virtual Step NotBefore(Step<DateTimeOffset> scheduledTimeStep)
    {
        SetOverride(new StepRefValue { StepId = scheduledTimeStep.InternalStep.Id }, "notBefore");
        AddDepIfMissing(scheduledTimeStep);
        return this;
    }

    public virtual Step WithDeduplicationId(string key)
    {
        SetOverride(new ConstantValue { Value = JsonSerializer.SerializeToElement(key) }, "deduplicationId");
        return this;
    }

    public virtual Step WithDeduplicationId(Step<string> dedupStep)
    {
        SetOverride(new StepRefValue { StepId = dedupStep.InternalStep.Id }, "deduplicationId");
        AddDepIfMissing(dedupStep);
        return this;
    }

    public virtual Step WithFallback(string fallbackJobName)
    {
        if (InternalStep is RunStep runStep)
            runStep.FallbackJobName = fallbackJobName;
        else
            throw new InvalidOperationException("WithFallback is only supported on Run steps.");
        return this;
    }

    private JobStep GetJobStep() =>
        InternalStep as JobStep ?? throw new InvalidOperationException("Step overrides are only supported on Run and Stream steps.");

    private void SetOverride(StepValue value, string field)
    {
        var step = GetJobStep();
        switch (field)
        {
            case "priority": step.PriorityOverride = value; break;
            case "notBefore": step.NotBeforeOverride = value; break;
            case "deduplicationId": step.DeduplicationIdOverride = value; break;
        }
    }

    private void AddDepIfMissing(Step refStep)
    {
        if (!InternalStep.DependsOn.Contains(refStep.InternalStep.Id))
            InternalStep.DependsOn.Add(refStep.InternalStep.Id);
    }
}

public class Step<T> : Step
{
    internal Step(PlanBuilder builder, PlanStep planStep) : base(builder, planStep) { }

    public override Step<T> DependsOn(params Step[] steps)
    {
        base.DependsOn(steps);
        return this;
    }

    public override Step<T> WithName(string name)
    {
        base.WithName(name);
        return this;
    }

    public override Step<T> WithTriggerRule(TriggerRule rule)
    {
        base.WithTriggerRule(rule);
        return this;
    }

    public override Step<T> Optional()
    {
        base.Optional();
        return this;
    }

    public override Step<T> WithPriority(int priority)
    {
        base.WithPriority(priority);
        return this;
    }

    public override Step<T> WithPriority(Step<int> priorityStep)
    {
        base.WithPriority(priorityStep);
        return this;
    }

    public override Step<T> NotBefore(DateTimeOffset notBefore)
    {
        base.NotBefore(notBefore);
        return this;
    }

    public override Step<T> NotBefore(Step<DateTimeOffset> scheduledTimeStep)
    {
        base.NotBefore(scheduledTimeStep);
        return this;
    }

    public override Step<T> WithDeduplicationId(string key)
    {
        base.WithDeduplicationId(key);
        return this;
    }

    public override Step<T> WithDeduplicationId(Step<string> dedupStep)
    {
        base.WithDeduplicationId(dedupStep);
        return this;
    }

    public override Step<T> WithFallback(string fallbackJobName)
    {
        base.WithFallback(fallbackJobName);
        return this;
    }
}

public class StreamStep<T> : Step<T>
{
    internal override bool IsStream => true;

    internal StreamStep(PlanBuilder builder, PlanStep planStep) : base(builder, planStep) { }

    public override StreamStep<T> DependsOn(params Step[] steps)
    {
        base.DependsOn(steps);
        return this;
    }

    public override StreamStep<T> WithName(string name)
    {
        base.WithName(name);
        return this;
    }

    public override StreamStep<T> WithTriggerRule(TriggerRule rule)
    {
        base.WithTriggerRule(rule);
        return this;
    }

    public override StreamStep<T> Optional()
    {
        base.Optional();
        return this;
    }

    public override StreamStep<T> WithPriority(int priority)
    {
        base.WithPriority(priority);
        return this;
    }

    public override StreamStep<T> WithPriority(Step<int> priorityStep)
    {
        base.WithPriority(priorityStep);
        return this;
    }

    public override StreamStep<T> NotBefore(DateTimeOffset notBefore)
    {
        base.NotBefore(notBefore);
        return this;
    }

    public override StreamStep<T> NotBefore(Step<DateTimeOffset> scheduledTimeStep)
    {
        base.NotBefore(scheduledTimeStep);
        return this;
    }

    public override StreamStep<T> WithDeduplicationId(string key)
    {
        base.WithDeduplicationId(key);
        return this;
    }

    public override StreamStep<T> WithDeduplicationId(Step<string> dedupStep)
    {
        base.WithDeduplicationId(dedupStep);
        return this;
    }

    public override StreamStep<T> WithFallback(string fallbackJobName)
    {
        base.WithFallback(fallbackJobName);
        return this;
    }
}
