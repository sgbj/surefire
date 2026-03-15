namespace Surefire;

public sealed class IfBuilder
{
    private readonly IfStep _ifStep;
    private readonly Step _step;

    internal IfBuilder(PlanBuilder parent, Step<bool> condition, Action<PlanBuilder> body)
    {
        _ifStep = new IfStep
        {
            Id = parent.NextStepId(),
            Branches =
            [
                new ConditionalBranch
                {
                    ConditionStepId = condition.InternalStep.Id,
                    Branch = BuildBranch(body)
                }
            ]
        };
        _ifStep.DependsOn.Add(condition.InternalStep.Id);

        parent.AddStep(_ifStep);
        _step = new Step(parent, _ifStep);
    }

    public IfBuilder ElseIf(Step<bool> condition, Action<PlanBuilder> body)
    {
        _ifStep.Branches.Add(new ConditionalBranch
        {
            ConditionStepId = condition.InternalStep.Id,
            Branch = BuildBranch(body)
        });

        if (!_ifStep.DependsOn.Contains(condition.InternalStep.Id))
            _ifStep.DependsOn.Add(condition.InternalStep.Id);

        return this;
    }

    public Step Else(Action<PlanBuilder> body)
    {
        _ifStep.ElseBranch = BuildBranch(body);
        return _step;
    }

    private static PlanGraph BuildBranch(Action<PlanBuilder> body)
    {
        var pb = new PlanBuilder();
        body(pb);
        return pb.Build();
    }

    public static implicit operator Step(IfBuilder builder) => builder._step;
}

public sealed class IfBuilder<T>
{
    private readonly IfStep _ifStep;
    private readonly Step<T> _step;

    internal IfBuilder(PlanBuilder parent, Step<bool> condition, Func<PlanBuilder, Step<T>> body)
    {
        _ifStep = new IfStep
        {
            Id = parent.NextStepId(),
            Branches =
            [
                new ConditionalBranch
                {
                    ConditionStepId = condition.InternalStep.Id,
                    Branch = BuildBranch(body)
                }
            ]
        };
        _ifStep.DependsOn.Add(condition.InternalStep.Id);

        parent.AddStep(_ifStep);
        _step = new Step<T>(parent, _ifStep);
    }

    public IfBuilder<T> ElseIf(Step<bool> condition, Func<PlanBuilder, Step<T>> body)
    {
        _ifStep.Branches.Add(new ConditionalBranch
        {
            ConditionStepId = condition.InternalStep.Id,
            Branch = BuildBranch(body)
        });

        if (!_ifStep.DependsOn.Contains(condition.InternalStep.Id))
            _ifStep.DependsOn.Add(condition.InternalStep.Id);

        return this;
    }

    public Step<T> Else(Func<PlanBuilder, Step<T>> body)
    {
        _ifStep.ElseBranch = BuildBranch(body);
        return _step;
    }

    private static PlanGraph BuildBranch(Func<PlanBuilder, Step<T>> body)
    {
        var pb = new PlanBuilder();
        var result = body(pb);
        return pb.Build(result.InternalStep.Id);
    }

    public static implicit operator Step<T>(IfBuilder<T> builder) => builder._step;
}
