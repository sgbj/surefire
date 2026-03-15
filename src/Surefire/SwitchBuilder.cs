namespace Surefire;

public sealed class SwitchBuilder
{
    private readonly SwitchStep _switchStep;
    private readonly Step _step;

    internal SwitchBuilder(PlanBuilder parent, Step<string> key)
    {
        _switchStep = new SwitchStep
        {
            Id = parent.NextStepId(),
            KeyStepId = key.InternalStep.Id,
            Cases = []
        };
        _switchStep.DependsOn.Add(key.InternalStep.Id);

        parent.AddStep(_switchStep);
        _step = new Step(parent, _switchStep);
    }

    public SwitchBuilder Case(string value, Action<PlanBuilder> body)
    {
        var pb = new PlanBuilder();
        body(pb);

        _switchStep.Cases.Add(new SwitchCase
        {
            Value = value,
            Branch = pb.Build()
        });
        return this;
    }

    public Step Default(Action<PlanBuilder> body)
    {
        var pb = new PlanBuilder();
        body(pb);
        _switchStep.DefaultBranch = pb.Build();
        return _step;
    }

    public static implicit operator Step(SwitchBuilder builder) => builder._step;
}

public sealed class SwitchBuilder<T>
{
    private readonly SwitchStep _switchStep;
    private readonly Step<T> _step;

    internal SwitchBuilder(PlanBuilder parent, Step<string> key)
    {
        _switchStep = new SwitchStep
        {
            Id = parent.NextStepId(),
            KeyStepId = key.InternalStep.Id,
            Cases = []
        };
        _switchStep.DependsOn.Add(key.InternalStep.Id);

        parent.AddStep(_switchStep);
        _step = new Step<T>(parent, _switchStep);
    }

    public SwitchBuilder<T> Case(string value, Func<PlanBuilder, Step<T>> body)
    {
        var pb = new PlanBuilder();
        var result = body(pb);

        _switchStep.Cases.Add(new SwitchCase
        {
            Value = value,
            Branch = pb.Build(result.InternalStep.Id)
        });
        return this;
    }

    public Step<T> Default(Func<PlanBuilder, Step<T>> body)
    {
        var pb = new PlanBuilder();
        var result = body(pb);
        _switchStep.DefaultBranch = pb.Build(result.InternalStep.Id);
        return _step;
    }

    public static implicit operator Step<T>(SwitchBuilder<T> builder) => builder._step;
}
