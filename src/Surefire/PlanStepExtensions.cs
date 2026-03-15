namespace Surefire;

public static class PlanStepExtensions
{
    // ForEach on Step<TItem[]> — array source, collected results

    public static Step<TResult[]> ForEach<TItem, TResult>(
        this Step<TItem[]> source,
        Func<PlanBuilder, Step<TItem>, Step<TResult>> body)
    {
        return ForEachCore<TItem, TResult>(source, body);
    }

    // ForEach on Step<TItem[]> — array source, no results

    public static Step ForEach<TItem>(
        this Step<TItem[]> source,
        Func<PlanBuilder, Step<TItem>, Step> body)
    {
        return ForEachCoreUntyped<TItem>(source, body);
    }

    // StreamForEach on Step<TItem[]> — array source, streamed results

    public static StreamStep<TResult> StreamForEach<TItem, TResult>(
        this Step<TItem[]> source,
        Func<PlanBuilder, Step<TItem>, Step<TResult>> body)
    {
        return StreamForEachCore<TItem, TResult>(source, body);
    }

    // ForEach on StreamStep<TItem> — streaming source, collected results

    public static Step<TResult[]> ForEach<TItem, TResult>(
        this StreamStep<TItem> source,
        Func<PlanBuilder, Step<TItem>, Step<TResult>> body)
    {
        return ForEachCore<TItem, TResult>(source, body);
    }

    // ForEach on StreamStep<TItem> — streaming source, no results

    public static Step ForEach<TItem>(
        this StreamStep<TItem> source,
        Func<PlanBuilder, Step<TItem>, Step> body)
    {
        return ForEachCoreUntyped<TItem>(source, body);
    }

    // StreamForEach on StreamStep<TItem> — streaming source, streamed results

    public static StreamStep<TResult> StreamForEach<TItem, TResult>(
        this StreamStep<TItem> source,
        Func<PlanBuilder, Step<TItem>, Step<TResult>> body)
    {
        return StreamForEachCore<TItem, TResult>(source, body);
    }

    // Core implementations

    private static Step<TResult[]> ForEachCore<TItem, TResult>(
        Step source,
        Func<PlanBuilder, Step<TItem>, Step<TResult>> body)
    {
        var parent = source.Builder;
        var pb = new PlanBuilder();
        var itemHandle = CreateItemHandle<TItem>(pb);
        var result = body(pb, itemHandle);
        var template = pb.Build(result.InternalStep.Id);

        var stepId = parent.NextStepId();
        var forEachStep = new ForEachStep
        {
            Id = stepId,
            SourceStepId = source.InternalStep.Id,
            Template = template
        };
        forEachStep.DependsOn.Add(source.InternalStep.Id);

        parent.AddStep(forEachStep);
        return new Step<TResult[]>(parent, forEachStep);
    }

    private static Step ForEachCoreUntyped<TItem>(
        Step source,
        Func<PlanBuilder, Step<TItem>, Step> body)
    {
        var parent = source.Builder;
        var pb = new PlanBuilder();
        var itemHandle = CreateItemHandle<TItem>(pb);
        body(pb, itemHandle);
        var template = pb.Build();

        var stepId = parent.NextStepId();
        var forEachStep = new ForEachStep
        {
            Id = stepId,
            SourceStepId = source.InternalStep.Id,
            Template = template
        };
        forEachStep.DependsOn.Add(source.InternalStep.Id);

        parent.AddStep(forEachStep);
        return new Step(parent, forEachStep);
    }

    private static StreamStep<TResult> StreamForEachCore<TItem, TResult>(
        Step source,
        Func<PlanBuilder, Step<TItem>, Step<TResult>> body)
    {
        var parent = source.Builder;
        var pb = new PlanBuilder();
        var itemHandle = CreateItemHandle<TItem>(pb);
        var result = body(pb, itemHandle);
        var template = pb.Build(result.InternalStep.Id);

        var stepId = parent.NextStepId();
        var forEachStep = new ForEachStep
        {
            Id = stepId,
            SourceStepId = source.InternalStep.Id,
            Template = template,
            IsStreaming = true
        };
        forEachStep.DependsOn.Add(source.InternalStep.Id);

        parent.AddStep(forEachStep);
        return new StreamStep<TResult>(parent, forEachStep);
    }

    private static Step<TItem> CreateItemHandle<TItem>(PlanBuilder builder)
    {
        var itemPlanStep = new InputStep { Id = "item", ParamName = "item" };
        return new Step<TItem>(builder, itemPlanStep) { IsItem = true };
    }
}
