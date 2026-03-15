namespace Surefire;

internal static class PlanGraphValidator
{
    public static void Validate(PlanGraph graph)
    {
        if (graph.Steps.Count == 0) return;

        var stepIds = new HashSet<string>();
        var stepNames = new HashSet<string>();

        foreach (var step in graph.Steps)
        {
            if (!stepIds.Add(step.Id))
                throw new InvalidOperationException($"Duplicate step ID '{step.Id}'.");

            if (step.Name is not null && !stepNames.Add(step.Name))
                throw new InvalidOperationException($"Duplicate step name '{step.Name}'.");
        }

        // Validate dependency references
        foreach (var step in graph.Steps)
        {
            foreach (var depId in step.DependsOn)
            {
                if (!stepIds.Contains(depId))
                    throw new InvalidOperationException($"Step '{step.Id}' depends on unknown step '{depId}'.");
            }

            // Validate argument references
            var args = (step as JobStep)?.Arguments;
            if (args is not null)
            {
                foreach (var (key, value) in args)
                {
                    var refId = value switch
                    {
                        StepRefValue sr => sr.StepId,
                        StreamRefValue sr => sr.StepId,
                        _ => null
                    };

                    if (refId is not null && !stepIds.Contains(refId))
                        throw new InvalidOperationException(
                            $"Step '{step.Id}' argument '{key}' references unknown step '{refId}'.");
                }
            }

            // Validate ForEachStep
            if (step is ForEachStep forEach)
            {
                if (!stepIds.Contains(forEach.SourceStepId))
                    throw new InvalidOperationException(
                        $"ForEach step '{step.Id}' references unknown source step '{forEach.SourceStepId}'.");

                if (forEach.Template.Steps.Count > 0)
                    Validate(forEach.Template); // Recursively validate template
            }

            // Validate WhenAllStep
            if (step is WhenAllStep whenAll)
            {
                foreach (var sourceId in whenAll.SourceStepIds)
                {
                    if (!stepIds.Contains(sourceId))
                        throw new InvalidOperationException(
                            $"WhenAll step '{step.Id}' references unknown source step '{sourceId}'.");
                }
            }

            // Validate IfStep
            if (step is IfStep ifStep)
            {
                foreach (var branch in ifStep.Branches)
                {
                    if (!stepIds.Contains(branch.ConditionStepId))
                        throw new InvalidOperationException(
                            $"If step '{step.Id}' references unknown condition step '{branch.ConditionStepId}'.");
                    if (branch.Branch.Steps.Count > 0)
                        Validate(branch.Branch);
                }
                if (ifStep.ElseBranch?.Steps.Count > 0)
                    Validate(ifStep.ElseBranch);
            }

            // Validate SwitchStep
            if (step is SwitchStep switchStep)
            {
                if (!stepIds.Contains(switchStep.KeyStepId))
                    throw new InvalidOperationException(
                        $"Switch step '{step.Id}' references unknown key step '{switchStep.KeyStepId}'.");
                foreach (var @case in switchStep.Cases)
                {
                    if (@case.Branch.Steps.Count > 0)
                        Validate(@case.Branch);
                }
                if (switchStep.DefaultBranch?.Steps.Count > 0)
                    Validate(switchStep.DefaultBranch);
            }

            // Validate SignalStep
            if (step is SignalStep signal)
            {
                if (string.IsNullOrWhiteSpace(signal.SignalName))
                    throw new InvalidOperationException(
                        $"Signal step '{step.Id}' has an empty signal name.");
            }

            // Validate OperatorStep
            if (step is OperatorStep operatorStep)
            {
                if (!stepIds.Contains(operatorStep.SourceStepId))
                    throw new InvalidOperationException(
                        $"Operator step '{step.Id}' references unknown source step '{operatorStep.SourceStepId}'.");
            }

            // Validate override StepRefValues
            if (step is JobStep jobStep)
            {
                ValidateOverrideRef(step, stepIds, jobStep.PriorityOverride, "PriorityOverride");
                ValidateOverrideRef(step, stepIds, jobStep.NotBeforeOverride, "NotBeforeOverride");
                ValidateOverrideRef(step, stepIds, jobStep.DeduplicationIdOverride, "DeduplicationIdOverride");
            }
        }

        // Validate OutputStepId
        if (graph.OutputStepId is not null && !stepIds.Contains(graph.OutputStepId))
            throw new InvalidOperationException($"Output step '{graph.OutputStepId}' not found in graph.");

        // Cycle detection via topological sort
        DetectCycles(graph);
    }

    private static void ValidateOverrideRef(PlanStep step, HashSet<string> stepIds, StepValue? overrideValue, string fieldName)
    {
        if (overrideValue is StepRefValue sr && !stepIds.Contains(sr.StepId))
            throw new InvalidOperationException(
                $"Step '{step.Id}' {fieldName} references unknown step '{sr.StepId}'.");
    }

    private static void DetectCycles(PlanGraph graph)
    {
        var visited = new Dictionary<string, int>();
        var stepMap = graph.Steps.ToDictionary(s => s.Id);
        foreach (var step in graph.Steps)
            visited[step.Id] = 0;

        foreach (var step in graph.Steps)
        {
            if (visited[step.Id] == 0 && HasCycle(step.Id, stepMap, visited))
                throw new InvalidOperationException("Plan graph contains a cycle.");
        }
    }

    private static bool HasCycle(string stepId, Dictionary<string, PlanStep> stepMap, Dictionary<string, int> visited)
    {
        visited[stepId] = 1;

        var step = stepMap[stepId];
        foreach (var depId in step.DependsOn)
        {
            if (!visited.TryGetValue(depId, out var state))
                continue;

            if (state == 1) return true;
            if (state == 0 && HasCycle(depId, stepMap, visited)) return true;
        }

        visited[stepId] = 2;
        return false;
    }
}
