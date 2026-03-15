using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class PlanEngine(
    IJobStore store,
    INotificationProvider notifications,
    TimeProvider timeProvider,
    SurefireOptions options,
    ILogger<PlanEngine> logger)
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _planLocks = new();
    private readonly ConcurrentDictionary<string, IAsyncDisposable> _streamSubscriptions = new();



    /// <summary>Creates a completed synthetic run for engine-resolved steps (input, whenAll, operator, no-match if/switch).</summary>
    private JobRun CreateCompletedSyntheticRun(string jobName, string stepId, string? stepName, StepContext ctx, string? result = null)
    {
        var now = timeProvider.GetUtcNow();
        return new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Completed,
            Result = result,
            CreatedAt = now,
            StartedAt = now,
            CompletedAt = now,
            NotBefore = now,
            ParentRunId = ctx.ParentRunId,
            PlanRunId = ctx.PlanRunId,
            PlanStepId = stepId,
            PlanStepName = stepName
        };
    }

    /// <summary>Creates a running synthetic run for engine-managed steps (if, switch, signal, forEach).</summary>
    private JobRun CreateRunningSyntheticRun(string jobName, string stepId, string? stepName, StepContext ctx, string? arguments = null)
    {
        var now = timeProvider.GetUtcNow();
        return new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Running,
            Arguments = arguments,
            CreatedAt = now,
            StartedAt = now,
            NotBefore = now,
            NodeName = "_engine",
            ParentRunId = ctx.ParentRunId,
            PlanRunId = ctx.PlanRunId,
            PlanStepId = stepId,
            PlanStepName = stepName,
            LastHeartbeatAt = now
        };
    }

    /// <summary>Creates the run and registers it in the step runs dictionary if successful.</summary>
    private async Task<bool> TryCreateStepRunAsync(JobRun run, string stepId, StepContext ctx, CancellationToken ct)
    {
        if (await store.TryCreateRunAsync(run, ct))
        {
            ctx.StepRuns[stepId] = run;
            return true;
        }
        return false;
    }

    public async Task AdvancePlanAsync(string planRunId, CancellationToken ct)
    {
        var semaphore = _planLocks.GetOrAdd(planRunId, _ => new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync(ct);
        try
        {
            await AdvanceCoreAsync(planRunId, ct);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task AdvanceCoreAsync(string planRunId, CancellationToken ct, int depth = 0)
    {
        // Guard against infinite recursion — each pass should create at least one new step run,
        // so the maximum useful depth equals the number of steps in the graph.
        const int maxDepth = 200;
        if (depth >= maxDepth)
        {
            logger.LogWarning("Plan {PlanRunId} hit advancement depth limit ({MaxDepth}), deferring to next cycle",
                planRunId, maxDepth);
            return;
        }

        var planRun = await store.GetRunAsync(planRunId, ct);
        if (planRun is null || planRun.PlanGraph is null) return;
        if (planRun.Status.IsTerminal)
        {
            _planLocks.TryRemove(planRunId, out _);
            await CleanupStreamSubscriptionsAsync(planRunId);
            return;
        }

        // Keep the plan run alive so inactive-run detection doesn't kill it
        await store.HeartbeatRunsAsync([planRunId], ct);

        var graph = JsonSerializer.Deserialize<PlanGraph>(planRun.PlanGraph, options.SerializerOptions)!;

        // Load all step runs for this plan (paginated to handle large ForEach expansions)
        var stepRuns = new Dictionary<string, JobRun>();
        var skip = 0;
        const int pageSize = 5000;
        while (true)
        {
            var page = await store.GetRunsAsync(
                new RunFilter { PlanRunId = planRunId, Take = pageSize, Skip = skip }, ct);
            foreach (var r in page.Items)
            {
                // Items are in descending CreatedAt order. Keep the newest run for each step
                // so retries take precedence over the original failed run.
                if (r.PlanStepId is not null)
                    stepRuns.TryAdd(r.PlanStepId, r);
            }
            if (page.Items.Count < pageSize) break;
            skip += pageSize;
        }

        var countBefore = stepRuns.Count;
        var ctx = new StepContext(graph, planRunId, stepRuns);

        foreach (var step in graph.Steps)
        {
            if (stepRuns.TryGetValue(step.Id, out var existingRun))
            {
                if (!ShouldReprocess(step, existingRun))
                    continue;
            }

            if (!IsUnblocked(step, ctx)) continue;

            await ProcessStepAsync(step, ctx, planRun, ct);
        }

        // Update plan progress based on terminal step count
        var totalSteps = graph.Steps.Count;
        if (totalSteps > 0)
        {
            var completedSteps = graph.Steps.Count(s =>
                stepRuns.TryGetValue(s.Id, out var r) && r.Status.IsTerminal);
            var progress = (double)completedSteps / totalSteps;
            if (Math.Abs(planRun.Progress - progress) > 0.001)
            {
                planRun.Progress = progress;
                await store.UpdateRunAsync(planRun, ct);
            }
        }

        // Handle failure policy — cancel dependents/all on step failure
        await ApplyFailurePolicyAsync(graph, stepRuns, planRunId, ct);

        // Check plan completion
        var completed = await CheckPlanCompletionAsync(planRun, graph, stepRuns, ct);

        // Clean up the lock entry when the plan completes
        if (completed)
        {
            _planLocks.TryRemove(planRunId, out _);
            return;
        }

        // If new step runs were actually added, recurse to unblock further steps.
        // Engine-native steps (Input, WhenAll, If, Switch, Operator) complete synchronously and may unblock downstream.
        if (stepRuns.Count > countBefore)
        {
            await AdvanceCoreAsync(planRunId, ct, depth + 1);
        }
    }

    /// <summary>Determines if an existing step run needs re-processing (streaming ForEach, sub-plan polling, fallback).</summary>
    private static bool ShouldReprocess(PlanStep step, JobRun existingRun)
    {
        // ForEach with a Running synthetic run needs re-processing
        // to heartbeat, check completion, and (for streaming) emit results
        if (step is ForEachStep && existingRun.Status == JobStatus.Running)
            return true;

        // Run/Stream step acting as a sub-plan proxy — needs re-checking for sub-plan completion.
        // These are identified by NodeName == "_engine" (synthetic runs not claimed by a worker).
        if (step is JobStep
            && existingRun.Status == JobStatus.Running
            && existingRun.NodeName == "_engine")
            return true;

        // If/Switch steps with Running synthetic runs need re-processing
        // to continue expanding their branch templates
        if (step is IfStep or SwitchStep
            && existingRun.Status == JobStatus.Running
            && existingRun.NodeName == "_engine")
            return true;

        // Check if a step with a fallback has failed and the fallback hasn't been tried yet
        if (step is RunStep { FallbackJobName: not null } rs
            && existingRun.Status is JobStatus.Failed or JobStatus.DeadLetter
            && existingRun.JobName != rs.FallbackJobName)
            return true;

        return false;
    }

    /// <summary>
    /// Shared step dispatch — handles all step types for both top-level and template steps.
    /// </summary>
    private async Task ProcessStepAsync(PlanStep step, StepContext ctx, JobRun? planRun, CancellationToken ct)
    {
        var stepId = ctx.MapStepId(step.Id);

        if (step is InputStep input)
        {
            await ResolveInputStepAsync(input, stepId, ctx, planRun, ct);
        }
        else if (step is WhenAllStep whenAll)
        {
            await ResolveWhenAllStepAsync(whenAll, stepId, ctx, ct);
        }
        else if (step is ForEachStep forEach)
        {
            await ExpandForEachAsync(forEach, stepId, ctx, ct);
        }
        else if (step is IfStep ifStep)
        {
            await ResolveIfStepAsync(ifStep, stepId, ctx, ct);
        }
        else if (step is SwitchStep switchStep)
        {
            await ResolveSwitchStepAsync(switchStep, stepId, ctx, ct);
        }
        else if (step is SignalStep signal)
        {
            await HandleSignalStepAsync(signal, stepId, ctx, ct);
        }
        else if (step is OperatorStep operatorStep)
        {
            await ResolveOperatorStepAsync(operatorStep, stepId, ctx, ct);
        }
        else if (step is RunStep runStep && runStep.FallbackJobName is not null
            && ctx.StepRuns.TryGetValue(stepId, out var failedRun)
            && failedRun.Status is JobStatus.Failed or JobStatus.DeadLetter
            && failedRun.JobName != runStep.FallbackJobName)
        {
            await CreateFallbackRunAsync(runStep, stepId, ctx, ct);
        }
        else if (step is JobStep)
        {
            await CreateStepRunAsync(step, stepId, ctx, ct);
        }
    }

    private static bool IsUnblocked(PlanStep step, StepContext ctx)
    {
        if (step.DependsOn.Count == 0) return true;

        var args = (step as JobStep)?.Arguments;
        var forEachSourceId = (step as ForEachStep)?.SourceStepId;
        var operatorSourceId = (step as OperatorStep)?.SourceStepId;

        foreach (var depId in step.DependsOn)
        {
            var dep = ctx.GetRun(depId);
            if (dep is null)
                return false; // Dep not yet created

            // ForEach source that is a streaming step — Running is sufficient
            if (depId == forEachSourceId)
            {
                var sourceStep = ctx.Graph.Steps.FirstOrDefault(s => s.Id == depId);
                if (sourceStep is Surefire.StreamStep)
                {
                    if (dep.Status is not (JobStatus.Running or JobStatus.Completed))
                        return false;
                    continue;
                }
            }

            var isStreamDep = args?.Values.Any(v => v is StreamRefValue r && r.StepId == depId) == true;
            var isDataDep = args?.Values.Any(v => v is StepRefValue r && r.StepId == depId) == true;

            // Also treat operator source as a data dep
            if (depId == operatorSourceId)
                isDataDep = true;

            if (isStreamDep)
            {
                // Stream deps require upstream to be running or completed
                if (dep.Status is not (JobStatus.Running or JobStatus.Completed))
                    return false;
            }
            else if (isDataDep)
            {
                // Optional dep that failed/deadlettered — treat as satisfied (with null result)
                var depStep = ctx.Graph.Steps.FirstOrDefault(s => s.Id == depId);
                if (depStep?.IsOptional == true && dep.Status is JobStatus.Failed or JobStatus.DeadLetter or JobStatus.Skipped)
                    continue;

                if (dep.Status != JobStatus.Completed)
                    return false;
            }
            else
            {
                // Find the dep step to check IsOptional
                var depStep = ctx.Graph.Steps.FirstOrDefault(s => s.Id == depId);

                // Ordering-only dependency — governed by trigger rule
                switch (step.TriggerRule)
                {
                    case TriggerRule.AllDone:
                        if (!dep.Status.IsTerminal) return false;
                        break;
                    case TriggerRule.AllFailed:
                        if (!dep.Status.IsTerminal) return false;
                        if (dep.Status is not (JobStatus.Failed or JobStatus.DeadLetter)) return false;
                        break;
                    case TriggerRule.OneSuccess:
                        // At least one dep completed — check across all deps later
                        // For now, allow if this dep is completed
                        if (dep.Status == JobStatus.Completed) continue;
                        // If dep is not terminal, we might still be waiting
                        if (!dep.Status.IsTerminal) continue;
                        break;
                    case TriggerRule.OneFailed:
                        if (dep.Status is JobStatus.Failed or JobStatus.DeadLetter) continue;
                        if (!dep.Status.IsTerminal) continue;
                        break;
                    case TriggerRule.NoneSkipped:
                        if (!dep.Status.IsTerminal) return false;
                        if (dep.Status == JobStatus.Skipped) return false;
                        break;
                    default: // AllSuccess
                        // Optional dep that failed/deadlettered — treat as satisfied
                        if (depStep?.IsOptional == true && dep.Status is JobStatus.Failed or JobStatus.DeadLetter or JobStatus.Skipped)
                            continue;
                        if (dep.Status != JobStatus.Completed) return false;
                        break;
                }
            }
        }

        // For OneSuccess/OneFailed, verify the aggregate condition
        if (step.TriggerRule == TriggerRule.OneSuccess)
        {
            var anySuccess = step.DependsOn.Any(depId =>
            {
                var d = ctx.GetRun(depId);
                return d is not null && d.Status == JobStatus.Completed;
            });
            if (!anySuccess)
            {
                // If all deps are terminal but none succeeded, still unblocked (will fail gracefully)
                var allTerminal = step.DependsOn.All(depId =>
                {
                    var d = ctx.GetRun(depId);
                    return d is not null && d.Status.IsTerminal;
                });
                if (!allTerminal) return false;
            }
        }
        else if (step.TriggerRule == TriggerRule.OneFailed)
        {
            var anyFailed = step.DependsOn.Any(depId =>
            {
                var d = ctx.GetRun(depId);
                return d is not null && d.Status is JobStatus.Failed or JobStatus.DeadLetter;
            });
            if (!anyFailed)
            {
                var allTerminal = step.DependsOn.All(depId =>
                {
                    var d = ctx.GetRun(depId);
                    return d is not null && d.Status.IsTerminal;
                });
                if (!allTerminal) return false;
            }
        }

        return true;
    }

    // -- Input --

    private async Task ResolveInputStepAsync(
        InputStep input, string stepId, StepContext ctx, JobRun? planRun, CancellationToken ct)
    {
        // If planRun wasn't passed, load it
        planRun ??= await store.GetRunAsync(ctx.PlanRunId, ct);

        // Resolve the input value from the plan run's arguments
        JsonElement? value = null;
        if (planRun?.Arguments is not null)
        {
            var args = JsonSerializer.Deserialize<JsonElement>(planRun.Arguments);
            if (args.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in args.EnumerateObject())
                {
                    if (string.Equals(prop.Name, input.ParamName, StringComparison.OrdinalIgnoreCase))
                    {
                        value = prop.Value;
                        break;
                    }
                }
            }
        }

        var result = value is not null ? value.Value.GetRawText() : null;
        var run = CreateCompletedSyntheticRun($"_input:{input.ParamName}", stepId, input.Name, ctx, result);
        await TryCreateStepRunAsync(run, stepId, ctx, ct);
    }

    // -- WhenAll --

    private async Task ResolveWhenAllStepAsync(
        WhenAllStep whenAll, string stepId, StepContext ctx, CancellationToken ct)
    {
        // Collect results from all source steps into an array
        var results = new JsonArray();
        foreach (var sourceId in whenAll.SourceStepIds)
        {
            var sourceRun = ctx.GetRun(sourceId);
            if (sourceRun?.Result is not null)
                results.Add(JsonNode.Parse(sourceRun.Result));
            else
                results.Add(null);
        }

        var run = CreateCompletedSyntheticRun($"_whenAll:{whenAll.Id}", stepId, whenAll.Name, ctx,
            results.ToJsonString(options.SerializerOptions));
        await TryCreateStepRunAsync(run, stepId, ctx, ct);
    }

    // -- If/Switch --

    private async Task ResolveIfStepAsync(
        IfStep ifStep, string stepId, StepContext ctx, CancellationToken ct)
    {
        // Re-entry: continue expanding the winning branch
        if (ctx.StepRuns.TryGetValue(stepId, out var existingRun)
            && existingRun.Status == JobStatus.Running
            && existingRun.NodeName == "_engine")
        {
            var branchIndex = existingRun.Arguments is not null
                ? JsonSerializer.Deserialize<int>(existingRun.Arguments, options.SerializerOptions) : -1;
            var branch = branchIndex >= 0 && branchIndex < ifStep.Branches.Count
                ? ifStep.Branches[branchIndex].Branch
                : ifStep.ElseBranch;

            if (branch is not null)
            {
                var branchKey = branchIndex >= 0 ? $"then{branchIndex}" : "else";
                var branchPrefix = $"{stepId}[{branchKey}]";
                await store.HeartbeatRunsAsync([existingRun.Id], ct);
                await ExpandTemplateAsync(branch, branchPrefix, ctx, ctx.ItemValue, ct, existingRun.Id);
                await CheckBranchCompletionAsync(stepId, branchPrefix, branch, ctx, ct);
            }
            return;
        }

        // Evaluate conditions in order — first true wins
        PlanGraph? winningBranch = null;
        var winningIndex = -1;

        for (var i = 0; i < ifStep.Branches.Count; i++)
        {
            var conditionRun = ctx.GetRun(ifStep.Branches[i].ConditionStepId);
            if (conditionRun?.Result is null) return; // Condition not yet resolved

            var conditionValue = JsonSerializer.Deserialize<bool>(conditionRun.Result, options.SerializerOptions);
            if (conditionValue)
            {
                winningBranch = ifStep.Branches[i].Branch;
                winningIndex = i;
                break;
            }
        }

        // No condition matched — use else branch
        if (winningBranch is null)
            winningBranch = ifStep.ElseBranch;

        // No branch matched and no else — complete immediately with null result
        if (winningBranch is null)
        {
            var noMatchRun = CreateCompletedSyntheticRun($"_if:{ifStep.Id}", stepId, ifStep.Name, ctx);
            await TryCreateStepRunAsync(noMatchRun, stepId, ctx, ct);
            return;
        }

        // Create Running synthetic run — branch index stored in Arguments for re-entry
        var ifRun = CreateRunningSyntheticRun($"_if:{ifStep.Id}", stepId, ifStep.Name, ctx,
            JsonSerializer.Serialize(winningIndex, options.SerializerOptions));
        await TryCreateStepRunAsync(ifRun, stepId, ctx, ct);

        var winningBranchKey = winningIndex >= 0 ? $"then{winningIndex}" : "else";
        var winningBranchPrefix = $"{stepId}[{winningBranchKey}]";
        await ExpandTemplateAsync(winningBranch, winningBranchPrefix, ctx, ctx.ItemValue, ct, ifRun.Id);
        await CheckBranchCompletionAsync(stepId, winningBranchPrefix, winningBranch, ctx, ct);
    }

    private async Task ResolveSwitchStepAsync(
        SwitchStep switchStep, string stepId, StepContext ctx, CancellationToken ct)
    {
        // Re-entry: continue expanding the winning branch
        if (ctx.StepRuns.TryGetValue(stepId, out var existingRun)
            && existingRun.Status == JobStatus.Running
            && existingRun.NodeName == "_engine")
        {
            var caseIndex = existingRun.Arguments is not null
                ? JsonSerializer.Deserialize<int>(existingRun.Arguments, options.SerializerOptions) : -1;
            var branch = caseIndex >= 0 && caseIndex < switchStep.Cases.Count
                ? switchStep.Cases[caseIndex].Branch
                : switchStep.DefaultBranch;

            if (branch is not null)
            {
                var branchKey = caseIndex >= 0 ? switchStep.Cases[caseIndex].Value : "default";
                var branchPrefix = $"{stepId}[{branchKey}]";
                await store.HeartbeatRunsAsync([existingRun.Id], ct);
                await ExpandTemplateAsync(branch, branchPrefix, ctx, ctx.ItemValue, ct, existingRun.Id);
                await CheckBranchCompletionAsync(stepId, branchPrefix, branch, ctx, ct);
            }
            return;
        }

        var keyRun = ctx.GetRun(switchStep.KeyStepId);
        if (keyRun?.Result is null) return;

        var keyValue = JsonSerializer.Deserialize<string>(keyRun.Result, options.SerializerOptions);

        // Find matching case
        PlanGraph? winningBranch = null;
        var winningIndex = -1;
        if (keyValue is not null)
        {
            for (var i = 0; i < switchStep.Cases.Count; i++)
            {
                if (switchStep.Cases[i].Value == keyValue)
                {
                    winningBranch = switchStep.Cases[i].Branch;
                    winningIndex = i;
                    break;
                }
            }
        }

        if (winningBranch is null)
            winningBranch = switchStep.DefaultBranch;

        // No case matched and no default — complete immediately with null result
        if (winningBranch is null)
        {
            var noMatchRun = CreateCompletedSyntheticRun($"_switch:{switchStep.Id}", stepId, switchStep.Name, ctx);
            await TryCreateStepRunAsync(noMatchRun, stepId, ctx, ct);
            return;
        }

        // Create Running synthetic run — case index stored in Arguments for re-entry
        var switchRun = CreateRunningSyntheticRun($"_switch:{switchStep.Id}", stepId, switchStep.Name, ctx,
            JsonSerializer.Serialize(winningIndex, options.SerializerOptions));
        await TryCreateStepRunAsync(switchRun, stepId, ctx, ct);

        var winningBranchKey = winningIndex >= 0 ? switchStep.Cases[winningIndex].Value : "default";
        var winningBranchPrefix = $"{stepId}[{winningBranchKey}]";
        await ExpandTemplateAsync(winningBranch, winningBranchPrefix, ctx, ctx.ItemValue, ct, switchRun.Id);
        await CheckBranchCompletionAsync(stepId, winningBranchPrefix, winningBranch, ctx, ct);
    }

    // -- Template expansion (shared by ForEach, If, and Switch) --

    /// <summary>
    /// Expands a template graph within an instance scope. Used by ForEach (per-item), If, and Switch (per-branch).
    /// </summary>
    private async Task ExpandTemplateAsync(
        PlanGraph template, string instancePrefix, StepContext parentCtx,
        JsonElement? itemValue, CancellationToken ct, string? parentRunIdOverride = null)
    {
        var templateCtx = new StepContext(
            template, parentCtx.PlanRunId, parentCtx.StepRuns,
            instancePrefix, itemValue, parentRunIdOverride);

        foreach (var templateStep in template.Steps)
        {
            var instanceStepId = templateCtx.MapStepId(templateStep.Id);

            if (parentCtx.StepRuns.TryGetValue(instanceStepId, out var existingRun))
            {
                if (!ShouldReprocess(templateStep, existingRun))
                    continue;
            }

            if (!IsUnblocked(templateStep, templateCtx)) continue;

            await ProcessStepAsync(templateStep, templateCtx, null, ct);
        }
    }

    private async Task CheckBranchCompletionAsync(
        string parentStepId, string branchPrefix, PlanGraph branch, StepContext ctx, CancellationToken ct)
    {
        // Check if all steps in the branch template are terminal
        foreach (var templateStep in branch.Steps)
        {
            var instanceId = $"{branchPrefix}.{templateStep.Id}";
            if (!ctx.StepRuns.TryGetValue(instanceId, out var run) || !run.Status.IsTerminal)
                return; // Not yet complete
        }

        // Branch is complete — transition If/Switch run from Running to Completed
        if (!ctx.StepRuns.TryGetValue(parentStepId, out var parentRun) || parentRun.Status != JobStatus.Running)
            return;

        // Get branch output result if available
        string? outputResult = null;
        if (branch.OutputStepId is not null)
        {
            var outputInstanceId = $"{branchPrefix}.{branch.OutputStepId}";
            if (ctx.StepRuns.TryGetValue(outputInstanceId, out var outputRun) && outputRun.Status == JobStatus.Completed)
                outputResult = outputRun.Result;
        }

        parentRun.Status = JobStatus.Completed;
        parentRun.Result = outputResult;
        parentRun.CompletedAt = timeProvider.GetUtcNow();
        parentRun.NodeName = null;
        await store.UpdateRunAsync(parentRun, ct);
    }

    // -- Signal --

    private async Task HandleSignalStepAsync(
        SignalStep signal, string stepId, StepContext ctx, CancellationToken ct)
    {
        if (ctx.StepRuns.ContainsKey(stepId)) return;

        var run = CreateRunningSyntheticRun($"_signal:{signal.SignalName}", stepId,
            signal.Name ?? signal.SignalName, ctx);
        await TryCreateStepRunAsync(run, stepId, ctx, ct);
    }

    /// <summary>
    /// Delivers a signal to a waiting signal step. Called by JobClient.SendSignalAsync.
    /// </summary>
    internal async Task DeliverSignalAsync(string planRunId, string signalName, string? payload, CancellationToken ct)
    {
        var planRun = await store.GetRunAsync(planRunId, ct);
        if (planRun is null || planRun.PlanGraph is null)
            throw new InvalidOperationException($"Plan run '{planRunId}' not found or is not a plan.");

        var graph = JsonSerializer.Deserialize<PlanGraph>(planRun.PlanGraph, options.SerializerOptions)!;

        // Find the signal step
        var signalStep = graph.Steps.OfType<SignalStep>().FirstOrDefault(s => s.SignalName == signalName)
            ?? throw new InvalidOperationException($"Signal '{signalName}' not found in plan '{planRunId}'.");

        // Find the signal step run (paginated to handle large ForEach expansions)
        JobRun? signalRun = null;
        var signalSkip = 0;
        const int signalPageSize = 5000;
        while (signalRun is null)
        {
            var signalPage = await store.GetRunsAsync(
                new RunFilter { PlanRunId = planRunId, Take = signalPageSize, Skip = signalSkip }, ct);
            signalRun = signalPage.Items.FirstOrDefault(r => r.PlanStepId == signalStep.Id && r.Status == JobStatus.Running);
            if (signalRun is not null || signalPage.Items.Count < signalPageSize) break;
            signalSkip += signalPageSize;
        }
        if (signalRun is null)
            throw new InvalidOperationException($"Signal step '{signalName}' is not in Running state.");

        // Complete the signal run with the payload
        var now = timeProvider.GetUtcNow();
        signalRun.Status = JobStatus.Completed;
        signalRun.Result = payload;
        signalRun.CompletedAt = now;
        await store.UpdateRunAsync(signalRun, ct);

        // Advance the plan to unblock downstream steps
        await AdvancePlanAsync(planRunId, ct);
    }

    // -- Operator --

    private async Task ResolveOperatorStepAsync(
        OperatorStep operatorStep, string stepId, StepContext ctx, CancellationToken ct)
    {
        var sourceRun = ctx.GetRun(operatorStep.SourceStepId);
        if (sourceRun is null) return;

        // Source must be completed (or optional and failed = null)
        string? sourceJson = sourceRun.Result;
        if (sourceJson is null && sourceRun.Status == JobStatus.Completed)
            sourceJson = "null";
        if (sourceJson is null) return;

        var sourceElement = JsonSerializer.Deserialize<JsonElement>(sourceJson, options.SerializerOptions);
        var result = OperatorEvaluator.Evaluate(operatorStep.OperatorType, sourceElement, operatorStep.Expression, options.SerializerOptions);

        var run = CreateCompletedSyntheticRun($"_op:{operatorStep.OperatorType}:{operatorStep.Id}", stepId,
            operatorStep.Name, ctx, result);
        await TryCreateStepRunAsync(run, stepId, ctx, ct);
    }

    // -- Fallback --

    private async Task CreateFallbackRunAsync(
        RunStep runStep, string stepId, StepContext ctx, CancellationToken ct)
    {
        var fallbackJobName = runStep.FallbackJobName!;

        // Check if fallback already attempted
        var existingRun = ctx.StepRuns[stepId];
        if (existingRun.JobName == fallbackJobName)
            return; // Fallback already tried

        // Get the original arguments
        var resolvedArgs = ResolveArguments(runStep.Arguments, ctx);

        var now = timeProvider.GetUtcNow();
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = fallbackJobName,
            Arguments = resolvedArgs,
            CreatedAt = now,
            NotBefore = now,
            ParentRunId = ctx.PlanRunId,
            PlanRunId = ctx.PlanRunId,
            PlanStepId = stepId,
            PlanStepName = runStep.Name,
            RetryOfRunId = existingRun.Id // Link to original run
        };

        if (await store.TryCreateRunAsync(run, ct))
        {
            ctx.StepRuns[stepId] = run;
            await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, ct);
        }
    }

    // -- Run/Stream step --

    private async Task CreateStepRunAsync(
        PlanStep step, string stepId, StepContext ctx, CancellationToken ct)
    {
        var jobStep = (JobStep)step;
        var args = jobStep.Arguments;
        var jobName = jobStep.JobName;

        // Re-entry: check if this step is a sub-plan proxy waiting for completion
        if (ctx.StepRuns.TryGetValue(stepId, out var existingRun)
            && existingRun.Status == JobStatus.Running
            && existingRun.NodeName == "_engine")
        {
            await CheckSubPlanCompletionAsync(existingRun, ct);
            return;
        }

        var resolvedArgs = ResolveArguments(args, ctx);

        // Check if the target job is a plan — if so, run it as a sub-plan
        var job = await store.GetJobAsync(jobName, ct);
        if (job?.IsPlan == true && job.PlanGraph is not null)
        {
            await CreateSubPlanRunAsync(jobName, job.PlanGraph, resolvedArgs, step, stepId, ctx, ct);
            return;
        }

        var now = timeProvider.GetUtcNow();
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Arguments = resolvedArgs,
            CreatedAt = now,
            NotBefore = now,
            ParentRunId = ctx.ParentRunId,
            PlanRunId = ctx.PlanRunId,
            PlanStepId = stepId,
            PlanStepName = step.Name
        };

        // Apply per-step overrides
        ApplyStepOverrides(run, step, ctx.StepRuns);

        if (await store.TryCreateRunAsync(run, ct))
        {
            ctx.StepRuns[stepId] = run;
            await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, ct);
        }
    }

    private async Task CheckSubPlanCompletionAsync(JobRun existingRun, CancellationToken ct)
    {
        // The sub-plan run ID is stored in the step run's Result field while Running
        var subPlanRunId = existingRun.Result;
        if (subPlanRunId is null) return;

        var subPlanRun = await store.GetRunAsync(subPlanRunId, ct);
        if (subPlanRun is null || !subPlanRun.Status.IsTerminal) return;

        var now = timeProvider.GetUtcNow();
        existingRun.Status = subPlanRun.Status == JobStatus.Completed ? JobStatus.Completed : JobStatus.Failed;
        existingRun.Result = subPlanRun.Result;
        existingRun.Error = subPlanRun.Status != JobStatus.Completed ? subPlanRun.Error : null;
        existingRun.CompletedAt = now;
        await store.UpdateRunAsync(existingRun, ct);
    }

    private async Task CreateSubPlanRunAsync(
        string jobName, string planGraph, string? resolvedArgs,
        PlanStep step, string stepId, StepContext ctx, CancellationToken ct)
    {
        var subRunId = Guid.CreateVersion7().ToString("N");
        var now = timeProvider.GetUtcNow();

        var subRun = new JobRun
        {
            Id = subRunId,
            JobName = jobName,
            Status = JobStatus.Running,
            Arguments = resolvedArgs,
            CreatedAt = now,
            StartedAt = now,
            NotBefore = now,
            ParentRunId = ctx.ParentRunId,
            PlanGraph = planGraph,
            LastHeartbeatAt = now
        };

        await store.CreateRunAsync(subRun, ct);

        // Create a synthetic Running step run that tracks the sub-plan run ID
        var stepRun = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Running,
            CreatedAt = now,
            StartedAt = now,
            NotBefore = now,
            NodeName = "_engine",
            ParentRunId = ctx.ParentRunId,
            PlanRunId = ctx.PlanRunId,
            PlanStepId = stepId,
            PlanStepName = step.Name,
            Result = subRunId, // Track the sub-plan run ID while Running
            LastHeartbeatAt = now
        };

        if (await store.TryCreateRunAsync(stepRun, ct))
            ctx.StepRuns[stepId] = stepRun;

        await AdvancePlanAsync(subRunId, ct);
    }

    // -- ForEach --

    private async Task ExpandForEachAsync(
        ForEachStep forEach, string forEachStepId, StepContext ctx, CancellationToken ct)
    {
        var sourceRun = ctx.GetRun(forEach.SourceStepId);
        if (sourceRun is null) return;

        await ExpandForEachCoreAsync(forEach, forEachStepId, sourceRun, ctx, ct);
    }

    private async Task ExpandForEachCoreAsync(
        ForEachStep forEach, string forEachInstanceId, JobRun sourceRun,
        StepContext ctx, CancellationToken ct)
    {
        // Check if source is a streaming step — items come from Output events, not Result
        var sourceStep = ctx.Graph.Steps.FirstOrDefault(s => s.Id == forEach.SourceStepId);
        if (sourceStep is Surefire.StreamStep || forEach.IsStreaming)
        {
            await ExpandStreamingForEachAsync(forEach, forEachInstanceId, sourceRun, ctx, ct);
            return;
        }

        // Array source — deserialize source result as array
        JsonElement[]? items = null;
        if (sourceRun.Result is not null)
            items = JsonSerializer.Deserialize<JsonElement[]>(sourceRun.Result, options.SerializerOptions);

        if (items is null || items.Length == 0)
        {
            await CreateEmptyForEachRunAsync(forEach, forEachInstanceId, ctx, ct);
            return;
        }

        // Create a Running synthetic run before expanding so iteration runs can
        // reference it as their ParentRunId (shows them in "Triggered runs").
        if (!ctx.StepRuns.ContainsKey(forEachInstanceId))
        {
            var syntheticRun = CreateRunningSyntheticRun($"_forEach:{forEach.Id}",
                forEachInstanceId, forEach.Name, ctx);
            await TryCreateStepRunAsync(syntheticRun, forEachInstanceId, ctx, ct);
        }

        // Heartbeat the synthetic run so run recovery doesn't kill it while iterations are in-flight
        if (ctx.StepRuns.TryGetValue(forEachInstanceId, out var forEachRun) && forEachRun.Status == JobStatus.Running)
            await store.HeartbeatRunsAsync([forEachRun.Id], ct);

        await ExpandTemplateItemsAsync(forEach, forEachInstanceId, items, ctx, ct);

        await CheckForEachCompletionAsync(forEach, forEachInstanceId, items.Length, ctx, ct);
    }

    private async Task ExpandStreamingForEachAsync(
        ForEachStep forEach, string forEachInstanceId, JobRun sourceRun,
        StepContext ctx, CancellationToken ct)
    {
        // Read Output events from the source run
        var outputEvents = await store.GetEventsAsync(sourceRun.Id, sinceId: 0,
            types: [RunEventType.Output], cancellationToken: ct);

        // Create synthetic Running run for streaming ForEach (emits Output events as iterations complete)
        if (forEach.IsStreaming && !ctx.StepRuns.ContainsKey(forEachInstanceId))
        {
            if (outputEvents.Count == 0 && sourceRun.Status.IsTerminal)
            {
                await CreateEmptyForEachRunAsync(forEach, forEachInstanceId, ctx, ct);
                return;
            }

            var syntheticRun = CreateRunningSyntheticRun($"_forEach:{forEach.Id}",
                forEachInstanceId, forEach.Name, ctx);
            await TryCreateStepRunAsync(syntheticRun, forEachInstanceId, ctx, ct);
        }

        // Non-streaming ForEach with streaming source — wait for source completion
        if (!forEach.IsStreaming && !sourceRun.Status.IsTerminal)
            return;

        // Handle empty source
        if (outputEvents.Count == 0 && sourceRun.Status.IsTerminal && !ctx.StepRuns.ContainsKey(forEachInstanceId))
        {
            await CreateEmptyForEachRunAsync(forEach, forEachInstanceId, ctx, ct);
            return;
        }

        // Heartbeat the synthetic run so run recovery doesn't kill it while iterations are in-flight
        if (ctx.StepRuns.TryGetValue(forEachInstanceId, out var streamForEachRun) && streamForEachRun.Status == JobStatus.Running)
            await store.HeartbeatRunsAsync([streamForEachRun.Id], ct);

        if (outputEvents.Count == 0) return;

        // Convert Output events to items array and expand
        var items = outputEvents
            .Select(e => JsonSerializer.Deserialize<JsonElement>(e.Payload, options.SerializerOptions))
            .ToArray();

        await ExpandTemplateItemsAsync(forEach, forEachInstanceId, items, ctx, ct);

        // For streaming ForEach, emit Output events for completed iterations (in order)
        if (forEach.IsStreaming && forEach.Template.OutputStepId is not null
            && ctx.StepRuns.TryGetValue(forEachInstanceId, out var forEachRun) && forEachRun.Status == JobStatus.Running)
        {
            var existingOutputs = await store.GetEventsAsync(forEachRun.Id, sinceId: 0,
                types: [RunEventType.Output], cancellationToken: ct);
            var emittedCount = existingOutputs.Count;

            for (var i = emittedCount; i < items.Length; i++)
            {
                var collectId = $"{forEachInstanceId}[{i}].{forEach.Template.OutputStepId}";
                if (!ctx.StepRuns.TryGetValue(collectId, out var collectRun) || collectRun.Status != JobStatus.Completed)
                    break; // Preserve order — wait for earlier iterations

                var payload = collectRun.Result ?? "null";
                await store.AppendEventAsync(new RunEvent
                {
                    RunId = forEachRun.Id,
                    EventType = RunEventType.Output,
                    Payload = payload,
                    CreatedAt = timeProvider.GetUtcNow()
                }, ct);
                await notifications.PublishAsync(NotificationChannels.RunEvent(forEachRun.Id), "", ct);
            }
        }

        // Check completion when source is terminal
        if (sourceRun.Status.IsTerminal)
        {
            await CheckForEachCompletionAsync(forEach, forEachInstanceId, items.Length, ctx, ct);
        }

        // Subscribe for responsive advancement when source is still producing
        if (sourceRun.Status == JobStatus.Running)
            await SubscribeToStreamSourceAsync(sourceRun.Id, ctx.PlanRunId);
    }

    private async Task ExpandTemplateItemsAsync(
        ForEachStep forEach, string forEachInstanceId, JsonElement[] items,
        StepContext parentCtx, CancellationToken ct)
    {
        // Iteration runs are parented to the ForEach step's own run so they appear
        // as "Triggered runs" on the ForEach run detail page.
        var forEachRunId = parentCtx.StepRuns.TryGetValue(forEachInstanceId, out var feRun) ? feRun.Id : null;

        for (var i = 0; i < items.Length; i++)
        {
            var instancePrefix = $"{forEachInstanceId}[{i}]";
            await ExpandTemplateAsync(forEach.Template, instancePrefix, parentCtx, items[i], ct, forEachRunId);
        }
    }

    private async Task CreateEmptyForEachRunAsync(
        ForEachStep forEach, string forEachInstanceId,
        StepContext ctx, CancellationToken ct)
    {
        var emptyRun = CreateCompletedSyntheticRun($"_forEach:{forEach.Id}",
            forEachInstanceId, forEach.Name, ctx, "[]");
        await TryCreateStepRunAsync(emptyRun, forEachInstanceId, ctx, ct);
    }

    private async Task CheckForEachCompletionAsync(
        ForEachStep forEach, string forEachInstanceId, int itemCount,
        StepContext ctx, CancellationToken ct)
    {
        // ForEach is complete when all template steps across all iterations are terminal
        for (var i = 0; i < itemCount; i++)
        {
            foreach (var templateStep in forEach.Template.Steps)
            {
                var instanceId = $"{forEachInstanceId}[{i}].{templateStep.Id}";
                if (!ctx.StepRuns.TryGetValue(instanceId, out var run) || !run.Status.IsTerminal)
                    return; // Not yet complete
            }
        }

        if (!ctx.StepRuns.TryGetValue(forEachInstanceId, out var forEachRun) || forEachRun.Status.IsTerminal)
            return;

        // Streaming ForEach: emit remaining Output events and OutputComplete before finalizing
        if (forEach.IsStreaming)
            await EmitStreamingOutputEventsAsync(forEach, forEachInstanceId, forEachRun, itemCount, ctx, ct);

        // Collect results
        string? result = null;
        if (forEach.Template.OutputStepId is not null)
        {
            var results = new JsonArray();
            for (var i = 0; i < itemCount; i++)
            {
                var collectId = $"{forEachInstanceId}[{i}].{forEach.Template.OutputStepId}";
                if (ctx.StepRuns.TryGetValue(collectId, out var collectRun) && collectRun.Result is not null)
                    results.Add(JsonNode.Parse(collectRun.Result));
                else
                    results.Add(null);
            }
            result = results.ToJsonString(options.SerializerOptions);
        }

        // Determine ForEach status — failed if any iteration step failed
        var anyFailed = false;
        for (var i = 0; i < itemCount && !anyFailed; i++)
        {
            foreach (var templateStep in forEach.Template.Steps)
            {
                var instanceId = $"{forEachInstanceId}[{i}].{templateStep.Id}";
                if (ctx.StepRuns.TryGetValue(instanceId, out var r) && r.Status is JobStatus.Failed or JobStatus.DeadLetter)
                {
                    anyFailed = true;
                    break;
                }
            }
        }

        // Transition to terminal
        var now = timeProvider.GetUtcNow();
        forEachRun.Status = anyFailed ? JobStatus.Failed : JobStatus.Completed;
        forEachRun.Result = result;
        forEachRun.CompletedAt = now;
        await store.UpdateRunAsync(forEachRun, ct);
    }

    private async Task EmitStreamingOutputEventsAsync(
        ForEachStep forEach, string forEachInstanceId, JobRun forEachRun,
        int itemCount, StepContext ctx, CancellationToken ct)
    {
        if (forEach.Template.OutputStepId is not null)
        {
            var existingOutputs = await store.GetEventsAsync(forEachRun.Id, sinceId: 0,
                types: [RunEventType.Output], cancellationToken: ct);
            var emittedCount = existingOutputs.Count;

            for (var i = emittedCount; i < itemCount; i++)
            {
                var collectId = $"{forEachInstanceId}[{i}].{forEach.Template.OutputStepId}";
                if (ctx.StepRuns.TryGetValue(collectId, out var collectRun) && collectRun.Status == JobStatus.Completed)
                {
                    var payload = collectRun.Result ?? "null";
                    await store.AppendEventAsync(new RunEvent
                    {
                        RunId = forEachRun.Id,
                        EventType = RunEventType.Output,
                        Payload = payload,
                        CreatedAt = timeProvider.GetUtcNow()
                    }, ct);
                    await notifications.PublishAsync(NotificationChannels.RunEvent(forEachRun.Id), "", ct);
                }
            }
        }

        await store.AppendEventAsync(new RunEvent
        {
            RunId = forEachRun.Id,
            EventType = RunEventType.OutputComplete,
            Payload = "{}",
            CreatedAt = timeProvider.GetUtcNow()
        }, ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(forEachRun.Id), "", ct);
    }

    private async Task SubscribeToStreamSourceAsync(string sourceRunId, string planRunId)
    {
        var subKey = $"{planRunId}:{sourceRunId}";
        if (_streamSubscriptions.ContainsKey(subKey)) return;

        var sub = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(sourceRunId),
            _msg =>
            {
                Task.Run(async () =>
                {
                    try { await AdvancePlanAsync(planRunId, CancellationToken.None); }
                    catch (Exception ex) { logger.LogError(ex, "Failed to advance plan {PlanRunId} from stream subscription", planRunId); }
                });
                return Task.CompletedTask;
            },
            CancellationToken.None);

        if (!_streamSubscriptions.TryAdd(subKey, sub))
            await sub.DisposeAsync();
    }

    private async Task CleanupStreamSubscriptionsAsync(string planRunId)
    {
        var prefix = $"{planRunId}:";
        foreach (var key in _streamSubscriptions.Keys)
        {
            if (key.StartsWith(prefix) && _streamSubscriptions.TryRemove(key, out var sub))
            {
                try { await sub.DisposeAsync(); }
                catch { /* best-effort */ }
            }
        }
    }

    // -- Step overrides --

    private void ApplyStepOverrides(JobRun run, PlanStep step, Dictionary<string, JobRun> stepRuns)
    {
        if (step is not JobStep jobStep) return;

        if (jobStep.PriorityOverride is not null)
            run.Priority = ResolveOverrideValue<int>(jobStep.PriorityOverride, stepRuns);

        if (jobStep.NotBeforeOverride is not null)
            run.NotBefore = ResolveOverrideValue<DateTimeOffset>(jobStep.NotBeforeOverride, stepRuns);

        if (jobStep.DeduplicationIdOverride is not null)
            run.DeduplicationId = ResolveOverrideValue<string>(jobStep.DeduplicationIdOverride, stepRuns);
    }

    private T ResolveOverrideValue<T>(StepValue value, Dictionary<string, JobRun> stepRuns)
    {
        if (value is ConstantValue cv)
            return JsonSerializer.Deserialize<T>(cv.Value.GetRawText(), options.SerializerOptions)!;

        if (value is StepRefValue sr && stepRuns.TryGetValue(sr.StepId, out var refRun) && refRun.Result is not null)
            return JsonSerializer.Deserialize<T>(refRun.Result, options.SerializerOptions)!;

        return default!;
    }

    // -- Failure policy --

    private async Task ApplyFailurePolicyAsync(
        PlanGraph graph, Dictionary<string, JobRun> stepRuns,
        string planRunId, CancellationToken ct)
    {
        // Find any failed steps (terminal + not completed + not skipped)
        var failedSteps = stepRuns
            .Where(kvp => kvp.Value.Status.IsTerminal
                && kvp.Value.Status is not (JobStatus.Completed or JobStatus.Skipped))
            .Select(kvp => kvp.Key)
            .ToHashSet();

        if (failedSteps.Count == 0) return;

        // Exclude optional steps from failure tracking
        var nonOptionalFailedSteps = failedSteps
            .Where(id =>
            {
                var step = graph.Steps.FirstOrDefault(s => s.Id == id);
                return step is not null && !step.IsOptional;
            })
            .ToHashSet();

        // Also exclude steps that have fallbacks still pending
        nonOptionalFailedSteps.RemoveWhere(id =>
        {
            var step = graph.Steps.FirstOrDefault(s => s.Id == id);
            if (step is RunStep rs && rs.FallbackJobName is not null)
            {
                // Check if the fallback has been tried
                if (stepRuns.TryGetValue(id, out var run) && run.JobName != rs.FallbackJobName)
                    return true; // Fallback not yet attempted, don't treat as failed
            }
            return false;
        });

        if (nonOptionalFailedSteps.Count == 0) return;

        if (graph.FailurePolicy == StepFailurePolicy.ContinueAll) return;

        if (graph.FailurePolicy == StepFailurePolicy.CancelAll)
        {
            // Cancel all non-terminal steps
            foreach (var step in graph.Steps)
            {
                if (stepRuns.TryGetValue(step.Id, out var run) && !run.Status.IsTerminal)
                {
                    await CancelStepRunAsync(run, ct);
                }
            }
        }
        else // CancelDependents
        {
            // Find all steps that transitively depend on failed steps
            var toCancelIds = FindTransitiveDependents(graph, nonOptionalFailedSteps);

            foreach (var stepId in toCancelIds)
            {
                if (stepRuns.TryGetValue(stepId, out var run) && !run.Status.IsTerminal)
                {
                    await CancelStepRunAsync(run, ct);
                }
                else if (!stepRuns.ContainsKey(stepId))
                {
                    // Step not yet created — mark as cancelled so it's never created
                    var step = graph.Steps.FirstOrDefault(s => s.Id == stepId);
                    if (step is null) continue;

                    // Skip steps with AllDone trigger rule — they should still run
                    if (step.TriggerRule == TriggerRule.AllDone) continue;

                    var now = timeProvider.GetUtcNow();
                    var cancelledRun = new JobRun
                    {
                        Id = Guid.CreateVersion7().ToString("N"),
                        JobName = GetStepJobName(step),
                        Status = JobStatus.Cancelled,
                        CreatedAt = now,
                        CompletedAt = now,
                        CancelledAt = now,
                        NotBefore = now,
                        ParentRunId = planRunId,
                        PlanRunId = planRunId,
                        PlanStepId = stepId,
                        PlanStepName = step.Name
                    };

                    if (await store.TryCreateRunAsync(cancelledRun, ct))
                        stepRuns[stepId] = cancelledRun;
                }
            }
        }
    }

    private static HashSet<string> FindTransitiveDependents(PlanGraph graph, HashSet<string> failedIds)
    {
        // Find all steps that transitively depend on failed steps.
        // AllDone steps are included in traversal (their dependents may need cancelling)
        // but are marked separately — the caller decides whether to cancel them.
        var dependents = new HashSet<string>();
        var queue = new Queue<string>(failedIds);

        while (queue.Count > 0)
        {
            var failedId = queue.Dequeue();

            foreach (var step in graph.Steps)
            {
                if (dependents.Contains(step.Id)) continue;

                if (step.DependsOn.Contains(failedId))
                {
                    dependents.Add(step.Id);
                    queue.Enqueue(step.Id);
                }
            }
        }

        return dependents;
    }

    private async Task CancelStepRunAsync(JobRun run, CancellationToken ct)
    {
        if (run.Status == JobStatus.Pending)
        {
            var now = timeProvider.GetUtcNow();
            var update = new JobRun
            {
                Id = run.Id,
                JobName = run.JobName,
                Status = JobStatus.Cancelled,
                CompletedAt = now,
                CancelledAt = now
            };
            if (await store.TryUpdateRunStatusAsync(update, JobStatus.Pending, ct))
            {
                var payload = JsonSerializer.Serialize(
                    new { status = (int)JobStatus.Cancelled }, options.SerializerOptions);
                await store.AppendEventAsync(new RunEvent
                {
                    RunId = run.Id,
                    EventType = RunEventType.Status,
                    Payload = payload,
                    CreatedAt = now
                }, ct);
                await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), "", ct);
                await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), "", ct);
            }
        }
        else if (run.Status == JobStatus.Running)
        {
            await notifications.PublishAsync(NotificationChannels.RunCancel(run.Id), "", ct);
        }
    }

    // -- Completion --

    /// <returns>true if the plan reached terminal state</returns>
    private async Task<bool> CheckPlanCompletionAsync(
        JobRun planRun, PlanGraph graph, Dictionary<string, JobRun> stepRuns, CancellationToken ct)
    {
        // Plan is complete when all top-level steps have runs in terminal state
        var allTerminal = true;
        var anyFailed = false;

        foreach (var step in graph.Steps)
        {
            if (!stepRuns.TryGetValue(step.Id, out var run))
            {
                allTerminal = false;
                break;
            }

            if (!run.Status.IsTerminal)
            {
                allTerminal = false;
                break;
            }

            // Skipped steps don't count as failures
            if (run.Status is JobStatus.Failed or JobStatus.DeadLetter)
            {
                // Optional steps that failed don't count as plan failure
                if (!step.IsOptional)
                    anyFailed = true;
            }
        }

        if (!allTerminal) return false;
        if (planRun.Status.IsTerminal) return true;

        // All steps terminal — complete the plan
        var now = timeProvider.GetUtcNow();
        planRun.Status = anyFailed ? JobStatus.Failed : JobStatus.Completed;
        planRun.CompletedAt = now;
        planRun.Progress = 1;

        // If there's an output step, copy its result to the plan run
        if (graph.OutputStepId is not null && stepRuns.TryGetValue(graph.OutputStepId, out var outputRun))
        {
            if (outputRun.Status == JobStatus.Skipped)
            {
                // Skipped output step = plan completes with null result
                planRun.Result = null;
            }
            else
            {
                planRun.Result = outputRun.Result;
                if (outputRun.Status != JobStatus.Completed)
                {
                    planRun.Error = outputRun.Error;
                    planRun.Status = JobStatus.Failed;
                }
            }
        }

        if (anyFailed && planRun.Error is null)
        {
            var failedSteps = stepRuns.Values
                .Where(r => r.Status is JobStatus.Failed or JobStatus.DeadLetter)
                .Select(r => r.PlanStepName ?? r.PlanStepId ?? r.JobName);
            planRun.Error = $"Plan failed: steps [{string.Join(", ", failedSteps)}] failed.";
        }

        await store.UpdateRunAsync(planRun, ct);
        await notifications.PublishAsync(NotificationChannels.RunCompleted(planRun.Id), "", ct);
        await CleanupStreamSubscriptionsAsync(planRun.Id);
        return true;
    }

    // -- Cancel plan --

    /// <summary>
    /// Cancels a plan run and all its step runs. Called by JobClient.CancelAsync
    /// when the target is a plan run.
    /// </summary>
    internal async Task CancelPlanAsync(string planRunId, CancellationToken ct)
    {
        var semaphore = _planLocks.GetOrAdd(planRunId, _ => new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync(ct);
        try
        {
            var planRun = await store.GetRunAsync(planRunId, ct);
            if (planRun is null || planRun.Status.IsTerminal) return;

            // Cancel all non-terminal step runs (paginate to handle large plans)
            var skip = 0;
            const int pageSize = 500;
            while (true)
            {
                var page = await store.GetRunsAsync(
                    new RunFilter { PlanRunId = planRunId, Take = pageSize, Skip = skip }, ct);

                foreach (var stepRun in page.Items)
                {
                    if (stepRun.Status.IsTerminal) continue;
                    await CancelStepRunAsync(stepRun, ct);
                }

                if (page.Items.Count < pageSize) break;
                skip += pageSize;
            }

            // Mark the plan itself as cancelled
            var now = timeProvider.GetUtcNow();
            planRun.Status = JobStatus.Cancelled;
            planRun.CompletedAt = now;
            planRun.CancelledAt = now;
            await store.UpdateRunAsync(planRun, ct);
            await notifications.PublishAsync(NotificationChannels.RunCompleted(planRunId), "", ct);
            await CleanupStreamSubscriptionsAsync(planRunId);

            _planLocks.TryRemove(planRunId, out _);
        }
        finally
        {
            semaphore.Release();
        }
    }

    // -- Reconciliation --

    public async Task ReconcileActivePlansAsync(CancellationToken ct)
    {
        // Find running plan runs. Plan runs are distinguished by having a non-null PlanGraph.
        // We query all running runs and filter, since there's no dedicated PlanGraph filter on RunFilter.
        // Paginate to ensure we don't miss plans when there are many concurrent running runs.
        var activePlanIds = new HashSet<string>();
        var skip = 0;
        const int pageSize = 500;
        while (true)
        {
            var runningRuns = await store.GetRunsAsync(new RunFilter
            {
                Status = JobStatus.Running,
                Take = pageSize,
                Skip = skip
            }, ct);

            foreach (var run in runningRuns.Items)
            {
                if (run.PlanGraph is null) continue;

                activePlanIds.Add(run.Id);
                try
                {
                    await AdvancePlanAsync(run.Id, ct);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to reconcile plan run {RunId}", run.Id);
                }
            }

            if (runningRuns.Items.Count < pageSize) break;
            skip += pageSize;
        }

        // Clean up stale semaphore entries for plans no longer active on this node.
        // This covers the case where a plan completed on another node and this node
        // never processed its terminal state to trigger the normal cleanup path.
        foreach (var planId in _planLocks.Keys)
        {
            if (!activePlanIds.Contains(planId))
                _planLocks.TryRemove(planId, out _);
        }
    }

    // -- Argument resolution --

    private string? ResolveArguments(Dictionary<string, StepValue>? arguments, StepContext ctx)
    {
        if (arguments is null || arguments.Count == 0) return null;

        var resolved = new JsonObject();
        foreach (var (key, value) in arguments)
        {
            switch (value)
            {
                case ConstantValue cv:
                    resolved[key] = JsonNode.Parse(cv.Value.GetRawText());
                    break;

                case StepRefValue sr:
                    var refRun = ctx.GetRun(sr.StepId);
                    if (refRun is not null)
                    {
                        if (refRun.Result is not null)
                            resolved[key] = JsonNode.Parse(refRun.Result);
                        // If run exists but Result is null, don't add anything (null result)
                    }
                    else
                    {
                        // No run found — may be optional and failed/skipped
                        if (ctx.StepRuns.TryGetValue(sr.StepId, out var optionalRun)
                            && optionalRun.Status is JobStatus.Failed or JobStatus.DeadLetter or JobStatus.Skipped)
                            resolved[key] = null;
                    }
                    break;

                case StreamRefValue sr:
                    var streamRun = ctx.GetRun(sr.StepId);
                    if (streamRun is not null)
                        resolved[key] = new JsonObject { ["$planStream"] = JsonValue.Create(streamRun.Id) };
                    break;

                case ItemValue:
                    if (ctx.ItemValue is not null)
                        resolved[key] = JsonNode.Parse(ctx.ItemValue.Value.GetRawText());
                    break;
            }
        }

        return resolved.Count > 0 ? resolved.ToJsonString(options.SerializerOptions) : null;
    }

    private static string GetStepJobName(PlanStep step) => step switch
    {
        JobStep js => js.JobName,
        ForEachStep fe => $"_forEach:{fe.Id}",
        WhenAllStep wa => $"_whenAll:{wa.Id}",
        InputStep ip => $"_input:{ip.ParamName}",
        IfStep ifs => $"_if:{ifs.Id}",
        SwitchStep sw => $"_switch:{sw.Id}",
        SignalStep sig => $"_signal:{sig.SignalName}",
        OperatorStep op => $"_op:{op.OperatorType}:{op.Id}",
        _ => $"_step:{step.Id}"
    };

    /// <summary>
    /// Carries the differences between top-level step processing and template step processing.
    /// Top-level: InstancePrefix is null, Graph is the plan's graph.
    /// ForEach template: InstancePrefix is "stepId[i]" (per iteration index).
    /// If template: InstancePrefix is "stepId[then0]" or "stepId[else]" (per branch).
    /// Switch template: InstancePrefix is "stepId[caseValue]" or "stepId[default]" (per case).
    /// </summary>
    private sealed class StepContext(
        PlanGraph graph,
        string planRunId,
        Dictionary<string, JobRun> stepRuns,
        string? instancePrefix = null,
        JsonElement? itemValue = null,
        string? parentRunIdOverride = null)
    {
        public PlanGraph Graph => graph;
        public string PlanRunId => planRunId;
        public Dictionary<string, JobRun> StepRuns => stepRuns;
        public string? InstancePrefix => instancePrefix;
        public JsonElement? ItemValue => itemValue;
        /// <summary>When set, step runs use this as ParentRunId instead of PlanRunId (e.g. ForEach iteration runs).</summary>
        public string ParentRunId => parentRunIdOverride ?? planRunId;

        /// <summary>Maps a template step ID to its instance-scoped ID (or returns it unchanged for top-level).</summary>
        public string MapStepId(string templateStepId) =>
            instancePrefix is not null ? $"{instancePrefix}.{templateStepId}" : templateStepId;

        /// <summary>Resolves a run for a template step ID, checking instance-scoped first, then global.</summary>
        public JobRun? GetRun(string templateStepId)
        {
            var id = MapStepId(templateStepId);
            if (stepRuns.TryGetValue(id, out var run)) return run;
            if (instancePrefix is not null)
                stepRuns.TryGetValue(templateStepId, out run);
            return run;
        }
    }
}
