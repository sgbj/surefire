using System.Reflection;

namespace Surefire.Tests.Integration;

/// <summary>
///     Conformance registry that pins every public method on <see cref="IJobClient" /> to an
///     expected durable behavior. When a new method is added to the interface without a registered
///     behavior here, the test fails, so the engineer adding the method has to consciously decide
///     what happens inside a durable orchestrator.
/// </summary>
public sealed class DurableConformanceTests
{
    /// <summary>
    ///     Durable-mode behavior categories. Every <see cref="IJobClient" /> method must be
    ///     mapped to one of these; the mapping is the durable contract.
    /// </summary>
    public enum DurableBehavior
    {
        /// <summary>
        ///     Creates a new run/batch. Derives a deterministic id from the orchestrator + step
        ///     counter, records a StepRecorded event atomically with the creation, returns the
        ///     existing record on replay. Examples: TriggerAsync, TriggerBatchAsync, RunAsync,
        ///     StreamAsync, RunBatchAsync, StreamBatchAsync, RerunAsync.
        /// </summary>
        StepRecording,

        /// <summary>
        ///     Awaits an existing run/batch terminal. Throws <c>DurableYieldException</c> if
        ///     non-terminal so the executor can suspend the orchestrator; returns recorded
        ///     state on resume. Examples: WaitAsync, WaitBatchAsync, ObserveRunEventsAsync,
        ///     ObserveBatchEventsAsync, WaitEachAsync, WaitStreamAsync.
        /// </summary>
        YieldOnIncomplete,

        /// <summary>
        ///     Read-only or trivially idempotent. Same behavior in durable and non-durable mode.
        ///     Examples: GetRunAsync, GetRunsAsync, GetBatchAsync, CancelAsync, CancelBatchAsync.
        /// </summary>
        Passthrough
    }

    /// <summary>
    ///     Source-of-truth mapping. Add a new entry when a new <see cref="IJobClient" /> method
    ///     ships; CI will fail until the entry is added. The key is the method's reflection
    ///     signature (name + parameter type names), so overloads are tracked separately.
    /// </summary>
    private static readonly Dictionary<string, DurableBehavior> ExpectedBehaviors = new(StringComparer.Ordinal)
    {
        // -------- StepRecording (creates state, deterministic id) --------
        ["TriggerAsync(String,Object,RunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["TriggerAsync(String,RunArguments,RunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["TriggerBatchAsync(IEnumerable`1,CancellationToken)"] = DurableBehavior.StepRecording,
        ["TriggerBatchAsync(String,IEnumerable`1,BatchRunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["RunAsync(String,Object,RunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["RunAsync(String,RunArguments,RunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["RunBatchAsync(String,IEnumerable`1,BatchRunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["RunBatchAsync(IEnumerable`1,CancellationToken)"] = DurableBehavior.StepRecording,
        ["StreamAsync(String,Object,RunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["StreamAsync(String,RunArguments,RunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["StreamBatchAsync(String,IEnumerable`1,BatchRunOptions,CancellationToken)"] = DurableBehavior.StepRecording,
        ["StreamBatchAsync(IEnumerable`1,CancellationToken)"] = DurableBehavior.StepRecording,
        ["RerunAsync(String,CancellationToken)"] = DurableBehavior.StepRecording,

        // -------- YieldOnIncomplete (awaits terminal, yields if not) --------
        ["WaitAsync(String,CancellationToken)"] = DurableBehavior.YieldOnIncomplete,
        ["WaitBatchAsync(String,CancellationToken)"] = DurableBehavior.YieldOnIncomplete,
        ["WaitEachAsync(String,CancellationToken)"] = DurableBehavior.YieldOnIncomplete,
        ["WaitStreamAsync(String,CancellationToken)"] = DurableBehavior.YieldOnIncomplete,
        ["ObserveRunEventsAsync(String,Int64,CancellationToken)"] = DurableBehavior.YieldOnIncomplete,
        ["ObserveBatchEventsAsync(String,Int64,CancellationToken)"] = DurableBehavior.YieldOnIncomplete,

        // -------- Passthrough (read-only, or idempotent on existing state) --------
        ["GetRunAsync(String,CancellationToken)"] = DurableBehavior.Passthrough,
        ["GetRunsAsync(RunFilter,CancellationToken)"] = DurableBehavior.Passthrough,
        ["GetBatchAsync(String,CancellationToken)"] = DurableBehavior.Passthrough,
        ["CancelAsync(String,CancellationToken)"] = DurableBehavior.Passthrough,
        ["CancelBatchAsync(String,CancellationToken)"] = DurableBehavior.Passthrough
    };

    [Fact]
    public void Every_IJobClient_Method_Has_A_Registered_Durable_Behavior()
    {
        var methods = typeof(IJobClient).GetMethods(BindingFlags.Public | BindingFlags.Instance);
        var unregistered = new List<string>();

        foreach (var method in methods)
        {
            var key = SignatureKey(method);
            if (!ExpectedBehaviors.ContainsKey(key))
            {
                unregistered.Add(key);
            }
        }

        Assert.True(unregistered.Count == 0,
            "Every IJobClient method must be registered in DurableConformanceTests.ExpectedBehaviors. " +
            "Unregistered methods:\n  " + string.Join("\n  ", unregistered) +
            "\n\nAdd an entry mapping each signature to a DurableBehavior so the durable contract is explicit. " +
            "If the method should yield, also confirm it throws DurableYieldException; if it records a step, " +
            "confirm it passes a DurableStepRecord through to the store so the orchestrator's " +
            "highest_recorded_step advances atomically with the child creation.");
    }

    [Fact]
    public void Registry_Does_Not_Reference_Removed_Methods()
    {
        var methods = typeof(IJobClient).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Select(SignatureKey)
            .ToHashSet(StringComparer.Ordinal);

        var stale = ExpectedBehaviors.Keys.Where(k => !methods.Contains(k)).ToList();

        Assert.True(stale.Count == 0,
            "DurableConformanceTests.ExpectedBehaviors references signatures that no longer exist on " +
            "IJobClient. Remove or update:\n  " + string.Join("\n  ", stale));
    }

    private static string SignatureKey(MethodInfo method)
    {
        var paramTypes = method.GetParameters().Select(p =>
        {
            var t = p.ParameterType;
            // For generic types, use the name without the parameter assembly qualification so the
            // key is stable across builds. e.g. IEnumerable`1.
            return t.IsGenericType ? t.GetGenericTypeDefinition().Name : t.Name;
        });
        return $"{method.Name}({string.Join(",", paramTypes)})";
    }
}
