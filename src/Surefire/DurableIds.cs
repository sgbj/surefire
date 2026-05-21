using System.Security.Cryptography;
using System.Text;

namespace Surefire;

/// <summary>
///     Deterministic id helpers used by the durable orchestrator subsystem. Each child run /
///     batch created from inside a durable handler is assigned an id derived from the
///     orchestrator's run id and a monotonic step counter, so replay re-derives the same id and
///     finds the existing record instead of creating a duplicate.
/// </summary>
internal static class DurableIds
{
    /// <summary>
    ///     Deterministic dedup id for a child run created on behalf of orchestrator step
    ///     <paramref name="step" />. Shared across replay attempts so re-issuing the same step
    ///     never creates a duplicate child.
    /// </summary>
    public static string DedupId(string orchestratorRunId, int step) =>
        $"durable:{orchestratorRunId}:step:{step}";

    /// <summary>
    ///     Deterministic 32-char-hex run id for a child created at orchestrator step
    ///     <paramref name="step" />. Lets the orchestrator look the child up directly on replay
    ///     even when the original <c>TryCreateRunAsync</c> succeeded but the host crashed before
    ///     the orchestrator observed the result.
    /// </summary>
    public static string DerivedRunId(string orchestratorRunId, int step)
    {
        Span<byte> input = stackalloc byte[Encoding.UTF8.GetMaxByteCount(orchestratorRunId.Length) + 32];
        var written = Encoding.UTF8.GetBytes(orchestratorRunId, input);
        var stepSpan = input[written..];
        var stepText = $":step:{step}";
        var stepBytes = Encoding.UTF8.GetBytes(stepText, stepSpan);
        var total = written + stepBytes;

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(input[..total], hash);
        return Convert.ToHexString(hash[..16]).ToLowerInvariant();
    }

    /// <summary>
    ///     Deterministic 32-char-hex batch id for a batch created at orchestrator step
    ///     <paramref name="step" />.
    /// </summary>
    public static string DerivedBatchId(string orchestratorRunId, int step) =>
        DerivedRunId(orchestratorRunId, step);

    /// <summary>
    ///     Deterministic 32-char-hex run id for one of a batch's children, derived from the
    ///     batch's deterministic id and the child's positional index.
    /// </summary>
    public static string DerivedBatchChildRunId(string batchId, int index)
    {
        Span<byte> input = stackalloc byte[Encoding.UTF8.GetMaxByteCount(batchId.Length) + 32];
        var written = Encoding.UTF8.GetBytes(batchId, input);
        var indexSpan = input[written..];
        var indexText = $":child:{index}";
        var indexBytes = Encoding.UTF8.GetBytes(indexText, indexSpan);
        var total = written + indexBytes;

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(input[..total], hash);
        return Convert.ToHexString(hash[..16]).ToLowerInvariant();
    }
}
