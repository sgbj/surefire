using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Surefire;

/// <summary>
///     Test-only extensions that deserialize <see cref="JobRun.Result" /> to a typed value.
///     Production code uses <see cref="IJobClient.WaitAsync{T}" /> / <see cref="IJobClient.WaitStreamAsync{T}" />;
///     these helpers let existing tests keep their "trigger + inspect snapshot" pattern without
///     re-awaiting the hydrated APIs (which would issue an extra round trip).
/// </summary>
internal static class TestHydrationExtensions
{
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public static T GetResult<T>(this JobRun run) =>
        run.Result is null
            ? throw new InvalidOperationException("Run did not produce a result.")
            : JsonSerializer.Deserialize<T>(run.Result, run.SerializerOptions)!;

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public static bool TryGetResult<T>(this JobRun run, out T? value)
    {
        if (run.Result is null)
        {
            value = default;
            return false;
        }

        value = JsonSerializer.Deserialize<T>(run.Result, run.SerializerOptions);
        return true;
    }
}