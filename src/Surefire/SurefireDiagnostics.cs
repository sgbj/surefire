namespace Surefire;

/// <summary>
///     Diagnostic source names emitted by Surefire. Use with OpenTelemetry:
///     <c>tracing.AddSource(SurefireDiagnostics.ActivitySourceName)</c> and
///     <c>metrics.AddMeter(SurefireDiagnostics.MeterName)</c>.
/// </summary>
public static class SurefireDiagnostics
{
    /// <summary>
    ///     The name of the <see cref="System.Diagnostics.ActivitySource" /> Surefire emits spans to.
    /// </summary>
    public const string ActivitySourceName = "surefire";

    /// <summary>
    ///     The name of the <see cref="System.Diagnostics.Metrics.Meter" /> Surefire emits metrics to.
    /// </summary>
    public const string MeterName = "surefire";
}
