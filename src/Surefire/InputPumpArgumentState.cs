namespace Surefire;

/// <summary>
///     Per-argument input pump state used to resume an interrupted stream pump after a host crash.
/// </summary>
/// <param name="LastSequence">The highest <c>Input</c> event sequence recorded for the argument.</param>
/// <param name="InputComplete">True if an <c>InputComplete</c> event has been recorded for the argument.</param>
public readonly record struct InputPumpArgumentState(long LastSequence, bool InputComplete);
