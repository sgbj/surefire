namespace Surefire;

/// <summary>
///     Identifies the type of a <see cref="RunEvent" />.
/// </summary>
public enum RunEventType : short
{
    /// <summary>A status change event.</summary>
    Status = 0,

    /// <summary>A log message emitted by the job.</summary>
    Log = 1,

    /// <summary>A progress update.</summary>
    Progress = 2,

    /// <summary>An output value produced by the job.</summary>
    Output = 3,

    /// <summary>Signals that the output stream is complete.</summary>
    OutputComplete = 4,

    /// <summary>An input value sent to the job.</summary>
    Input = 5,

    /// <summary>Signals that the input stream is complete.</summary>
    InputComplete = 6,

    /// <summary>A structured failure record for a specific retry attempt.</summary>
    AttemptFailure = 7,

    /// <summary>Declares which input streams will be sent to this run.</summary>
    InputDeclared = 8
}