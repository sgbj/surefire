namespace Surefire;

/// <summary>
///     Thrown internally to suspend a durable orchestrator that is waiting on a non-terminal
///     child or batch.
/// </summary>
internal sealed class DurableYieldException : Exception
{
    public DurableYieldException()
        : base("Durable orchestrator yielding pending child.")
    {
    }
}
