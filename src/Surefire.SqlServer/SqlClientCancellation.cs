using Microsoft.Data.SqlClient;

namespace Surefire.SqlServer;

/// <summary>
///     Microsoft.Data.SqlClient surfaces caller-driven cancellation as a <see cref="SqlException" />
///     with message "Operation cancelled by user." rather than the
///     <see cref="OperationCanceledException" /> required by the .NET cancellation contract.
///     These helpers normalize that quirk at the SqlClient call boundary so the rest of the
///     codebase can rely on the standard contract.
/// </summary>
internal static class SqlClientCancellation
{
    public static async Task WithSqlCancellation(this Task task, CancellationToken cancellationToken)
    {
        try
        {
            await task;
        }
        catch (SqlException ex) when (IsCancellation(ex, cancellationToken))
        {
            throw new OperationCanceledException("The operation was canceled.", ex, cancellationToken);
        }
    }

    public static async Task<T> WithSqlCancellation<T>(this Task<T> task, CancellationToken cancellationToken)
    {
        try
        {
            return await task;
        }
        catch (SqlException ex) when (IsCancellation(ex, cancellationToken))
        {
            throw new OperationCanceledException("The operation was canceled.", ex, cancellationToken);
        }
    }

    public static async ValueTask<T> WithSqlCancellation<T>(this ValueTask<T> task, CancellationToken cancellationToken)
    {
        try
        {
            return await task;
        }
        catch (SqlException ex) when (IsCancellation(ex, cancellationToken))
        {
            throw new OperationCanceledException("The operation was canceled.", ex, cancellationToken);
        }
    }

    // SqlClient surfaces caller-driven cancel as SqlException with Number == 0 (and either
    // an OperationCanceledException inner or no inner). Pairing that with the caller's token
    // state avoids misclassifying unrelated SqlExceptions that happen to coincide with a
    // cancelled token.
    private static bool IsCancellation(SqlException ex, CancellationToken cancellationToken) =>
        cancellationToken.IsCancellationRequested
        && (ex.Number == 0 || ex.InnerException is OperationCanceledException);
}
