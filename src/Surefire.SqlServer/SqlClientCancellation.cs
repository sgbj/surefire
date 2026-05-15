using Microsoft.Data.SqlClient;

namespace Surefire.SqlServer;

/// <summary>
///     Normalizes SqlClient cancellation behavior to <see cref="OperationCanceledException" />.
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

    private static bool IsCancellation(SqlException ex, CancellationToken cancellationToken) =>
        cancellationToken.IsCancellationRequested
        && (ex.Number == 0 || ex.InnerException is OperationCanceledException);
}
