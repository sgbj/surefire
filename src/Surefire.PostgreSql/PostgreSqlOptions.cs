using System.Text.RegularExpressions;
using Npgsql;

namespace Surefire.PostgreSql;

public sealed partial class PostgreSqlOptions : IAsyncDisposable
{
    private string _schema = "surefire";
    private NpgsqlDataSource? _dataSource;

    public required string ConnectionString { get; set; }

    public string Schema
    {
        get => _schema;
        set
        {
            if (!SafeIdentifierRegex().IsMatch(value))
                throw new ArgumentException($"Schema name '{value}' is not a valid SQL identifier. Only letters, digits, and underscores are allowed.", nameof(value));
            _schema = value;
        }
    }

    /// <summary>
    /// Gets the shared NpgsqlDataSource for this connection string.
    /// Lazily created on first access; shared by the job store and notification provider.
    /// </summary>
    internal NpgsqlDataSource DataSource =>
        LazyInitializer.EnsureInitialized(ref _dataSource, () => NpgsqlDataSource.Create(ConnectionString));

    public async ValueTask DisposeAsync()
    {
        if (_dataSource is not null)
            await _dataSource.DisposeAsync();
    }

    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    private static partial Regex SafeIdentifierRegex();
}
