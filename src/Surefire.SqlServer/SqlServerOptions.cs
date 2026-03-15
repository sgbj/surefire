using System.Text.RegularExpressions;

namespace Surefire.SqlServer;

public sealed partial class SqlServerOptions
{
    private string _schema = "surefire";

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

    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    private static partial Regex SafeIdentifierRegex();
}
