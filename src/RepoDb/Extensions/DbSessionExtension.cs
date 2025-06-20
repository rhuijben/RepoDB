using System.Data.Common;

namespace RepoDb;

/// <summary>
/// Contains the extension methods for the <see cref="DbSession"/> struct.
/// </summary>
public static partial class DbSessionExtension
{
    public static DbSession AsDbSession(this DbConnection connection)
    {
        return new DbSession(connection);
    }

    public static DbSession AsDbSession(this DbConnection connection, DbTransaction? transaction)
    {
        return transaction is { } t ? new DbSession(transaction) : new DbSession(connection);
    }

    public static DbSession AsDbSession(this DbTransaction transaction)
    {
        return new DbSession(transaction);
    }
}
