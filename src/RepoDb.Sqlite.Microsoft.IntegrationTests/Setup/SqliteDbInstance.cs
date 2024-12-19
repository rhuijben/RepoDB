using System.Data.Common;
using Microsoft.Data.Sqlite;
using RepoDb.TestCore;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests.Setup;

public class SqliteDbInstance : DbInstance<SqliteConnection>
{
    readonly SqliteConnection _conn;

    static SqliteDbInstance()
    {
        GlobalConfiguration.Setup(GlobalConfiguration.Options).UseSqlite();
    }

    public SqliteDbInstance()
    {
        var cacheKey = Guid.NewGuid();

        // Database is shared when cache key is shared, until last connection dies
        AdminConnectionString = ConnectionString = $"Data Source=file::memory:?cache={cacheKey};";

        // Keep one connection open, but don't use it
        _conn = new SqliteConnection(AdminConnectionString);
        _conn.Open();
    }

    protected override Task CreateUserDatabase(DbConnection sql)
    {
        return Task.CompletedTask;
    }
}
