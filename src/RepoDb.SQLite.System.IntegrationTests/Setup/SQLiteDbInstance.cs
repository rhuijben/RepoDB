using System.Data.Common;
using System.Data.SQLite;
using RepoDb.TestCore;

namespace RepoDb.SQLite.System.IntegrationTests.Setup;

public class SQLiteDbInstance : DbInstance<SQLiteConnection>
{
    readonly SQLiteConnection _conn;

    static SQLiteDbInstance()
    {
        GlobalConfiguration.Setup(GlobalConfiguration.Options).UseSQLite();
    }

    public SQLiteDbInstance()
    {
        var cacheKey = Guid.NewGuid();

        // Database is shared when cache key is shared, until last connection dies
        AdminConnectionString = ConnectionString = $"Data Source=file::memory:?cache={cacheKey};";

        // Keep one connection open, but don't use it
        _conn = new SQLiteConnection(AdminConnectionString);
        _conn.Open();
    }

    protected override Task CreateUserDatabase(DbConnection sql)
    {
        return Task.CompletedTask;
    }
}
