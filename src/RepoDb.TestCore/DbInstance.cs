using System.Data.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace RepoDb.TestCore;

public abstract class DbInstance : IAsyncDisposable
{
    bool _initialized;
    internal DbInstance()
    {

    }

    public ValueTask DisposeAsync()
    {
#if NET
        return ValueTask.CompletedTask;
#else
        return new();
#endif
    }

    public async Task ClassInitializeAsync(TestContext? context)
    {
        if (!_initialized)
        {
#if NET
            await using var sql = CreateAdminConnection();
#else
            using var sql = CreateAdminConnection();
#endif
            await sql.EnsureOpenAsync();

            await CreateUserDatabase(sql);

            _initialized = true;
        }
    }

    protected abstract Task CreateUserDatabase(DbConnection sql);

    public abstract DbConnection CreateConnection();

    public abstract DbConnection CreateAdminConnection();

    public DbConnection CreateOpenConnection()
    {
        var c = CreateConnection();
        try
        {
            c.EnsureOpen();
            return c;
        }
        catch
        {
            c.Dispose();
            throw;
        }
    }

    public async ValueTask<DbConnection> CreateOpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var c = CreateConnection();
        try
        {
            await c.EnsureOpenAsync(cancellationToken);
            return c;
        }
        catch
        {
            c.Dispose();
            throw;
        }
    }

    public string AdminConnectionString { get; protected set; }
    public string ConnectionString { get; protected set; }
}

public abstract class DbInstance<TDbConnection> : DbInstance where TDbConnection : DbConnection, new()
{
    public override DbConnection CreateConnection()
    {
        var c = new TDbConnection();
        c.ConnectionString = ConnectionString;
        return c;
    }

    public override DbConnection CreateAdminConnection()
    {
        var c = new TDbConnection();
        c.ConnectionString = AdminConnectionString;
        return c;
    }
}
