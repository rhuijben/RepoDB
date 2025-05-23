using System.Data.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Enumerations;

namespace RepoDb.TestCore;

public abstract class DbTestBase<TDbInstance> where TDbInstance : DbInstance, new()
{
    public TestContext TestContext { get; set; }

    public TDbInstance DbInstance = new();

    [ClassInitialize(InheritanceBehavior.BeforeEachDerivedClass)]
    public static async Task TestClassInitialize(TestContext context)
    {
        await using var q = new TDbInstance();

        await q.ClassInitializeAsync(context);
    }

    [TestInitialize]
    public void Initialize()
    {
        InitializeCore();
    }

    protected abstract void InitializeCore();

    public abstract DbConnection CreateConnection();

    public async Task<DbConnection> CreateOpenConnectionAsync()
    {
        var db = CreateConnection();
        try
        {
            await db.OpenAsync();
            return db;
        }
        catch
        {
#if NET
            await db.DisposeAsync();
#else
            db.Dispose();
#endif
            throw;
        }
    }

    public async ValueTask<bool> TableExistsAsync(DbConnection sql, string tableName)
    {
        await sql.EnsureOpenAsync();

        var tables = await sql.GetDbHelper().GetSchemaObjectsAsync(sql);

        return tables.Any(x => x.Type == DbSchemaType.Table && x.Name == tableName);
    }
}
