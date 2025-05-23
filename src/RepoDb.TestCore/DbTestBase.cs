using System.Data;
using System.Data.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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

    protected virtual string SchemaDatabaseColumnName => "Catalog";

    public IEnumerable<string> GetAllTables(DbConnection sql)
    {
        sql.EnsureOpen();

        return sql.GetDbHelper().GetSchemaObjects(sql).Where(t => t.Type == Enumerations.DbSchemaType.Table).Select(x => x.Name).ToArray();
    }
}
