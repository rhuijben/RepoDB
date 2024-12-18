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

    public virtual IEnumerable<string> GetAllTables()
    {
        using var sql = CreateConnection();
        sql.EnsureOpen();

        DataTable tables;
        try
        {
            tables = sql.GetSchema("Tables");
        }
        catch (ArgumentException)
        {
            yield break;
        }

        var col = tables.Columns.Cast<DataColumn>().FirstOrDefault(x => x.ColumnName.EndsWith("Name", StringComparison.OrdinalIgnoreCase))?.Ordinal;

        if (!col.HasValue)
            yield break;

        using var rdr = tables.CreateDataReader();

        while (rdr.Read())
        {
            yield return rdr.GetString(col.Value);
        }
    }
}
