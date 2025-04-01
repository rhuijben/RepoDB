using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Setup;
using RepoDb.Trace;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests.Common;

[TestClass]
public class NullTests : RepoDb.TestCore.NullTestsBase<SqliteDbInstance>
{
    protected override void InitializeCore() => Database.Initialize(TestContext);

    public override DbConnection CreateConnection() => new SqliteConnection(Database.GetConnectionString(TestContext));

#if NET
    public override string TimeOnlyDbType => "TEXT";
#endif


    [TestMethod]
    public async Task MultiPrimaryKeyDelete()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables(sql).Any(x => string.Equals(x, "MultiKeyDelete", StringComparison.OrdinalIgnoreCase)))
        {
            var sqlText = @"CREATE TABLE [MultiKeyDelete] (
                        [ID] int NOT NULL,
                        [Txt] varchar(128) NOT NULL

                        ,CONSTRAINT [PK] PRIMARY KEY ([ID], [Txt])
                )";

            var set = sql.GetDbSetting();

            if (set.OpeningQuote != "[")
                sqlText = sqlText.Replace("[", set.OpeningQuote);
            if (set.ClosingQuote != "]")
                sqlText = sqlText.Replace("]", set.ClosingQuote);

            await sql.ExecuteNonQueryAsync(sqlText);
        }

        Assert.AreEqual(2, sql.GetDbHelper().GetFields(sql, "MultiKeyDelete").Count(x => x.IsPrimary));

        await sql.DeleteAsync(new MultiKeyDelete { ID = 1, Txt = "2" }, trace: new DiagnosticsTracer());
    }

    class MultiKeyDelete
    {
        public int ID { get; set; }
        public string Txt { get; set; }
    }
}

