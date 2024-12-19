using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Enumerations;
using RepoDb.Trace;

namespace RepoDb.TestCore;

public abstract partial class NullTestsBase<TDbInstance> : DbTestBase<TDbInstance> where TDbInstance : DbInstance, new()
{
    public record CommonNullTestData
    {
        public int ID { get; set; }
        public string Txt { get; set; }
        public string? TxtNull { get; set; }

        public int Nr { get; set; }
        public int? NrNull { get; set; }
    }


    [TestMethod]
    public async Task NullQueryTest()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables().Any(x => string.Equals(x, "CommonNullTestData", StringComparison.OrdinalIgnoreCase)))
        {
            var sqlText = @"CREATE TABLE [CommonNullTestData] (
                        [ID] int NOT NULL,
                        [Txt] varchar(128) NOT NULL,
                        [TxtNull] varchar(128) NULL,
                        [Nr] int NOT NULL,
                        [NrNull] int NULL
                )";

            var set = sql.GetDbSetting();

            if (set.OpeningQuote != "[")
                sqlText = sqlText.Replace("[", set.OpeningQuote);
            if (set.ClosingQuote != "]")
                sqlText = sqlText.Replace("]", set.ClosingQuote);

            await sql.ExecuteNonQueryAsync(sqlText);
        }

        var t = await sql.BeginTransactionAsync();

        await sql.InsertAllAsync(
            new CommonNullTestData[]
            {
                new CommonNullTestData(){ ID = 1, Txt = "t1", TxtNull = "t2", Nr = 10, NrNull = 11},
                new CommonNullTestData(){ ID = 2, Txt = "t5", TxtNull = null, Nr = 15, NrNull = null}
            }, transaction: t);

        var all = sql.QueryAll<CommonNullTestData>(transaction: t);
        Assert.AreEqual(2, all.Count());


        Assert.AreEqual(1, await sql.CountAsync<CommonNullTestData>(where: x => x.Txt != "t1", transaction: t, trace: new DiagnosticsTracer()));
        Assert.AreEqual(1, await sql.CountAsync<CommonNullTestData>(where: x => x.Nr != 10, transaction: t));

        Assert.AreEqual(0, await sql.CountAsync<CommonNullTestData>(where: x => x.TxtNull != "t2", transaction: t, trace: new DiagnosticsTracer()));
        Assert.AreEqual(0, await sql.CountAsync<CommonNullTestData>(where: x => x.NrNull != 11, transaction: t));

        GlobalConfiguration.Setup(GlobalConfiguration.Options with { ExpressionNullSemantics = ExpressionNullSemantics.NullNotEqual });

        Assert.AreEqual(1, await sql.CountAsync<CommonNullTestData>(where: x => x.TxtNull != "t2", transaction: t, trace: new DiagnosticsTracer()));
        Assert.AreEqual(1, await sql.CountAsync<CommonNullTestData>(where: x => x.NrNull != 11, transaction: t, trace: new DiagnosticsTracer()));

        var storeQueries = new StoreQueryTracer();

        Assert.AreEqual(1, await sql.CountAsync<CommonNullTestData>(where: x => x.Txt != "t1", transaction: t, trace: storeQueries));
        Assert.AreEqual(1, await sql.CountAsync<CommonNullTestData>(where: x => x.Nr != 10, transaction: t, trace: storeQueries));


        Assert.IsFalse(storeQueries.Traces.Any(x => x.Contains("NULL")), string.Join(Environment.NewLine, storeQueries.Traces));


        Assert.AreEqual(1, await sql.CountAsync<CommonNullTestData>(where: x => x.ID == 1 && !(x.Txt == "t5" && x.Nr == 22), transaction: t, trace: new DiagnosticsTracer()));

        await t.RollbackAsync();
    }
}
