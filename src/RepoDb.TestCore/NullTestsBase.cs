using System.ComponentModel.DataAnnotations.Schema;
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


    public enum NullTest
    {
        Value1 = 1,
        Value2 = 2,
        Value3 = 3
    }

    [Table(nameof(CommonNullTestData))]
    public record EnumNullTestData
    {
        public int ID { get; set; }
        public NullTest Txt { get; set; }
        public NullTest? TxtNull { get; set; }

        public NullTest Nr { get; set; }
        public NullTest? NrNull { get; set; }
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


        sql.Insert(new EnumNullTestData(), trace: new DiagnosticsTracer(), transaction: t);

        await t.RollbackAsync();
    }

    public record GuidNullData
    {
        public int ID { get; set; }
        public Guid Txt { get; set; }
        public Guid? TxtNull { get; set; }

        public Guid Uuid { get; set; }
        public Guid? UuidNull { get; set; }
    }

    public virtual string UuidDbType => "[uniqueidentifier]";

    [TestMethod]
    public async Task GuidNullTest()
    {
        // Regression test. Failed on sqlite and sqlserver before this commit
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables().Any(x => string.Equals(x, "GuidNullData", StringComparison.OrdinalIgnoreCase)))
        {
            var sqlText = $@"CREATE TABLE [GuidNullData] (
                        [ID] int NOT NULL,
                        [Txt] TEXT NOT NULL,
                        [TxtNull] varchar(128) NULL,
                        [Uuid] {UuidDbType} NOT NULL,
                        [UuidNull] {UuidDbType} NULL
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
            new GuidNullData[]
            {
                new GuidNullData(){ ID = 1, Txt = Guid.NewGuid(), TxtNull = Guid.NewGuid(), Uuid = Guid.NewGuid(), UuidNull=Guid.NewGuid()},
                new GuidNullData(){ ID = 2, Txt = Guid.NewGuid(), Uuid = Guid.NewGuid()},
            }, transaction: t);

        await t.RollbackAsync();
    }

    [Table("CommonDateTimeNullTestData")]
    class DateTestData
    {
        public int ID { get; set; }
        public DateTime? Txt { get; set; }
        public DateTime? Date { get; set; }
    }

    [Table("CommonDateTimeNullTestData")]
    class DateOffsetTestData
    {
        public int ID { get; set; }
        public DateTimeOffset? Txt { get; set; }
        public DateTimeOffset? Date { get; set; }
    }

    public virtual string DateTimeDbType => "datetime";

    [TestMethod]
    public async Task DateTimeToDateTimeOffsetTests()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (sql.GetType().Name is { } name && (name.Contains("Postgre") || name.Contains("Npgsql")))
            return;

        if (!GetAllTables().Any(x => string.Equals(x, "CommonDateTimeNullTestData", StringComparison.OrdinalIgnoreCase)))
        {
            var sqlText = $@"CREATE TABLE [CommonDateTimeNullTestData] (
                        [ID] int NOT NULL,
                        [Txt] varchar(128) NULL,
                        [Date] {DateTimeDbType} NULL
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
            new[]
            {
                new DateTestData(){ ID = 1, Txt = new DateTime(2001, 1,1,1,1,1, DateTimeKind.Utc), Date = new DateTime(2002, 1,2,2,2,2, DateTimeKind.Utc)},
                new DateTestData(){ ID = 2, Txt =null, Date = null }
            }, transaction: t);
        await sql.InsertAllAsync(
            new[]
            {
                new DateOffsetTestData(){ ID = 3, Txt = new DateTimeOffset(2003, 1,3,3,3,3, TimeSpan.Zero), Date = new DateTimeOffset(2004, 1,4,4,4,4, TimeSpan.Zero)},
                new DateOffsetTestData(){ ID = 4, Txt =null, Date = null }
            }, transaction: t);

        var all = sql.QueryAll<DateTestData>(transaction: t);
        Assert.AreEqual(4, all.Count());

        var allOffset = sql.QueryAll<DateOffsetTestData>(transaction: t);
        Assert.AreEqual(4, all.Count());

        foreach (var v in all)
        {
            if (v.Txt is { } d2)
            {
                Assert.AreEqual(v.ID, d2.Day);
            }
            if (v.Date is { } d)
            {
                Assert.AreEqual(v.ID + 1, d.Day);
            }
        }

        await t.RollbackAsync();
    }


}
