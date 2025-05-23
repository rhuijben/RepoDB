﻿using System.ComponentModel.DataAnnotations.Schema;
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

        if (!GetAllTables(sql).Any(x => string.Equals(x, "CommonNullTestData", StringComparison.OrdinalIgnoreCase)))
        {
            var sqlText = @$"CREATE TABLE [CommonNullTestData] (
                        [ID] int NOT NULL,
                        [Txt] {VarCharName}(128) NOT NULL,
                        [TxtNull] {VarCharName}(128) NULL,
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

        public byte[] BlobData { get; set; }
        public byte[]? BlobDataNull { get; set; }
    }

    public virtual string UuidDbType => "[uniqueidentifier]";
    public virtual string BlobDbType => "varbinary(128)";
    public virtual string TextDbType => "TEXT";

    [TestMethod]
    public async Task GuidNullTest()
    {
        // Regression test. Failed on sqlite and sqlserver before this commit
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables(sql).Any(x => string.Equals(x, "GuidNullData", StringComparison.OrdinalIgnoreCase)))
        {
            await PerformCreateTableAsync(sql, $@"CREATE TABLE [GuidNullData] (
                        [ID] int NOT NULL,
                        [Txt] {TextDbType} NOT NULL,
                        [TxtNull] {VarCharName}(128) NULL,
                        [Uuid] {UuidDbType} NOT NULL,
                        [UuidNull] {UuidDbType} NULL,
                        [BlobData] {BlobDbType} NOT NULL,
                        [BlobDataNull] {BlobDbType} NULL
                )");
        }

        var t = await sql.BeginTransactionAsync();

        await sql.InsertAllAsync(
            new GuidNullData[]
            {
                new GuidNullData(){ ID = 1, Txt = Guid.NewGuid(), TxtNull = Guid.NewGuid(), Uuid = Guid.NewGuid(), UuidNull=Guid.NewGuid(), BlobData = new byte[]{32}, BlobDataNull = new byte[]{65 } },
                new GuidNullData(){ ID = 2, Txt = Guid.NewGuid(), Uuid = Guid.NewGuid(), BlobData = new byte[]{ 97 } },
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

        if (!GetAllTables(sql).Any(x => string.Equals(x, "CommonDateTimeNullTestData", StringComparison.OrdinalIgnoreCase)))
        {
            await PerformCreateTableAsync(sql, $@"CREATE TABLE [CommonDateTimeNullTestData] (
                        [ID] int NOT NULL,
                        [Txt] {VarCharName}(128) NULL,
                        [Date] {DateTimeDbType} NULL
                )");
        }

        var t = await sql.BeginTransactionAsync();

        await sql.InsertAllAsync(
            new[]
            {
                new DateTestData(){ ID = 1, Txt = new DateTime(2001, 1,1,1,1,1, DateTimeKind.Utc), Date = new DateTime(2002, 1,2,2,2,2, DateTimeKind.Utc)},
                new DateTestData(){ ID = 2, Txt =null, Date = null }
            },
            trace: new DiagnosticsTracer(),
            transaction: t);
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

        var l = sql.Query<DateOffsetTestData>(where: x => x.Date < DateTimeOffset.Now, transaction: t);

        await t.RollbackAsync();
    }

    private static async Task<string> PerformCreateTableAsync(System.Data.Common.DbConnection sql, string sqlText)
    {
        sqlText = ApplySqlRules(sql, sqlText);

        try
        {
            await sql.ExecuteNonQueryAsync(sqlText);
        }
        catch (Exception e)
        {
            throw new Exception($"While performing: {sqlText}", e);
        }
        return sqlText;
    }

    protected static string ApplySqlRules(System.Data.Common.DbConnection sql, string sqlText)
    {
        var set = sql.GetDbSetting();

        if (set.OpeningQuote != "[")
            sqlText = sqlText.Replace("[", set.OpeningQuote);
        if (set.ClosingQuote != "]")
            sqlText = sqlText.Replace("]", set.ClosingQuote);
        if (set.ParameterPrefix != "@")
            sqlText = sqlText.Replace("@", set.ParameterPrefix);
        return sqlText;
    }

    public virtual string DateTimeOffsetDbType => "datetimeoffset";

#if NET
    public virtual string TimeOnlyDbType => "time";
    public virtual string DateOnlyDbType => "date";

    record DateTimeOnlyTable
    {
        public TimeOnly TOnly { get; set; }
        public DateOnly DOnly { get; set; }

        public DateTimeOffset DOffset { get; set; }
    }

    [TestMethod]
    public async Task DateTimeOnlyTests()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables(sql).Any(x => string.Equals(x, "DateTimeOnlyTable", StringComparison.OrdinalIgnoreCase)))
        {
            await PerformCreateTableAsync(sql, $@"CREATE TABLE [DateTimeOnlyTable] (
                        [TOnly] {TimeOnlyDbType} NOT NULL,
                        [DOnly] {DateOnlyDbType} NOT NULL,
                        [DOffset] {DateTimeOffsetDbType} NOT NULL
            )");
        }

        await sql.TruncateAsync<DateTimeOnlyTable>();

        await sql.InsertAsync(new DateTimeOnlyTable() { DOnly = new DateOnly(2021, 1, 1), TOnly = new TimeOnly(1, 2, 3), DOffset = new DateTimeOffset(2023, 1, 1, 1, 1, 1, TimeSpan.Zero) });

        Assert.IsNotEmpty(await sql.QueryAllAsync<DateTimeOnlyTable>());

        if (sql.GetType().Name is { } name && (name.Contains("Postgre") || name.Contains("Npgsql")))
            return; // Case sensitive issue

        Assert.IsNotEmpty(await sql.ExecuteQueryAsync<DateTimeOnlyTable>("SELECT * FROM DateTimeOnlyTable"));

        var today = DateOnly.FromDateTime(DateTime.Now);
        await sql.QueryAsync<DateTimeOnlyTable>(where: x => x.DOnly < today);


        await sql.QueryAsync<DateTimeOnlyTable>(where: x => x.DOffset < DateTime.Now);
        await sql.QueryAsync<DateTimeOnlyTable>(where: x => x.DOffset < DateTimeOffset.Now);
    }
#endif

    record WithComputed
    {
        public int ID { get; set; }
        public string Writable { get; set; }
        public string Computed { get; set; }
    }

    public virtual string GeneratedColumnDefinition(string expression, string type) => $"GENERATED ALWAYS AS ({expression})";

    [TestMethod]
    public async Task ComputedColumnTest()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables(sql).Any(x => string.Equals(x, "WithComputed", StringComparison.OrdinalIgnoreCase)))
        {
            await PerformCreateTableAsync(sql, $@"CREATE TABLE [WithComputed] (
                        [ID] int NOT NULL,
                        [Writable] {VarCharName}(128) NOT NULL,
                        [Computed] {GeneratedColumnDefinition("CONCAT('-', CONCAT([Writable], '-'))", $"{VarCharName}(130)")}
            )");
        }
        else
        {
            await sql.TruncateAsync<WithComputed>(trace: new DiagnosticsTracer());
        }

        var fields = await DbFieldCache.GetAsync(sql, nameof(WithComputed), transaction: null);
        Assert.AreEqual(true, fields.First(x => x.Name == "Computed").IsGenerated);

        await sql.InsertAsync(new WithComputed() { ID = 1, Writable = "a" });

        var r = (await sql.QueryAllAsync<WithComputed>()).FirstOrDefault();

        Assert.AreEqual(1, r.ID);
        Assert.AreEqual("a", r.Writable);
        Assert.AreEqual("-a-", r.Computed);

        await sql.QueryAllAsync<WithComputed>(orderBy: [OrderField.Parse<WithComputed>(x => x.Computed, Order.Ascending)]);

        await sql.UpdateAsync<WithComputed>(
            new()
            {
                Writable = "b"
            },
            where: x => x.Computed == "-a-",
            trace: new DiagnosticsTracer());
    }

    [TestMethod]
    public void TestReadTuple()
    {
        var sql = CreateConnection().EnsureOpen();

        var t = sql.ExecuteQuery<Tuple<int, string>>("SELECT 1, 'a'").FirstOrDefault();
        Assert.AreEqual(1, t.Item1);
        Assert.AreEqual("a", t.Item2);
    }

    [TestMethod]
    public void TestReadValueTuple()
    {
        var sql = CreateConnection().EnsureOpen();

        var t = sql.ExecuteQuery<(int v1, string c2)>("SELECT 1, 'a'").FirstOrDefault();

        Assert.AreEqual(1, t.Item1);
        Assert.AreEqual("a", t.Item2);
    }

    record WithGroupByItems
    {
        public int ID { get; set; }
        public string Txt { get; set; }
    }

    [TestMethod]
    public async Task VarPrefix()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables(sql).Any(x => string.Equals(x, "WithGroupByItems", StringComparison.OrdinalIgnoreCase)))
        {
            await PerformCreateTableAsync(sql, $@"CREATE TABLE [WithGroupByItems] (
                        [ID] int NOT NULL,
                        [Txt] {VarCharName}(128) NOT NULL,
                        [Nr] int NOT NULL
            )");
        }

        // This used to trigger an escaping issue between @a and @aa (which starts with '@a')
        var s = await sql.ExecuteQueryAsync<WithGroupByItems>(
            ApplySqlRules(sql, "SELECT [Txt] from [WithGroupByItems] WHERE [Txt] IN (@a) GROUP BY [Txt] HAVING COUNT(1) = @aa"),
            new { a = new string[] { "a" }, aa = 1 }
            );
    }

    class id2record
    {

        public int ID { get; set; }
        public int ID2 { get; set; }
    }
    class id2recordNullable
    {
        public int ID { get; set; }
        public int? ID2 { get; set; }
    }

    const string IntNotNullable = nameof(IntNotNullable);
    [TestMethod]
    public async Task NullableIntError()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables(sql).Any(x => string.Equals(x, IntNotNullable, StringComparison.OrdinalIgnoreCase)))
        {
            await PerformCreateTableAsync(sql, $@"CREATE TABLE [{IntNotNullable}] (
                        [ID] int NOT NULL,
                        [ID2] int NOT NULL
            )");
        }

        await sql.InsertAsync(tableName: IntNotNullable, new id2record { ID = 1, ID2 = 2 });

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await sql.InsertAsync(tableName: IntNotNullable, new id2recordNullable { ID = 3, ID2 = null }),
            "Required Nullable<Int32> property ID2 evaluated to null.");
    }


    class FieldLengthTable
    {
        public string ID { get; set; }
        public string ID2 { get; set; }
        public string? VAL3 { get; set; }
    }

    public virtual string AltVarChar => "varchar";
    public virtual string VarCharName => "varchar";

    [TestMethod]
    public async Task FieldLengthTest()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (!GetAllTables(sql).Any(x => string.Equals(x, nameof(FieldLengthTable), StringComparison.OrdinalIgnoreCase)))
        {
            await PerformCreateTableAsync(sql, $@"CREATE TABLE [{nameof(FieldLengthTable)}] (
                    [ID] {VarCharName}(36) NOT NULL,
                    [ID2] {AltVarChar}(37) NOT NULL,
                    [VAL3] {AltVarChar}(38) NULL,
                    CONSTRAINT [PK_{nameof(FieldLengthTable)}] PRIMARY KEY
                    (
                        [ID], [ID2]
                    )
            )");
        }
        else
        {
            await sql.TruncateAsync<FieldLengthTable>();
        }

        var fd = await sql.GetDbHelper().GetFieldsAsync(sql, nameof(FieldLengthTable));

        var id1 = fd.First(x => x.Name == "ID");
        var id2 = fd.First(x => x.Name == "ID2");
        var val3 = fd.First(x => x.Name == "VAL3");
        Assert.AreEqual("ID", id1?.Name);
        Assert.AreEqual("ID2", id2?.Name);
        Assert.AreEqual("VAL3", val3?.Name);
        Assert.AreEqual(typeof(string), id1?.Type);
        Assert.AreEqual(typeof(string), id2?.Type);
        Assert.AreEqual(typeof(string), val3?.Type);

        Assert.AreEqual(VarCharName, id1?.DatabaseType);
        Assert.AreEqual(AltVarChar == "varchar" ? VarCharName : AltVarChar, id2?.DatabaseType);
        Assert.AreEqual(AltVarChar == "varchar" ? VarCharName : AltVarChar, val3?.DatabaseType);

        Assert.AreEqual(36, id1?.Size);
        Assert.AreEqual(37, id2?.Size);
        Assert.AreEqual(38, val3?.Size);

        Assert.AreEqual(true, id1?.IsPrimary);
        Assert.AreEqual(true, id2?.IsPrimary);
        Assert.AreEqual(false, val3?.IsPrimary);

        Assert.AreEqual(false, id1?.IsIdentity);
        Assert.AreEqual(false, id2?.IsIdentity);
        Assert.AreEqual(false, val3?.IsIdentity);

        Assert.AreEqual(false, id1?.IsNullable);
        Assert.AreEqual(false, id2?.IsNullable);
        Assert.AreEqual(true, val3?.IsNullable);

        var ftf = new FieldLengthTable[]
        {
            new FieldLengthTable { ID = "a12345678901234567890123456789012345", ID2 = "b12345678901234567890123456789012345", VAL3 = "c" },
            new FieldLengthTable { ID = "d12345678901234567890123456789012345", ID2 = "e12345678901234567890123456789012345", VAL3 = null }
        };

        Assert.AreEqual(2, await sql.InsertAllAsync(ftf, trace: new DiagnosticsTracer()));

        var data = (await sql.QueryAllAsync<FieldLengthTable>()).ToArray();

        Assert.AreEqual(2, data.Count());
        Assert.AreEqual(ftf[0].ID, data[0].ID);
    }

    class MorePrimaryKeyTable
    {
        public string ID { get; set; }
        public int ID2 { get; set; }
        public string? Value { get; set; }
    }

    protected virtual string IdentityDefinition => "INT GENERATED ALWAYS AS IDENTITY";



    [TestMethod]
    public async Task MultiKeyReturnIdentity()
    {
        using var sql = await CreateOpenConnectionAsync();

        if (sql.GetType().Name.Contains("iteConnection"))
            return;

        if (!GetAllTables(sql).Any(x => string.Equals(x, nameof(MorePrimaryKeyTable), StringComparison.OrdinalIgnoreCase)))
        {
            await PerformCreateTableAsync(sql, $@"CREATE TABLE [{nameof(MorePrimaryKeyTable)}] (
                    [ID] {VarCharName}(20) NOT NULL,
                    [ID2] {IdentityDefinition},
                    [Value] {AltVarChar}(38) NULL,
                    CONSTRAINT [PK_{nameof(MorePrimaryKeyTable)}] PRIMARY KEY
                    (
                        [ID2], [ID]
                    )
            )");
        }
        else
        {
            await sql.TruncateAsync<MorePrimaryKeyTable>();
        }

        var fd = await sql.GetDbHelper().GetFieldsAsync(sql, nameof(MorePrimaryKeyTable));

        var id1 = fd.First(x => x.Name == "ID");
        var id2 = fd.First(x => x.Name == "ID2");
        var val3 = fd.First(x => x.Name == "Value");
        Assert.AreEqual("ID", id1?.Name);
        Assert.AreEqual("ID2", id2?.Name);
        Assert.AreEqual("Value", val3?.Name);

        Assert.AreEqual(VarCharName, id1?.DatabaseType);
        //Assert.AreEqual("INT", id2?.DatabaseType); // Or 'int' or 'integer', or ...
        Assert.AreEqual(AltVarChar == "varchar" ? VarCharName : AltVarChar, val3?.DatabaseType);

        Assert.AreEqual(typeof(string), id1?.Type);
        Assert.IsTrue(id2?.Type == typeof(int) || id2?.Type == typeof(decimal));
        Assert.AreEqual(typeof(string), val3?.Type);

        Assert.AreEqual(true, id1?.IsPrimary);
        Assert.AreEqual(true, id2?.IsPrimary);
        Assert.AreEqual(false, val3?.IsPrimary);

        Assert.AreEqual(false, id1?.IsIdentity);
        Assert.AreEqual(true, id2?.IsIdentity);
        Assert.AreEqual(false, val3?.IsIdentity);

        Assert.AreEqual(false, id1?.IsNullable);
        Assert.AreEqual(false, id2?.IsNullable);
        Assert.AreEqual(true, val3?.IsNullable);

        var ftf = new MorePrimaryKeyTable[]
        {
            new MorePrimaryKeyTable { ID = "A", ID2 = 0, Value = "c" },
            new MorePrimaryKeyTable { ID = "B", ID2 = 0, Value = null }
        };

        Assert.AreEqual(2, await sql.InsertAllAsync(ftf, trace: new DiagnosticsTracer()));

        var data = (await sql.QueryAllAsync<MorePrimaryKeyTable>()).ToArray();

        Assert.AreEqual(2, data.Count());
        Assert.AreEqual(ftf[0].ID, data[0].ID);
        Assert.AreEqual(ftf[1].ID, data[1].ID);
        Assert.AreEqual(ftf[0].ID2, data[0].ID2);
        Assert.AreEqual(ftf[1].ID2, data[1].ID2);
        Assert.AreNotEqual(ftf[0].ID2, ftf[1].ID2);
        Assert.AreEqual(ftf[0].Value, data[0].Value);
        Assert.AreEqual(ftf[1].Value, data[1].Value);
    }
}
