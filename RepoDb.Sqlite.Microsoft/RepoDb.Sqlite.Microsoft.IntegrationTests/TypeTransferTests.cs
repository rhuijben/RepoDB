using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Attributes;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Setup;
using RepoDb.Trace;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests
{
    [TestClass]
    public class TypeTransferTests
    {

        [TestInitialize]
        public void Initialize()
        {
            Database.Initialize();
            Cleanup();
            using var connection = new SqliteConnection(Database.ConnectionStringMDS);
            {
                AddTables(connection);
            }
        }

        private static void AddTables(SqliteConnection connection)
        {
            connection.ExecuteNonQuery(
                    @"CREATE TABLE IF NOT EXISTS DateTypesData (
                        ID INTEGER PRIMARY KEY NOT NULL,
                        Bool                INT NOT NULL,
                        BoolNull            INT NULL,
                        Date                DATE NOT NULL,
                        DateNull            DATE NULL,
                        DateTime            DATE NOT NULL,
                        DateTimeNull        DATE NULL,
                        DateTimeOffset      DATE NOT NULL,
                        DateTimeOffsetNull  DATE NULL,
                        Decimal             Decimal NOT NULL,
                        DecimalNull         Decimal NULL
                    )");


            connection.ExecuteNonQuery(
                    @"CREATE TABLE IF NOT EXISTS EnumTypesTests (
                        ID INTEGER PRIMARY KEY NOT NULL,
                        EnumInt             INT NOT NULL,
                        EnumIntNull         INT NULL,
                        EnumString          TEXT NOT NULL,
                        EnumStringNull      TEXT NULL,
                        EnumVarChar         VarChar NOT NULL,
                        EnumVarCharNull     VarChar NULL
                    )");
        }

        public record DateTypesData
        {
            [Primary, Identity]
            public int ID { get; set; }

            public bool Bool { get; set; }
            public bool? BoolNull { get; set; }

            public DateOnly Date { get; set; }
            public DateOnly? DateNull { get; set; }
            public DateTime DateTime { get; set; }
            public DateTime? DateTimeNull { get; set; }

            public DateTimeOffset DateTimeOffset { get; set; }
            public DateTimeOffset? DateTimeOffsetNull { get; set; }

            public decimal Decimal { get; set; }
            public decimal? DecimalNull { get; set; }
        }

        public enum Wind
        {
            North,
            East,
            South,
            West
        }

        public enum Current
        {
            AC,
            DC,
            SomeC
        }

        public enum VAT
        {
            High,
            Low,
            Exempt
        }

        public record EnumTypesTests
        {
            [Primary, Identity]
            public int ID { get; set; }
            public Wind EnumInt { get; set; }
            public Wind? EnumIntNull { get; set; }
            public Current EnumString { get; set; }
            public Current? EnumStringNull { get; set; }

            public VAT EnumVarChar { get; set; }
            public VAT? EnumVarCharNull { get; set; }
        }

        [TestCleanup]
        public void Cleanup()
        {
            Database.Cleanup();
        }

        [TestMethod]
        public void BoolDateDecimalInsertRead()
        {
            using var connection = new SqliteConnection(Database.ConnectionStringMDS);
            {
                AddTables(connection);

                var src = new DateTypesData
                {
                    Bool = true,
                    BoolNull = false,
                    Date = DateOnly.FromDateTime(new DateTime(2000, 1, 1)),
                    DateNull = DateOnly.FromDateTime(new DateTime(2001, 2, 2)),
                    DateTime = new DateTime(2004, 3, 3),
                    DateTimeNull = new DateTime(2004, 4, 4),
                    DateTimeOffset = new DateTimeOffset(2010, 5, 5, 0, 0, 0, TimeSpan.Zero),
                    DateTimeOffsetNull = new DateTimeOffset(2011, 6, 6, 0, 0, 0, TimeSpan.Zero),
                    Decimal = 2345m,
                    DecimalNull = 6789m
                };

                connection.Insert(src);


                var item = connection.QueryAll<DateTypesData>().Single();

                Assert.AreEqual(src, item);

                src = src with
                {
                    BoolNull = null,
                    DateNull = null,
                    DateTimeNull = null,
                    DateTimeOffsetNull = null,
                    DecimalNull = null,
                };

                connection.Update(src, trace: new DiagnosticsTracer());

                var item2 = connection.QueryAll<DateTypesData>().Single();

                Assert.AreEqual(src, item2);
            }
        }

        [TestMethod]
        public void EnumInsertRead()
        {
            using var connection = new SqliteConnection(Database.ConnectionStringMDS);
            {
                AddTables(connection);

                var src = new EnumTypesTests
                {
                    EnumInt = Wind.South,
                    EnumIntNull = Wind.East,
                    EnumString = Current.AC,
                    EnumStringNull = Current.DC,
                    EnumVarChar = VAT.High,
                    EnumVarCharNull = VAT.Exempt
                };

                connection.Insert(src, trace: new DiagnosticsTracer());


                var item = connection.QueryAll<EnumTypesTests>().Single();

                Assert.AreEqual(src, item);

                src = src with
                {
                    EnumIntNull = null,
                    EnumStringNull = null,
                    EnumVarCharNull = null
                };

                connection.Update(src);

                var item2 = connection.QueryAll<EnumTypesTests>().Single();

                Assert.AreEqual(src, item2);
            }
        }
    }
}
