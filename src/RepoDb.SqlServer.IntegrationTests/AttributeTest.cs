﻿using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Attributes.Parameter.SqlServer;
using RepoDb.Extensions;
using RepoDb.SqlServer.IntegrationTests.Setup;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Text;

namespace RepoDb.SqlServer.IntegrationTests;

[TestClass]
public class AttributeTest
{
    [TestInitialize]
    public void Initialize()
    {
        Database.Initialize();
        Cleanup();
    }

    [TestCleanup]
    public void Cleanup()
    {
        Database.Cleanup();
    }

    #region Classes

    [Table("CompleteTable")]
    public class MdsAttributeTable
    {
        public int Id { get; set; }

        [SqlDbType(SqlDbType.UniqueIdentifier)]
        public Guid SessionId { get; set; }

        [SqlDbType(SqlDbType.Binary)]
        public byte[] ColumnBinary { get; set; }

        [SqlDbType(SqlDbType.BigInt)]
        public long ColumnBigInt { get; set; }

        [SqlDbType(SqlDbType.DateTime2)]
        public DateTime ColumnDateTime2 { get; set; }

        [SqlDbType(SqlDbType.Text)]
        public string ColumnNVarChar { get; set; }
    }

    #endregion

    #region Helpers

    private IEnumerable<MdsAttributeTable> CreateMdsAttributeTables(int count = 10)
    {
        var random = new Random();
        for (var i = 0; i < count; i++)
        {
            yield return new MdsAttributeTable
            {
                Id = i,
                ColumnBigInt = Convert.ToInt64(random.Next(int.MaxValue)),
                ColumnBinary = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
                ColumnDateTime2 = DateTime.UtcNow.AddDays(-random.Next(100)),
                ColumnNVarChar = $"ColumnNVarChar-{i}-{Guid.NewGuid()}",
                SessionId = Guid.NewGuid()
            };
        }
    }

    #endregion

    #region MDS

    [TestMethod]
    public void TestSqlConnectionForInsertForMicrosoftSqlServerTypeMapAttribute()
    {
        // Setup
        var table = CreateMdsAttributeTables(1).First();

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            connection.Insert<MdsAttributeTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<MdsAttributeTable>());

            // Query
            var queryResult = connection.QueryAll<MdsAttributeTable>().First();

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult);
        }
    }

    [TestMethod]
    public void TestSqlConnectionForInsertAllForMicrosoftSqlServerTypeMapAttribute()
    {
        // Setup
        var tables = CreateMdsAttributeTables(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            connection.InsertAll<MdsAttributeTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<MdsAttributeTable>());

            // Query
            var queryResult = connection.QueryAll<MdsAttributeTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestSqlConnectionForQueryForMicrosoftSqlServerTypeMapAttribute()
    {
        // Setup
        var table = CreateMdsAttributeTables(1).First();

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var id = connection.Insert<MdsAttributeTable>(table);

            // Query
            var queryResult = connection.Query<MdsAttributeTable>(id).First();

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult);
        }
    }

    [TestMethod]
    public void TestSqlConnectionForQueryAllForMicrosoftSqlServerTypeMapAttribute()
    {
        // Setup
        var tables = CreateMdsAttributeTables(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            connection.InsertAll<MdsAttributeTable>(tables);

            // Query
            var queryResult = connection.QueryAll<MdsAttributeTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
        }
    }

    #endregion
}
