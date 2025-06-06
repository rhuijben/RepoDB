﻿using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Extensions;
using RepoDb.Reflection;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Models;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Setup;
using System.Data.Common;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests.Operations.MDS;

[TestClass]
public class ExecuteReaderTest
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

    #region Sync

    [TestMethod]
    public void TestSqLiteConnectionExecuteReader()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            using (var reader = connection.ExecuteReader("SELECT Id, ColumnInt, ColumnDateTime FROM [MdsCompleteTable];"))
            {
                while (reader.Read())
                {
                    // Act
                    var id = reader.GetInt64(0);
                    var columnInt = reader.GetInt32(1);
                    var columnDateTime = reader.GetDateTime(2).ToString(Helper.DATE_FORMAT);
                    var table = tables.FirstOrDefault(e => e.Id == id);

                    // Assert
                    Assert.IsNotNull(table);
                    Assert.AreEqual(columnInt, table.ColumnInt);
                    Assert.AreEqual(columnDateTime, table.ColumnDateTime);
                }
            }
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionExecuteReaderWithMultipleStatements()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            using (var reader = connection.ExecuteReader("SELECT Id, ColumnInt, ColumnDateTime FROM [MdsCompleteTable]; SELECT Id, ColumnInt, ColumnDateTime FROM [MdsCompleteTable];"))
            {
                do
                {
                    while (reader.Read())
                    {
                        // Act
                        var id = reader.GetInt64(0);
                        var columnInt = reader.GetInt32(1);
                        var columnDateTime = reader.GetDateTime(2).ToString(Helper.DATE_FORMAT);
                        var table = tables.FirstOrDefault(e => e.Id == id);

                        // Assert
                        Assert.IsNotNull(table);
                        Assert.AreEqual(columnInt, table.ColumnInt);
                        Assert.AreEqual(columnDateTime, table.ColumnDateTime);
                    }
                } while (reader.NextResult());
            }
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionExecuteReaderAsExtractedEntity()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            using (var reader = connection.ExecuteReader("SELECT * FROM [MdsCompleteTable];"))
            {
                // Act
                var result = DataReader.ToEnumerable<MdsCompleteTable>((DbDataReader)reader).AsList();

                // Assert
                tables.AsList().ForEach(table => Helper.AssertPropertiesEquality(table, result.First(e => e.Id == table.Id)));
            }
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionExecuteReaderAsExtractedDynamic()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            using (var reader = connection.ExecuteReader("SELECT *, 'MDS' AS MDS  FROM [MdsCompleteTable];"))
            {
                // Act
                var result = DataReader.ToEnumerable((DbDataReader)reader).AsList();

                // Assert
                tables.AsList().ForEach(table => Helper.AssertMembersEquality(table, result.First(e => e.Id == table.Id)));
            }
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqLiteConnectionExecuteReaderAsync()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            using (var reader = await connection.ExecuteReaderAsync("SELECT Id, ColumnInt, ColumnDateTime FROM [MdsCompleteTable];"))
            {
                while (reader.Read())
                {
                    // Act
                    var id = reader.GetInt64(0);
                    var columnInt = reader.GetInt32(1);
                    var columnDateTime = reader.GetDateTime(2).ToString(Helper.DATE_FORMAT);
                    var table = tables.FirstOrDefault(e => e.Id == id);

                    // Assert
                    Assert.IsNotNull(table);
                    Assert.AreEqual(columnInt, table.ColumnInt);
                    Assert.AreEqual(columnDateTime, table.ColumnDateTime);
                }
            }
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionExecuteReaderAsyncWithMultipleStatements()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            using (var reader = await connection.ExecuteReaderAsync("SELECT Id, ColumnInt, ColumnDateTime FROM [MdsCompleteTable]; SELECT Id, ColumnInt, ColumnDateTime FROM [MdsCompleteTable];"))
            {
                do
                {
                    while (reader.Read())
                    {
                        // Act
                        var id = reader.GetInt64(0);
                        var columnInt = reader.GetInt32(1);
                        var columnDateTime = reader.GetDateTime(2).ToString(Helper.DATE_FORMAT);
                        var table = tables.FirstOrDefault(e => e.Id == id);

                        // Assert
                        Assert.IsNotNull(table);
                        Assert.AreEqual(columnInt, table.ColumnInt);
                        Assert.AreEqual(columnDateTime, table.ColumnDateTime);
                    }
                } while (reader.NextResult());
            }
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionExecuteReaderAsyncAsExtractedEntity()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            using (var reader = await connection.ExecuteReaderAsync("SELECT * FROM [MdsCompleteTable];"))
            {
                // Act
                var result = DataReader.ToEnumerable<MdsCompleteTable>((DbDataReader)reader).AsList();

                // Assert
                tables.AsList().ForEach(table => Helper.AssertPropertiesEquality(table, result.First(e => e.Id == table.Id)));
            }
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionExecuteReaderAsyncAsExtractedDynamic()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            using (var reader = await connection.ExecuteReaderAsync("SELECT *, 'MDS' AS MDS FROM [MdsCompleteTable];"))
            {
                // Act
                var result = DataReader.ToEnumerable((DbDataReader)reader).AsList();

                // Assert
                tables.AsList().ForEach(table => Helper.AssertMembersEquality(table, result.First(e => e.Id == table.Id)));
            }
        }
    }

    #endregion
}
