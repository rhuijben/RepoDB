﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Extensions;
using RepoDb.Reflection;
using RepoDb.SQLite.System.IntegrationTests.Models;
using RepoDb.SQLite.System.IntegrationTests.Setup;
using System.Data.Common;
using System.Data.SQLite;

namespace RepoDb.SQLite.System.IntegrationTests.Operations.SDS;

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
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            using (var reader = connection.ExecuteReader("SELECT Id, ColumnInt, ColumnDateTime FROM [SdsCompleteTable];"))
            {
                while (reader.Read())
                {
                    // Act
                    var id = reader.GetInt64(0);
                    var columnInt = reader.GetInt32(1);
                    var columnDateTime = reader.GetDateTime(2);
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
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            using (var reader = connection.ExecuteReader("SELECT Id, ColumnInt, ColumnDateTime FROM [SdsCompleteTable]; SELECT Id, ColumnInt, ColumnDateTime FROM [SdsCompleteTable];"))
            {
                do
                {
                    while (reader.Read())
                    {
                        // Act
                        var id = reader.GetInt64(0);
                        var columnInt = reader.GetInt32(1);
                        var columnDateTime = reader.GetDateTime(2);
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
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            using (var reader = connection.ExecuteReader("SELECT * FROM [SdsCompleteTable];"))
            {
                // Act
                var result = DataReader.ToEnumerable<SdsCompleteTable>((DbDataReader)reader).AsList();

                // Assert
                tables.AsList().ForEach(table => Helper.AssertPropertiesEquality(table, result.First(e => e.Id == table.Id)));
            }
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionExecuteReaderAsExtractedDynamic()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            using (var reader = connection.ExecuteReader("SELECT * FROM [SdsCompleteTable];"))
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
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            using (var reader = await connection.ExecuteReaderAsync("SELECT Id, ColumnInt, ColumnDateTime FROM [SdsCompleteTable];"))
            {
                while (reader.Read())
                {
                    // Act
                    var id = reader.GetInt64(0);
                    var columnInt = reader.GetInt32(1);
                    var columnDateTime = reader.GetDateTime(2);
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
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            using (var reader = await connection.ExecuteReaderAsync("SELECT Id, ColumnInt, ColumnDateTime FROM [SdsCompleteTable]; SELECT Id, ColumnInt, ColumnDateTime FROM [SdsCompleteTable];"))
            {
                do
                {
                    while (reader.Read())
                    {
                        // Act
                        var id = reader.GetInt64(0);
                        var columnInt = reader.GetInt32(1);
                        var columnDateTime = reader.GetDateTime(2);
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
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            using (var reader = await connection.ExecuteReaderAsync("SELECT * FROM [SdsCompleteTable];"))
            {
                // Act
                var result = DataReader.ToEnumerable<SdsCompleteTable>((DbDataReader)reader).AsList();

                // Assert
                tables.AsList().ForEach(table => Helper.AssertPropertiesEquality(table, result.First(e => e.Id == table.Id)));
            }
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionExecuteReaderAsyncAsExtractedDynamic()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            using (var reader = await connection.ExecuteReaderAsync("SELECT * FROM [SdsCompleteTable];"))
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
