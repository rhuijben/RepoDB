﻿using System.Data.SQLite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.SQLite.System.IntegrationTests.Setup;

namespace RepoDb.SQLite.System.IntegrationTests.Operations.SDS;

[TestClass]
public class ExecuteScalarTest
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
    public void TestSqLiteConnectionExecuteScalar()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            var result = connection.ExecuteScalar("SELECT COUNT(*) FROM [SdsCompleteTable];");

            // Assert
            Assert.AreEqual(tables.Count(), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionExecuteScalarWithReturnType()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            var result = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM [SdsCompleteTable];");

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqLiteConnectionExecuteScalarAsync()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            var result = await connection.ExecuteScalarAsync("SELECT COUNT(*) FROM [SdsCompleteTable];");

            // Assert
            Assert.AreEqual(tables.Count(), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionExecuteScalarAsyncWithReturnType()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateSdsCompleteTables(10, connection);

            // Act
            var result = await connection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [SdsCompleteTable];");

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    #endregion
}
