﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

[TestClass]
public class ExecuteNonQueryTest
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
    public void TestPostgreSqlConnectionExecuteNonQuery()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.ExecuteNonQuery("DELETE FROM \"CompleteTable\";");

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionExecuteNonQueryWithParameters()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.ExecuteNonQuery("DELETE FROM \"CompleteTable\" WHERE \"Id\" = @Id;",
                new { tables.Last().Id });

            // Assert
            Assert.AreEqual(1, result);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionExecuteNonQueryWithMultipleStatement()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.ExecuteNonQuery("DELETE FROM \"CompleteTable\"; DELETE FROM \"CompleteTable\";");

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionExecuteNonQueryAsync()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.ExecuteNonQueryAsync("DELETE FROM \"CompleteTable\";");

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionExecuteNonQueryAsyncWithParameters()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.ExecuteNonQueryAsync("DELETE FROM \"CompleteTable\" WHERE \"Id\" = @Id;",
                new { tables.Last().Id });

            // Assert
            Assert.AreEqual(1, result);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionExecuteNonQueryAsyncWithMultipleStatement()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.ExecuteNonQueryAsync("DELETE FROM \"CompleteTable\"; DELETE FROM \"CompleteTable\";");

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    #endregion
}
