﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.Extensions;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

[TestClass]
public class UpdateAllTest
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

    #region DataEntity

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionUpdateAll()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.AsList().ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = connection.UpdateAll<CompleteTable>(tables);

            // Assert
            Assert.AreEqual(10, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionUpdateAllAsync()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.AsList().ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = await connection.UpdateAllAsync<CompleteTable>(tables);

            // Assert
            Assert.AreEqual(10, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionUpdateAllViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.AsList().ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = connection.UpdateAll(ClassMappedNameCache.Get<CompleteTable>(), tables);

            // Assert
            Assert.AreEqual(10, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionUpdateAllViaTableNameAsExpandoObjects()
    {
        // Setup
        var entities = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            var tables = Helper.CreateCompleteTablesAsExpandoObjects(10).AsList();
            tables.ForEach(e => ((IDictionary<string, object>)e)["Id"] = entities[tables.IndexOf(e)].Id);

            // Act
            var result = connection.UpdateAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(10, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionUpdateAllAsyncViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.AsList().ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = await connection.UpdateAllAsync(ClassMappedNameCache.Get<CompleteTable>(), tables);

            // Assert
            Assert.AreEqual(10, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionUpdateAllAsyncViaTableNameAsExpandoObjects()
    {
        // Setup
        var entities = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            var tables = Helper.CreateCompleteTablesAsExpandoObjects(10).AsList();
            tables.ForEach(e => ((IDictionary<string, object>)e)["Id"] = entities[tables.IndexOf(e)].Id);

            // Act
            var result = await connection.UpdateAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(10, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    #endregion

    #endregion
}
