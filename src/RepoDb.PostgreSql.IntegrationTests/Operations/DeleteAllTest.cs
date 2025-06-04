﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

[TestClass]
public class DeleteAllTest
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
    public void TestPostgreSqlConnectionDeleteAll()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.DeleteAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionDeleteAllViaPrimaryKeys()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var primaryKeys = ClassExpression.GetEntitiesPropertyValues<CompleteTable, object>(tables, e => e.Id);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.DeleteAll<CompleteTable>(primaryKeys);

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionDeleteAllViaPrimaryKeysBeyondLimits()
    {
        // Setup
        var tables = Database.CreateCompleteTables(5000);
        var primaryKeys = ClassExpression.GetEntitiesPropertyValues<CompleteTable, object>(tables, e => e.Id);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.DeleteAll<CompleteTable>(primaryKeys);

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionDeleteAllAsync()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.DeleteAllAsync<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionDeleteAllAsyncViaPrimaryKeys()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var primaryKeys = ClassExpression.GetEntitiesPropertyValues<CompleteTable, object>(tables, e => e.Id);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.DeleteAllAsync<CompleteTable>(primaryKeys);

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionDeleteAllAsyncViaPrimaryKeysBeyondLimits()
    {
        // Setup
        var tables = Database.CreateCompleteTables(5000);
        var primaryKeys = ClassExpression.GetEntitiesPropertyValues<CompleteTable, object>(tables, e => e.Id);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.DeleteAllAsync<CompleteTable>(primaryKeys);

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionDeleteAllViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.DeleteAll(ClassMappedNameCache.Get<CompleteTable>());

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionDeleteAllViaTableNameViaPrimaryKeys()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var primaryKeys = ClassExpression.GetEntitiesPropertyValues<CompleteTable, object>(tables, e => e.Id);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.DeleteAll(ClassMappedNameCache.Get<CompleteTable>(), primaryKeys);

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionDeleteAllViaTableNameViaPrimaryKeysBeyondLimits()
    {
        // Setup
        var tables = Database.CreateCompleteTables(5000);
        var primaryKeys = ClassExpression.GetEntitiesPropertyValues<CompleteTable, object>(tables, e => e.Id);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.DeleteAll(ClassMappedNameCache.Get<CompleteTable>(), primaryKeys);

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionDeleteAllAsyncViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.DeleteAllAsync(ClassMappedNameCache.Get<CompleteTable>());

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionDeleteAllAsyncViaTableNameViaPrimaryKeys()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var primaryKeys = ClassExpression.GetEntitiesPropertyValues<CompleteTable, object>(tables, e => e.Id);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.DeleteAllAsync(ClassMappedNameCache.Get<CompleteTable>(), primaryKeys);

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionDeleteAllAsyncViaTableNameViaPrimaryKeysBeyondLimits()
    {
        // Setup
        var tables = Database.CreateCompleteTables(5000);
        var primaryKeys = ClassExpression.GetEntitiesPropertyValues<CompleteTable, object>(tables, e => e.Id);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.DeleteAllAsync(ClassMappedNameCache.Get<CompleteTable>(), primaryKeys);

            // Assert
            Assert.AreEqual(tables.Count(), result);
        }
    }

    #endregion

    #endregion
}
