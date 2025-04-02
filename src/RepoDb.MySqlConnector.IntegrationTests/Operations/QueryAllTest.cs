﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySqlConnector;
using RepoDb.Extensions;
using RepoDb.MySqlConnector.IntegrationTests.Models;
using RepoDb.MySqlConnector.IntegrationTests.Setup;

namespace RepoDb.MySqlConnector.IntegrationTests.Operations;

[TestClass]
public class QueryAllTest
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
    public void TestMySqlConnectionQueryAll()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionQueryAllWithHints()
    {
        // Setup
        var table = Database.CreateCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            connection.QueryAll<CompleteTable>(hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestMySqlConnectionQueryAllAsync()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            var queryResult = await connection.QueryAllAsync<CompleteTable>();

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionQueryAllAsyncWithHints()
    {
        // Setup
        var table = Database.CreateCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            await connection.QueryAllAsync<CompleteTable>(hints: "WhatEver");
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestMySqlConnectionQueryAllViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            var queryResult = connection.QueryAll(ClassMappedNameCache.Get<CompleteTable>());

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionQueryAllViaTableNameWithHints()
    {
        // Setup
        var table = Database.CreateCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            connection.Query(ClassMappedNameCache.Get<CompleteTable>(),
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestMySqlConnectionQueryAllAsyncViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            var queryResult = await connection.QueryAllAsync(ClassMappedNameCache.Get<CompleteTable>());

            // Assert
            tables.AsList().ForEach(table =>
                Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionQueryAllAsyncViaTableNameWithHints()
    {
        // Setup
        var table = Database.CreateCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            await connection.QueryAsync(ClassMappedNameCache.Get<CompleteTable>(),
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion
}
