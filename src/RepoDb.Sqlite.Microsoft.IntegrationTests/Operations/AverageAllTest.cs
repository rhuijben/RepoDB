﻿using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Models;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Setup;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests.Operations.MDS;

[TestClass]
public class AverageAllTest
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
    public void TestSqLiteConnectionAverageAll()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = connection.AverageAll<MdsCompleteTable>(e => e.ColumnInt);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnSqLiteConnectionAverageAllWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            connection.AverageAll<MdsCompleteTable>(e => e.ColumnInt,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqLiteConnectionAverageAllAsync()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = await connection.AverageAllAsync<MdsCompleteTable>(e => e.ColumnInt);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionOnSqLiteConnectionAverageAllAsyncWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            await connection.AverageAllAsync<MdsCompleteTable>(e => e.ColumnInt,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSqLiteConnectionAverageAllViaTableName()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = connection.AverageAll(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First());

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnSqLiteConnectionAverageAllViaTableNameWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            connection.AverageAll(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First(),
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqLiteConnectionAverageAllAsyncViaTableName()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = await connection.AverageAllAsync(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First());

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionOnSqLiteConnectionAverageAllAsyncViaTableNameWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            await connection.AverageAllAsync(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First(),
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion
}
