﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Data.SqlClient;
using RepoDb.SqlServer.IntegrationTests.Models;
using RepoDb.SqlServer.IntegrationTests.Setup;

namespace RepoDb.SqlServer.IntegrationTests.Operations;

[TestClass]
public class SumAllTest
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
    public void TestSqlServerConnectionSumAll()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.SumAll<CompleteTable>(e => e.ColumnInt);

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionSumAllWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.SumAll<CompleteTable>(e => e.ColumnInt,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionSumAllAsync()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.SumAllAsync<CompleteTable>(e => e.ColumnInt);

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionSumAllAsyncWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.SumAllAsync<CompleteTable>(e => e.ColumnInt,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSqlServerConnectionSumAllViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.SumAll(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First());

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionSumAllViaTableNameWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.SumAll(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionSumAllAsyncViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.SumAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First());

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionSumAllAsyncViaTableNameWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.SumAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #endregion
}
