﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Data.SqlClient;
using RepoDb.Extensions;
using RepoDb.SqlServer.IntegrationTests.Models;
using RepoDb.SqlServer.IntegrationTests.Setup;

namespace RepoDb.SqlServer.IntegrationTests.Operations;

[TestClass]
public class BatchQueryTest
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
    public void TestSqlServerConnectionBatchQueryFirstBatchAscending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery<CompleteTable>(0,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(0), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(2), result.ElementAt(2));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryFirstBatchDescending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery<CompleteTable>(0,
                3,
                OrderField.Descending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(9), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(7), result.ElementAt(2));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryThirdBatchAscending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery<CompleteTable>(2,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(6), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(8), result.ElementAt(2));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryThirdBatchDescending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery<CompleteTable>(2,
                3,
                OrderField.Descending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(3), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(1), result.ElementAt(2));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryFirstBatchAscendingWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery<CompleteTable>(page: 0,
                rowsPerBatch: 3,
                orderBy: OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                where: (object?)null,
                hints: SqlServerTableHints.NoLock);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(0), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(2), result.ElementAt(2));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryAsyncFirstBatchAscending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync<CompleteTable>(0,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(0), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(2), result.ElementAt(2));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryAsyncFirstBatchDescending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync<CompleteTable>(0,
                3,
                OrderField.Descending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(9), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(7), result.ElementAt(2));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryAsyncThirdBatchAscending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync<CompleteTable>(2,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(6), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(8), result.ElementAt(2));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryAsyncThirdBatchDescending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync<CompleteTable>(2,
                3,
                OrderField.Descending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(3), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(1), result.ElementAt(2));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryAsyncFirstBatchAscendingWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync<CompleteTable>(page: 0,
                rowsPerBatch: 3,
                orderBy: OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                where: (object?)null,
                hints: SqlServerTableHints.NoLock);

            // Assert
            Helper.AssertPropertiesEquality(tables.ElementAt(0), result.ElementAt(0));
            Helper.AssertPropertiesEquality(tables.ElementAt(2), result.ElementAt(2));
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryViaTableNameFirstBatchAscending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery(ClassMappedNameCache.Get<CompleteTable>(),
                0,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(0), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(2), result.ElementAt(2));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryViaTableNameFirstBatchDescending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery(ClassMappedNameCache.Get<CompleteTable>(),
                0,
                3,
                OrderField.Descending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(9), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(7), result.ElementAt(2));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryViaTableNameThirdBatchAscending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery(ClassMappedNameCache.Get<CompleteTable>(),
                2,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(6), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(8), result.ElementAt(2));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryViaTableNameThirdBatchDescending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery(ClassMappedNameCache.Get<CompleteTable>(),
                2,
                3,
                OrderField.Descending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(3), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(1), result.ElementAt(2));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionBatchQueryViaTableNameFirstBatchAscendingWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.BatchQuery(ClassMappedNameCache.Get<CompleteTable>(),
                0,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                where: (object?)null,
                hints: SqlServerTableHints.NoLock);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(0), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(2), result.ElementAt(2));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryViaTableNameAsyncFirstBatchAscending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync(ClassMappedNameCache.Get<CompleteTable>(),
                0,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(0), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(2), result.ElementAt(2));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryViaTableNameAsyncFirstBatchDescending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync(ClassMappedNameCache.Get<CompleteTable>(),
                0,
                3,
                OrderField.Descending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(9), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(7), result.ElementAt(2));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryViaTableNameAsyncThirdBatchAscending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync(ClassMappedNameCache.Get<CompleteTable>(),
                2,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(6), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(8), result.ElementAt(2));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryViaTableNameAsyncThirdBatchDescending()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync(ClassMappedNameCache.Get<CompleteTable>(),
                2,
                3,
                OrderField.Descending<CompleteTable>(c => c.Id).AsEnumerable(),
                (object?)null);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(3), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(1), result.ElementAt(2));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionBatchQueryViaTableNameAsyncFirstBatchAscendingWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.BatchQueryAsync(ClassMappedNameCache.Get<CompleteTable>(),
                0,
                3,
                OrderField.Ascending<CompleteTable>(c => c.Id).AsEnumerable(),
                where: (object?)null,
                hints: SqlServerTableHints.NoLock);

            // Assert
            Helper.AssertMembersEquality(tables.ElementAt(0), result.ElementAt(0));
            Helper.AssertMembersEquality(tables.ElementAt(2), result.ElementAt(2));
        }
    }

    #endregion

    #endregion
}
