﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Data.SqlClient;
using RepoDb.Enumerations;
using RepoDb.SqlServer.IntegrationTests.Models;
using RepoDb.SqlServer.IntegrationTests.Setup;

namespace RepoDb.SqlServer.IntegrationTests.Operations;

[TestClass]
public class AverageTest
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
    public void TestSqlServerConnectionAverageWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Average<CompleteTable>(e => e.ColumnInt,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionAverageWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Average<CompleteTable>(e => e.ColumnInt,
                (object?)null,
                SqlServerTableHints.TabLock);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionAverageWithExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = connection.Average<CompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionAverageWithExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = connection.Average<CompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id),
                SqlServerTableHints.TabLock);

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionAverageAsyncWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.AverageAsync<CompleteTable>(e => e.ColumnInt,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionAverageAsyncWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.AverageAsync<CompleteTable>(e => e.ColumnInt,
                (object?)null,
                SqlServerTableHints.TabLock);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionAverageAsyncWithExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = await connection.AverageAsync<CompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionAverageAsyncWithExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = await connection.AverageAsync<CompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id),
                SqlServerTableHints.TabLock);

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSqlServerConnectionAverageViaTableNameWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Average(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionAverageViaTableNameWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Average(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                (object?)null,
                SqlServerTableHints.TabLock);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionAverageViaTableNameWithExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = connection.Average(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                new QueryField("Id", Operation.In, ids));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionAverageViaTableNameWithExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = connection.Average(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                new QueryField("Id", Operation.In, ids),
                SqlServerTableHints.TabLock);

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionAverageAsyncViaTableNameWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.AverageAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionAverageAsyncViaTableNameWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.AverageAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                (object?)null,
                SqlServerTableHints.TabLock);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionAverageAsyncViaTableNameWithExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = await connection.AverageAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                new QueryField("Id", Operation.In, ids));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionAverageAsyncViaTableNameWithExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = await connection.AverageAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInt).First(),
                new QueryField("Id", Operation.In, ids),
                SqlServerTableHints.TabLock);

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), Convert.ToDouble(result));
        }
    }

    #endregion

    #endregion
}
