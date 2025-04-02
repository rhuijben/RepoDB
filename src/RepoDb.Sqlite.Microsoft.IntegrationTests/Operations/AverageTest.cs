﻿using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Enumerations;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Models;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Setup;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests.Operations.MDS;

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
    public void TestSqLiteConnectionAverageWithoutExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = connection.Average<MdsCompleteTable>(e => e.ColumnInt,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionAverageWithExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = connection.Average<MdsCompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void TestSqLiteConnectionAverageWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            connection.Average<MdsCompleteTable>(e => e.ColumnInt,
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqLiteConnectionAverageAsyncWithoutExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = await connection.AverageAsync<MdsCompleteTable>(e => e.ColumnInt,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionAverageAsyncWithExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = await connection.AverageAsync<MdsCompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task TestSqLiteConnectionAverageAsyncWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            await connection.AverageAsync<MdsCompleteTable>(e => e.ColumnInt,
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSqLiteConnectionAverageViaTableNameWithoutExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = connection.Average(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First(),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionAverageViaTableNameWithExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = connection.Average(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First(),
                new QueryField("Id", Operation.In, ids));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void TestSqLiteConnectionAverageViaTableNameWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            connection.Average(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First(),
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqLiteConnectionAverageAsyncViaTableNameWithoutExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = await connection.AverageAsync(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First(),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionAverageAsyncViaTableNameWithExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var ids = new[] { tables.First().Id, tables.Last().Id };
            var result = await connection.AverageAsync(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First(),
                new QueryField("Id", Operation.In, ids));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Average(e => e.ColumnInt), result);
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task TestSqLiteConnectionAverageAsyncViaTableNameWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            await connection.AverageAsync(ClassMappedNameCache.Get<MdsCompleteTable>(),
                Field.Parse<MdsCompleteTable>(e => e.ColumnInt).First(),
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion
}
