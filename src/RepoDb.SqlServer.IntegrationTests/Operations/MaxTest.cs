﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Data.SqlClient;
using RepoDb.Enumerations;
using RepoDb.SqlServer.IntegrationTests.Models;
using RepoDb.SqlServer.IntegrationTests.Setup;

namespace RepoDb.SqlServer.IntegrationTests.Operations;

[TestClass]
public class MaxTest
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
    public void TestSqlServerConnectionMaxWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max<CompleteTable>(e => e.ColumnInt,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var ids = new[] { tables.First().Id, tables.Last().Id };

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max<CompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max<CompleteTable>(e => e.ColumnInt,
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max<CompleteTable>(e => e.ColumnInt,
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaQueryFields()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max<CompleteTable>(e => e.ColumnInt,
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaQueryGroup()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };
        var queryGroup = new QueryGroup(queryFields);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max<CompleteTable>(e => e.ColumnInt,
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max<CompleteTable>(e => e.ColumnInt,
                (object?)null,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync<CompleteTable>(e => e.ColumnInt,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var ids = new[] { tables.First().Id, tables.Last().Id };

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync<CompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync<CompleteTable>(e => e.ColumnInt,
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync<CompleteTable>(e => e.ColumnInt,
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaQueryFields()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync<CompleteTable>(e => e.ColumnInt,
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaQueryGroup()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };
        var queryGroup = new QueryGroup(queryFields);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync<CompleteTable>(e => e.ColumnInt,
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync<CompleteTable>(e => e.ColumnInt,
                (object?)null,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSqlServerConnectionMaxViaTableNameWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaTableNameViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaTableNameViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaTableNameViaQueryFields()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaTableNameViaQueryGroup()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };
        var queryGroup = new QueryGroup(queryFields);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMaxViaTableNameWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Max(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                (object?)null,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaTableNameWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaTableNameViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaTableNameViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaTableNameViaQueryFields()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaTableNameViaQueryGroup()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };
        var queryGroup = new QueryGroup(queryFields);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMaxAsyncViaTableNameWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MaxAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                (object?)null,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #endregion
}
