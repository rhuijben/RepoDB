﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Data.SqlClient;
using RepoDb.Enumerations;
using RepoDb.SqlServer.IntegrationTests.Models;
using RepoDb.SqlServer.IntegrationTests.Setup;

namespace RepoDb.SqlServer.IntegrationTests.Operations;

[TestClass]
public class MinTest
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
    public void TestSqlServerConnectionMinWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInt,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var ids = new[] { tables.First().Id, tables.Last().Id };

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInt,
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInt,
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaQueryFields()
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
            var result = connection.Min<CompleteTable>(e => e.ColumnInt,
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaQueryGroup()
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
            var result = connection.Min<CompleteTable>(e => e.ColumnInt,
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInt,
                (object?)null,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInt,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var ids = new[] { tables.First().Id, tables.Last().Id };

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInt,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInt,
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInt,
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaQueryFields()
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
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInt,
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaQueryGroup()
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
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInt,
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInt,
                (object?)null,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSqlServerConnectionMinViaTableNameWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaTableNameViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaTableNameViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaTableNameViaQueryFields()
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
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaTableNameViaQueryGroup()
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
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestSqlServerConnectionMinViaTableNameWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                (object?)null,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaTableNameWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaTableNameViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaTableNameViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaTableNameViaQueryFields()
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
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaTableNameViaQueryGroup()
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
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestSqlServerConnectionMinAsyncViaTableNameWithoutExpressionWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInt", typeof(int)),
                (object?)null,
                SqlServerTableHints.NoLock);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInt), Convert.ToInt32(result));
        }
    }

    #endregion

    #endregion
}
