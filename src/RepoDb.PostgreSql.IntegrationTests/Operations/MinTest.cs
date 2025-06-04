﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.Enumerations;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

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
    public void TestPostgreSqlConnectionMinWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInteger,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var ids = new[] { tables.First().Id, tables.Last().Id };

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInteger,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInteger,
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInteger,
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaQueryFields()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInteger,
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaQueryGroup()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };
        var queryGroup = new QueryGroup(queryFields);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min<CompleteTable>(e => e.ColumnInteger,
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnPostgreSqlConnectionMinWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            connection.Min<CompleteTable>(e => e.ColumnInteger,
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInteger,
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var ids = new[] { tables.First().Id, tables.Last().Id };

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInteger,
                e => ids.Contains(e.Id));

            // Assert
            Assert.AreEqual(tables.Where(e => ids.Contains(e.Id)).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInteger,
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInteger,
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaQueryFields()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInteger,
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaQueryGroup()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };
        var queryGroup = new QueryGroup(queryFields);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync<CompleteTable>(e => e.ColumnInteger,
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionOnPostgreSqlConnectionMinAsyncWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            await connection.MinAsync<CompleteTable>(e => e.ColumnInteger,
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaTableNameWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaTableNameViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaTableNameViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaTableNameViaQueryFields()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMinViaTableNameViaQueryGroup()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };
        var queryGroup = new QueryGroup(queryFields);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnPostgreSqlConnectionMinViaTableNameWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            connection.Min(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaTableNameWithoutExpression()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                (object?)null);

            // Assert
            Assert.AreEqual(tables.Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaTableNameViaDynamic()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                new { tables.First().Id });

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaTableNameViaQueryField()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                new QueryField("Id", tables.First().Id));

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id == tables.First().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaTableNameViaQueryFields()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                queryFields);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMinAsyncViaTableNameViaQueryGroup()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);
        var queryFields = new[]
        {
            new QueryField("Id", Operation.GreaterThan, tables.First().Id),
            new QueryField("Id", Operation.LessThan, tables.Last().Id)
        };
        var queryGroup = new QueryGroup(queryFields);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                queryGroup);

            // Assert
            Assert.AreEqual(tables.Where(e => e.Id > tables.First().Id && e.Id < tables.Last().Id).Min(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionOnPostgreSqlConnectionMinAsyncViaTableNameWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            await connection.MinAsync(ClassMappedNameCache.Get<CompleteTable>(),
                new Field("ColumnInteger", typeof(int)),
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion
}
