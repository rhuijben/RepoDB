using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

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
    public void TestPostgreSqlConnectionSumAll()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.SumAll<CompleteTable>(e => e.ColumnInteger);

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnPostgreSqlConnectionSumAllWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            connection.SumAll<CompleteTable>(e => e.ColumnInteger,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionSumAllAsync()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.SumAllAsync<CompleteTable>(e => e.ColumnInteger);

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionOnPostgreSqlConnectionSumAllAsyncWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            await connection.SumAllAsync<CompleteTable>(e => e.ColumnInteger,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionSumAllViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.SumAll(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInteger).First());

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnPostgreSqlConnectionSumAllViaTableNameWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            connection.SumAll(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInteger).First(),
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionSumAllAsyncViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.SumAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInteger).First());

            // Assert
            Assert.AreEqual(tables.Sum(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionOnPostgreSqlConnectionSumAllAsyncViaTableNameWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            await connection.SumAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInteger).First(),
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion
}
