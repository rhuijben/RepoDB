using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

[TestClass]
public class MaxAllTest
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
    public void TestPostgreSqlConnectionMaxAll()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MaxAll<CompleteTable>(e => e.ColumnInteger);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnPostgreSqlConnectionMaxAllWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            connection.MaxAll<CompleteTable>(e => e.ColumnInteger,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionMaxAllAsync()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MaxAllAsync<CompleteTable>(e => e.ColumnInteger);

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionOnPostgreSqlConnectionMaxAllAsyncWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            await connection.MaxAllAsync<CompleteTable>(e => e.ColumnInteger,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionMaxAllViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MaxAll(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInteger).First());

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnPostgreSqlConnectionMaxAllViaTableNameWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            connection.MaxAll(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInteger).First(),
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionMaxAllAsyncViaTableName()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MaxAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInteger).First());

            // Assert
            Assert.AreEqual(tables.Max(e => e.ColumnInteger), Convert.ToInt32(result));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionOnPostgreSqlConnectionMaxAllAsyncViaTableNameWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            await connection.MaxAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                Field.Parse<CompleteTable>(e => e.ColumnInteger).First(),
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion
}
