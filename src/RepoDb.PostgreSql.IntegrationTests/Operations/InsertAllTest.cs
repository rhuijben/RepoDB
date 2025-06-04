﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

[TestClass]
public class InsertAllTest
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
    public void TestPostgreSqlConnectionInsertAllForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.InsertAll<CompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);
            Assert.IsTrue(tables.All(table => table.Id > 0));

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertAllForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.InsertAll<NonIdentityCompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAllAsyncForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAllAsync<CompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);
            Assert.IsTrue(tables.All(table => table.Id > 0));

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAllAsyncForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAllAsync<NonIdentityCompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionInsertAllViaTableNameForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.InsertAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertAllViaTableNameAsDynamicsForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsDynamics(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.InsertAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertAllViaTableNameAsExpandoObjectsForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsExpandoObjects(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.InsertAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);
            Assert.IsTrue(tables.All(table => ((dynamic)table).Id > 0));

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertAllViaTableNameForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.InsertAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertAllViaTableNameAsDynamicsForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsDynamics(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.InsertAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertAllViaTableNameAsExpandoObjectsForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsExpandoObjects(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.InsertAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);
            Assert.IsTrue(tables.All(table => ((dynamic)table).Id > 0));

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAllViaTableNameAsyncForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAllAsyncViaTableNameAsDynamicsForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsDynamics(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAllAsyncViaTableNameAsExpandoObjectsForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsExpandoObjects(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);
            Assert.IsTrue(tables.All(table => ((dynamic)table).Id > 0));

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAllViaTableNameAsyncForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAllAsyncViaTableNameAsDynamicsForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsDynamics(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAllAsyncViaTableNameAsExpandoObjectsForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsExpandoObjects(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    #endregion

    #endregion
}
