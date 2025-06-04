﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

[TestClass]
public class InsertTest
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
    public void TestPostgreSqlConnectionInsertForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTables(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Insert<CompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);
            Assert.IsTrue(table.Id > 0);

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTables(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Insert<NonIdentityCompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAsyncForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTables(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAsync<CompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);
            Assert.IsTrue(table.Id > 0);

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAsyncForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTables(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAsync<NonIdentityCompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionInsertViaTableNameForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTables(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Insert(ClassMappedNameCache.Get<CompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertViaTableNameAsDynamicForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTablesAsDynamics(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Insert(ClassMappedNameCache.Get<CompleteTable>(),
                (object)table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertViaTableNameAsExpandoObjectForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTablesAsExpandoObjects(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Insert(ClassMappedNameCache.Get<CompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);
            Assert.AreEqual(((dynamic)table).Id, result);

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertViaTableNameForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTables(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Insert(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertViaTableNameAsDynamicForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTablesAsDynamics(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Insert(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                (object)table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionInsertViaTableNameAsExpandoObjectForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTablesAsExpandoObjects(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.Insert(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertViaTableNameAsyncForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTables(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAsync(ClassMappedNameCache.Get<CompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAsyncViaTableNameAsDynamicForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTablesAsDynamics(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAsync(ClassMappedNameCache.Get<CompleteTable>(),
                (object)table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAsyncViaTableNameAsExpandoObjectForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTablesAsExpandoObjects(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAsync(ClassMappedNameCache.Get<CompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);
            Assert.AreEqual(((dynamic)table).Id, result);

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertViaTableNameAsyncForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTables(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAsyncViaTableNameAsDynamicForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTablesAsDynamics(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                (object)table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionInsertAsyncViaTableNameAsExpandoObjectForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTablesAsExpandoObjects(1).First();

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.InsertAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    #endregion

    #endregion
}
