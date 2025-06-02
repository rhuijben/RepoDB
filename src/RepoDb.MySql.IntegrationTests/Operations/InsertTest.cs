﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;
using RepoDb.MySql.IntegrationTests.Models;
using RepoDb.MySql.IntegrationTests.Setup;

namespace RepoDb.MySql.IntegrationTests.Operations;

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
    public void TestMySqlConnectionInsertForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public void TestMySqlConnectionInsertForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public async Task TestMySqlConnectionInsertAsyncForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public async Task TestMySqlConnectionInsertAsyncForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.InsertAsync<NonIdentityCompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<NonIdentityCompleteTable>(table.Id);

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
    public void TestMySqlConnectionInsertViaTableNameForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public void TestMySqlConnectionInsertViaTableNameAsDynamicForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTablesAsDynamics(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public void TestMySqlConnectionInsertViaTableNameAsExpandoObjectForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTablesAsExpandoObjects(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            var result = connection.Insert(ClassMappedNameCache.Get<CompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);
            Assert.IsTrue(((dynamic)table).Id == Convert.ToInt64(result));

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public void TestMySqlConnectionInsertViaTableNameForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public void TestMySqlConnectionInsertViaTableNameAsDynamicForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTablesAsDynamics(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public void TestMySqlConnectionInsertViaTableNameAsExpandoObjectForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTablesAsExpandoObjects(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public async Task TestMySqlConnectionInsertViaTableNameAsyncForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public async Task TestMySqlConnectionInsertAsyncViaTableNameAsDynamicForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTablesAsDynamics(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public async Task TestMySqlConnectionInsertAsyncViaTableNameAsExpandoObjectForIdentity()
    {
        // Setup
        var table = Helper.CreateCompleteTablesAsExpandoObjects(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
        {
            // Act
            var result = await connection.InsertAsync(ClassMappedNameCache.Get<CompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<CompleteTable>());
            Assert.IsTrue(Convert.ToInt64(result) > 0);
            Assert.IsTrue(((dynamic)table).Id == Convert.ToInt64(result));

            // Act
            var queryResult = connection.Query<CompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public async Task TestMySqlConnectionInsertViaTableNameAsyncForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTables(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public async Task TestMySqlConnectionInsertAsyncViaTableNameAsDynamicForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTablesAsDynamics(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
    public async Task TestMySqlConnectionInsertAsyncViaTableNameAsExpandoObjectForNonIdentity()
    {
        // Setup
        var table = Helper.CreateNonIdentityCompleteTablesAsExpandoObjects(1).First();

        using (var connection = new MySqlConnection(Database.ConnectionString))
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
