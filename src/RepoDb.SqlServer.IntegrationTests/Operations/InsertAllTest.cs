﻿using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.SqlServer.IntegrationTests.Models;
using RepoDb.SqlServer.IntegrationTests.Setup;

namespace RepoDb.SqlServer.IntegrationTests.Operations;

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
    public void TestSqlConnectionInsertAllForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public void TestSqlConnectionInsertAllForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public async Task TestSqlConnectionInsertAllAsyncForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public async Task TestSqlConnectionInsertAllAsyncForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public void TestSqlConnectionInsertAllViaTableNameForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public void TestSqlConnectionInsertAllViaTableNameAsDynamicsForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsDynamics(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public void TestSqlConnectionInsertAllViaTableNameAsExpandoObjectForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsExpandoObjects(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public void TestSqlConnectionInsertAllViaTableNameForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public void TestSqlConnectionInsertAllViaTableNameAsDynamicsForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsDynamics(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public void TestSqlConnectionInsertAllViaTableNameAsExpandoObjectForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsExpandoObjects(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqlConnectionInsertAllViaTableNameAsyncForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public async Task TestSqlConnectionInsertAllAsyncViaTableNameAsDynamicsForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsDynamics(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public async Task TestSqlConnectionInsertAllAsyncViaTableNameAsExpandoObjectForIdentity()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsExpandoObjects(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public async Task TestSqlConnectionInsertAllViaTableNameAsyncForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public async Task TestSqlConnectionInsertAllAsyncViaTableNameAsDynamicsForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsDynamics(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
    public async Task TestSqlConnectionInsertAllAsyncViaTableNameAsExpandoObjectForNonIdentity()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsExpandoObjects(10);

        using (var connection = new SqlConnection(Database.ConnectionString))
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
