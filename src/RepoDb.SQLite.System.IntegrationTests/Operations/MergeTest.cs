﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.SQLite.System.IntegrationTests.Models;
using RepoDb.SQLite.System.IntegrationTests.Setup;
using System.Data.SQLite;

namespace RepoDb.SQLite.System.IntegrationTests.Operations.SDS;

[TestClass]
public class MergeTest
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
    public void TestSQLiteConnectionMergeForIdentityForEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Merge<SdsCompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeForIdentityForNonEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();

            // Setup
            Helper.UpdateSdsCompleteTableProperties(table);

            // Act
            var result = connection.Merge<SdsCompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeForIdentityForNonEmptyTableWithQualifiers()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            var qualifiers = new[]
            {
                new Field("Id", typeof(long))
            };
            Helper.UpdateSdsCompleteTableProperties(table);
            table.ColumnInt = 0;
            table.ColumnChar = "C";

            // Act
            var result = connection.Merge<SdsCompleteTable>(table,
                qualifiers: qualifiers);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncForIdentityForEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();

            // Act
            var result = await connection.MergeAsync<SdsCompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncForIdentityForNonEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();

            // Setup
            Helper.UpdateSdsCompleteTableProperties(table);

            // Act
            var result = await connection.MergeAsync<SdsCompleteTable>(table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncForIdentityForNonEmptyTableWithQualifiers()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            var qualifiers = new[]
            {
                new Field("Id", typeof(long))
            };
            Helper.UpdateSdsCompleteTableProperties(table);
            table.ColumnInt = 0;
            table.ColumnChar = "C";

            // Act
            var result = await connection.MergeAsync<SdsCompleteTable>(table,
                qualifiers: qualifiers);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSQLiteConnectionMergeViaTableNameForIdentityForEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Merge(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeViaTableNameForIdentityForNonEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            Helper.UpdateSdsCompleteTableProperties(table);

            // Act
            var result = connection.Merge(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeViaTableNameForIdentityForNonEmptyTableWithQualifiers()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            var qualifiers = new[]
            {
                new Field("Id", typeof(long))
            };
            Helper.UpdateSdsCompleteTableProperties(table);
            table.ColumnInt = 0;
            table.ColumnChar = "C";

            // Act
            var result = connection.Merge(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table,
                qualifiers: qualifiers);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeAsDynamicViaTableNameForIdentityForEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Create the tables
            Database.CreateSdsTables(connection);

            // Setup
            var table = Helper.CreateSdsCompleteTablesAsDynamics(1).First();

            // Act
            var result = connection.Merge(ClassMappedNameCache.Get<SdsCompleteTable>(),
                (object)table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, queryResult?.Count());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeAsDynamicViaTableNameForIdentityForNonEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            Helper.UpdateSdsCompleteTableProperties(table);

            // Act
            var result = connection.Merge(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeAsDynamicViaTableNameForIdentityForNonEmptyTableWithQualifiers()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            Helper.UpdateSdsCompleteTableProperties(table);
            var qualifiers = new[]
            {
                new Field("Id", typeof(long))
            };

            // Act
            var result = connection.Merge(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table,
                qualifiers: qualifiers);

            // Assert
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeViaTableNameAsExpandoObjectForIdentityForEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            Database.CreateSdsCompleteTables(1, connection).First();
            var table = Helper.CreateSdsCompleteTablesAsExpandoObjects(1).First();

            // Act
            var result = connection.Merge(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.IsTrue((long)result > 0);
            Assert.AreEqual(((dynamic)table).Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public void TestSQLiteConnectionMergeViaTableNameAsExpandoObjectForIdentityForNonEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            Database.CreateSdsCompleteTables(1, connection).First();
            var table = Helper.CreateSdsCompleteTablesAsExpandoObjects(1).First();

            // Act
            var result = connection.Merge(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(((dynamic)table).Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncViaTableNameForIdentityForEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();

            // Act
            var result = await connection.MergeAsync(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncViaTableNameForIdentityForNonEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            Helper.UpdateSdsCompleteTableProperties(table);

            // Act
            var result = await connection.MergeAsync(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncViaTableNameForIdentityForNonEmptyTableWithQualifiers()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            var qualifiers = new[]
            {
                new Field("Id", typeof(long))
            };
            Helper.UpdateSdsCompleteTableProperties(table);

            // Act
            var result = await connection.MergeAsync(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table,
                qualifiers: qualifiers);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncAsDynamicViaTableNameForIdentityForEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Create the tables
            Database.CreateSdsTables(connection);

            // Setup
            var table = Helper.CreateSdsCompleteTablesAsDynamics(1).First();

            // Act
            var result = await connection.MergeAsync(ClassMappedNameCache.Get<SdsCompleteTable>(),
                (object)table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncAsDynamicViaTableNameForIdentityForNonEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            Helper.UpdateSdsCompleteTableProperties(table);

            // Act
            var result = await connection.MergeAsync(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncAsDynamicViaTableNameForIdentityForNonEmptyTableWithQualifiers()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateSdsCompleteTables(1, connection).First();
            Helper.UpdateSdsCompleteTableProperties(table);
            var qualifiers = new[]
            {
                new Field("Id", typeof(long))
            };

            // Act
            var result = await connection.MergeAsync(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table,
                qualifiers: qualifiers);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(table.Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertPropertiesEquality(table, queryResult.First());
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncViaTableNameAsExpandoObjectForIdentityForEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            Database.CreateSdsCompleteTables(1, connection).First();
            var table = Helper.CreateSdsCompleteTablesAsExpandoObjects(1).First();

            // Act
            var result = await connection.MergeAsync(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.IsTrue((long)result > 0);
            Assert.AreEqual(((dynamic)table).Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    [TestMethod]
    public async Task TestSQLiteConnectionMergeAsyncViaTableNameAsExpandoObjectForIdentityForNonEmptyTable()
    {
        using (var connection = new SQLiteConnection(Database.ConnectionString))
        {
            // Setup
            Database.CreateSdsCompleteTables(1, connection).First();
            var table = Helper.CreateSdsCompleteTablesAsExpandoObjects(1).First();

            // Act
            var result = await connection.MergeAsync(ClassMappedNameCache.Get<SdsCompleteTable>(),
                table);

            // Assert
            Assert.AreEqual(1, connection.CountAll<SdsCompleteTable>());
            Assert.AreEqual(((dynamic)table).Id, result);

            // Act
            var queryResult = connection.Query<SdsCompleteTable>(result);

            // Assert
            Helper.AssertMembersEquality(queryResult.First(), table);
        }
    }

    #endregion

    #endregion
}
