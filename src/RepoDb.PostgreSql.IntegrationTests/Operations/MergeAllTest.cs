﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.Extensions;
using RepoDb.PostgreSql.IntegrationTests.Setup;
using RepoDb.PostgreSql.IntegrationTests.Models;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

[TestClass]
public class MergeAllTest
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
    public void TestPostgreSqlConnectionMergeAllForIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MergeAll<CompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllForIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll<CompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllForIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll<CompleteTable>(tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllForNonIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MergeAll<NonIdentityCompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllForNonIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll<NonIdentityCompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllForNonIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll<NonIdentityCompleteTable>(tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncForIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MergeAllAsync<CompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncForIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync<CompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncForIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync<CompleteTable>(tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncForNonIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MergeAllAsync<NonIdentityCompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncForNonIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync<NonIdentityCompleteTable>(tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncForNonIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync<NonIdentityCompleteTable>(tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllViaTableNameForIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllAsExpandoObjectViaTableNameForIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsExpandoObjects(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);
            Assert.IsTrue(tables.All(table => ((dynamic)table).Id > 0));

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllViaTableNameForIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllAsExpandoObjectViaTableNameForIdentityForNonEmptyTable()
    {
        // Setup
        var entities = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            var tables = Helper.CreateCompleteTablesAsExpandoObjects(10).AsList();
            tables.ForEach(e => ((IDictionary<string, object>)e)["Id"] = entities[tables.IndexOf(e)].Id);

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(entities.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(entities.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllViaTableNameForIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllAsDynamicsViaTableNameForIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsDynamics(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllAsDynamicsViaTableNameForIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllAsDynamicsViaTableNameForIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<CompleteTable>(),
                tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllViaTableNameForNonIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllViaTableNameForNonIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.ElementAt(tables.IndexOf(table))));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllViaTableNameForNonIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllAsDynamicsViaTableNameForNonIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsDynamics(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Assert
            Assert.AreEqual(tables.Count, result);

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllAsDynamicsViaTableNameForNonIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionMergeAllAsDynamicsViaTableNameForNonIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = connection.MergeAll(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllViaTableNameAsyncForIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncAsExpandoObjectViaTableNameForIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsExpandoObjects(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());
            Assert.AreEqual(tables.Count, result);
            Assert.IsTrue(tables.All(table => ((dynamic)table).Id > 0));

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllViaTableNameAsyncForIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncAsExpandoObjectViaTableNameForIdentityForNonEmptyTable()
    {
        // Setup
        var entities = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            var tables = Helper.CreateCompleteTablesAsExpandoObjects(10).AsList();
            tables.ForEach(e => ((IDictionary<string, object>)e)["Id"] = entities[tables.IndexOf(e)].Id);

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(entities.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(entities.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(queryResult.First(e => e.Id == ((dynamic)table).Id), table));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllViaTableNameAsyncForIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncAsDynamicsViaTableNameForIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateCompleteTablesAsDynamics(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertMembersEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncAsDynamicsViaTableNameForIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncAsDynamicsViaTableNameForIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<CompleteTable>());

            // Act
            var queryResult = connection.QueryAll<CompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncViaTableNameForNonIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncViaTableNameForNonIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncViaTableNameForNonIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncAsDynamicsViaTableNameForNonIdentityForEmptyTable()
    {
        // Setup
        var tables = Helper.CreateNonIdentityCompleteTablesAsDynamics(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncAsDynamicsViaTableNameForNonIdentityForNonEmptyTable()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionMergeAllAsyncAsDynamicsViaTableNameForNonIdentityForNonEmptyTableWithQualifiers()
    {
        // Setup
        var tables = Database.CreateNonIdentityCompleteTables(10).AsList();
        var qualifiers = new[]
        {
            new Field("Id", typeof(long))
        };

        using (var connection = this.CreateTestConnection())
        {
            // Setup
            tables.ForEach(table => Helper.UpdateNonIdentityCompleteTableProperties(table));

            // Act
            var result = await connection.MergeAllAsync(ClassMappedNameCache.Get<NonIdentityCompleteTable>(),
                tables,
                qualifiers);

            // Assert
            Assert.AreEqual(tables.Count, connection.CountAll<NonIdentityCompleteTable>());

            // Act
            var queryResult = connection.QueryAll<NonIdentityCompleteTable>();

            // Assert
            Assert.AreEqual(tables.Count, queryResult.Count());
            tables.ForEach(table => Helper.AssertPropertiesEquality(table, queryResult.First(e => e.Id == table.Id)));
        }
    }

    #endregion

    #endregion
}
