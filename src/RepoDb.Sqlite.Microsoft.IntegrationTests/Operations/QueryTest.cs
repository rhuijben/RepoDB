﻿using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Extensions;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Models;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Setup;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests.Operations.MDS;

[TestClass]
public class QueryTest
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
    public void TestSqLiteConnectionQueryViaPrimaryKey()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Query<MdsCompleteTable>(table.Id).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Query<MdsCompleteTable>(e => e.Id == table.Id).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaDynamic()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Query<MdsCompleteTable>(new { table.Id }).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaQueryField()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Query<MdsCompleteTable>(new QueryField("Id", table.Id)).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaQueryFields()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();
            var queryFields = new[]
            {
                new QueryField("Id", table.Id),
                new QueryField("ColumnInt", table.ColumnInt)
            };

            // Act
            var result = connection.Query<MdsCompleteTable>(queryFields).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaQueryGroup()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();
            var queryFields = new[]
            {
                new QueryField("Id", table.Id),
                new QueryField("ColumnInt", table.ColumnInt)
            };
            var queryGroup = new QueryGroup(queryFields);

            // Act
            var result = connection.Query<MdsCompleteTable>(queryGroup).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryWithTop()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = connection.Query<MdsCompleteTable>((object?)null,
                top: 2);

            // Assert
            Assert.AreEqual(2, result.Count());
            result.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionQueryWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            connection.Query<MdsCompleteTable>((object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaPrimaryKey()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = (await connection.QueryAsync<MdsCompleteTable>(table.Id)).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaExpression()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = (await connection.QueryAsync<MdsCompleteTable>(e => e.Id == table.Id)).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaDynamic()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = (await connection.QueryAsync<MdsCompleteTable>(new { table.Id })).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaQueryField()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = (await connection.QueryAsync<MdsCompleteTable>(new QueryField("Id", table.Id))).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaQueryFields()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();
            var queryFields = new[]
            {
                new QueryField("Id", table.Id),
                new QueryField("ColumnInt", table.ColumnInt)
            };

            // Act
            var result = (await connection.QueryAsync<MdsCompleteTable>(queryFields)).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaQueryGroup()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();
            var queryFields = new[]
            {
                new QueryField("Id", table.Id),
                new QueryField("ColumnInt", table.ColumnInt)
            };
            var queryGroup = new QueryGroup(queryFields);

            // Act
            var result = (await connection.QueryAsync<MdsCompleteTable>(queryGroup)).First();

            // Assert
            Helper.AssertPropertiesEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncWithTop()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = await connection.QueryAsync<MdsCompleteTable>((object?)null,
                top: 2);

            // Assert
            Assert.AreEqual(2, result.Count());
            result.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionQueryAsyncWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            await connection.QueryAsync<MdsCompleteTable>((object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion

    #region TableName

    #region Sync

    [TestMethod]
    public void TestSqLiteConnectionQueryViaTableNameViaPrimaryKey()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Query(ClassMappedNameCache.Get<MdsCompleteTable>(), table.Id).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaTableNameViaDynamic()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Query(ClassMappedNameCache.Get<MdsCompleteTable>(), new { table.Id }).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaTableNameViaQueryField()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = connection.Query(ClassMappedNameCache.Get<MdsCompleteTable>(), new QueryField("Id", table.Id)).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaTableNameViaQueryFields()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();
            var queryFields = new[]
            {
                new QueryField("Id", table.Id),
                new QueryField("ColumnInt", table.ColumnInt)
            };

            // Act
            var result = connection.Query(ClassMappedNameCache.Get<MdsCompleteTable>(), queryFields).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaTableNameViaQueryGroup()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();
            var queryFields = new[]
            {
                new QueryField("Id", table.Id),
                new QueryField("ColumnInt", table.ColumnInt)
            };
            var queryGroup = new QueryGroup(queryFields);

            // Act
            var result = connection.Query(ClassMappedNameCache.Get<MdsCompleteTable>(), queryGroup).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public void TestSqLiteConnectionQueryViaTableNameWithTop()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = connection.Query(ClassMappedNameCache.Get<MdsCompleteTable>(),
                (object?)null,
                top: 2);

            // Assert
            Assert.AreEqual(2, result.Count());
            result.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionQueryViaTableNameWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            connection.Query(ClassMappedNameCache.Get<MdsCompleteTable>(),
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaTableNameViaPrimaryKey()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = (await connection.QueryAsync(ClassMappedNameCache.Get<MdsCompleteTable>(), table.Id)).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaTableNameViaDynamic()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = (await connection.QueryAsync(ClassMappedNameCache.Get<MdsCompleteTable>(), new { table.Id })).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaTableNameViaQueryField()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            var result = (await connection.QueryAsync(ClassMappedNameCache.Get<MdsCompleteTable>(), new QueryField("Id", table.Id))).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaTableNameViaQueryFields()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();
            var queryFields = new[]
            {
                new QueryField("Id", table.Id),
                new QueryField("ColumnInt", table.ColumnInt)
            };

            // Act
            var result = (await connection.QueryAsync(ClassMappedNameCache.Get<MdsCompleteTable>(), queryFields)).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaTableNameViaQueryGroup()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();
            var queryFields = new[]
            {
                new QueryField("Id", table.Id),
                new QueryField("ColumnInt", table.ColumnInt)
            };
            var queryGroup = new QueryGroup(queryFields);

            // Act
            var result = (await connection.QueryAsync(ClassMappedNameCache.Get<MdsCompleteTable>(), queryGroup)).First();

            // Assert
            Helper.AssertMembersEquality(table, result);
        }
    }

    [TestMethod]
    public async Task TestSqLiteConnectionQueryAsyncViaTableNameWithTop()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var tables = Database.CreateMdsCompleteTables(10, connection);

            // Act
            var result = await connection.QueryAsync(ClassMappedNameCache.Get<MdsCompleteTable>(),
                (object?)null,
                top: 2);

            // Assert
            Assert.AreEqual(2, result.Count());
            result.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionQueryAsyncViaTableNameWithHints()
    {
        using (var connection = new SqliteConnection(Database.ConnectionString))
        {
            // Setup
            var table = Database.CreateMdsCompleteTables(1, connection).First();

            // Act
            await connection.QueryAsync(ClassMappedNameCache.Get<MdsCompleteTable>(),
                (object?)null,
                hints: "WhatEver");
        }
    }

    #endregion

    #endregion
}
