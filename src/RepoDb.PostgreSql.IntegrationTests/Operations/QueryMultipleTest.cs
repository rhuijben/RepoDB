﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.Extensions;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations;

[TestClass]
public class QueryMultipleTest
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
    public void TestPostgreSqlConnectionQueryMultipleForT2()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.QueryMultiple<CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionQueryMultipleForT3()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.QueryMultiple<CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionQueryMultipleForT4()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.QueryMultiple<CompleteTable, CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3,
                top4: 4);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            Assert.AreEqual(4, result.Item4.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item4.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionQueryMultipleForT5()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.QueryMultiple<CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3,
                top4: 4,
                top5: 5);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            Assert.AreEqual(4, result.Item4.Count());
            Assert.AreEqual(5, result.Item5.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item4.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item5.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionQueryMultipleForT6()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.QueryMultiple<CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3,
                top4: 4,
                top5: 5,
                top6: 6);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            Assert.AreEqual(4, result.Item4.Count());
            Assert.AreEqual(5, result.Item5.Count());
            Assert.AreEqual(6, result.Item6.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item4.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item5.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item6.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public void TestPostgreSqlConnectionQueryMultipleForT7()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = connection.QueryMultiple<CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3,
                top4: 4,
                top5: 5,
                top6: 6,
                top7: 7);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            Assert.AreEqual(4, result.Item4.Count());
            Assert.AreEqual(5, result.Item5.Count());
            Assert.AreEqual(6, result.Item6.Count());
            Assert.AreEqual(7, result.Item7.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item4.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item5.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item6.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item7.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionQueryMultipleWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            connection.QueryMultiple<CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                hints1: "WhatEver",
                top2: 2,
                hints2: "WhatEver");
        }
    }

    #endregion

    #region Async

    [TestMethod]
    public async Task TestPostgreSqlConnectionQueryMultipleAsyncForT2()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.QueryMultipleAsync<CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionQueryMultipleAsyncForT3()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.QueryMultipleAsync<CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionQueryMultipleAsyncForT4()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.QueryMultipleAsync<CompleteTable, CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3,
                top4: 4);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            Assert.AreEqual(4, result.Item4.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item4.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionQueryMultipleAsyncForT5()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.QueryMultipleAsync<CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3,
                top4: 4,
                top5: 5);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            Assert.AreEqual(4, result.Item4.Count());
            Assert.AreEqual(5, result.Item5.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item4.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item5.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionQueryMultipleAsyncForT6()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.QueryMultipleAsync<CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3,
                top4: 4,
                top5: 5,
                top6: 6);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            Assert.AreEqual(4, result.Item4.Count());
            Assert.AreEqual(5, result.Item5.Count());
            Assert.AreEqual(6, result.Item6.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item4.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item5.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item6.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod]
    public async Task TestPostgreSqlConnectionQueryMultipleAsyncForT7()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            var result = await connection.QueryMultipleAsync<CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                top2: 2,
                top3: 3,
                top4: 4,
                top5: 5,
                top6: 6,
                top7: 7);

            // Assert
            Assert.AreEqual(1, result.Item1.Count());
            Assert.AreEqual(2, result.Item2.Count());
            Assert.AreEqual(3, result.Item3.Count());
            Assert.AreEqual(4, result.Item4.Count());
            Assert.AreEqual(5, result.Item5.Count());
            Assert.AreEqual(6, result.Item6.Count());
            Assert.AreEqual(7, result.Item7.Count());
            result.Item1.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item2.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item3.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item4.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item5.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item6.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
            result.Item7.AsList().ForEach(item => Helper.AssertPropertiesEquality(tables.First(e => e.Id == item.Id), item));
        }
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public async Task ThrowExceptionQueryMultipleAsyncWithHints()
    {
        // Setup
        var tables = Database.CreateCompleteTables(10);

        using (var connection = this.CreateTestConnection())
        {
            // Act
            await connection.QueryMultipleAsync<CompleteTable, CompleteTable>(e => e.Id > 0,
                e => e.Id > 0,
                top1: 1,
                hints1: "WhatEver",
                top2: 2,
                hints2: "WhatEver");
        }
    }

    #endregion

    #endregion
}
