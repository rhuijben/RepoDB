﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using RepoDb.PostgreSql.IntegrationTests.Models;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Operations
{
    [TestClass]
    public class AverageAllTest
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
        public void TestPostgreSqlConnectionAverageAll()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new NpgsqlConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.AverageAll<CompleteTable>(e => e.ColumnInteger);

                // Assert
                Assert.AreEqual(tables.Average(e => e.ColumnInteger), Convert.ToDouble(result));
            }
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void ThrowExceptionOnPostgreSqlConnectionAverageAllWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new NpgsqlConnection(Database.ConnectionString))
            {
                // Act
                connection.AverageAll<CompleteTable>(e => e.ColumnInteger,
                    hints: "WhatEver");
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public async Task TestPostgreSqlConnectionAverageAllAsync()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new NpgsqlConnection(Database.ConnectionString))
            {
                // Act
                var result = await connection.AverageAllAsync<CompleteTable>(e => e.ColumnInteger);

                // Assert
                Assert.AreEqual(tables.Average(e => e.ColumnInteger), Convert.ToDouble(result));
            }
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public async Task ThrowExceptionOnPostgreSqlConnectionAverageAllAsyncWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new NpgsqlConnection(Database.ConnectionString))
            {
                // Act
                await connection.AverageAllAsync<CompleteTable>(e => e.ColumnInteger,
                    hints: "WhatEver");
            }
        }

        #endregion

        #endregion

        #region TableName

        #region Sync

        [TestMethod]
        public void TestPostgreSqlConnectionAverageAllViaTableName()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new NpgsqlConnection(Database.ConnectionString))
            {
                // Act
                var result = connection.AverageAll(ClassMappedNameCache.Get<CompleteTable>(),
                    Field.Parse<CompleteTable>(e => e.ColumnInteger).First());

                // Assert
                Assert.AreEqual(tables.Average(e => e.ColumnInteger), Convert.ToDouble(result));
            }
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public void ThrowExceptionOnPostgreSqlConnectionAverageAllViaTableNameWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new NpgsqlConnection(Database.ConnectionString))
            {
                // Act
                connection.AverageAll(ClassMappedNameCache.Get<CompleteTable>(),
                    Field.Parse<CompleteTable>(e => e.ColumnInteger).First(),
                    hints: "WhatEver");
            }
        }

        #endregion

        #region Async

        [TestMethod]
        public async Task TestPostgreSqlConnectionAverageAllAsyncViaTableName()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new NpgsqlConnection(Database.ConnectionString))
            {
                // Act
                var result = await connection.AverageAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    Field.Parse<CompleteTable>(e => e.ColumnInteger).First());

                // Assert
                Assert.AreEqual(tables.Average(e => e.ColumnInteger), Convert.ToDouble(result));
            }
        }

        [TestMethod, ExpectedException(typeof(NotSupportedException))]
        public async Task ThrowExceptionOnPostgreSqlConnectionAverageAllAsyncViaTableNameWithHints()
        {
            // Setup
            var tables = Database.CreateCompleteTables(10);

            using (var connection = new NpgsqlConnection(Database.ConnectionString))
            {
                // Act
                await connection.AverageAllAsync(ClassMappedNameCache.Get<CompleteTable>(),
                    Field.Parse<CompleteTable>(e => e.ColumnInteger).First(),
                    hints: "WhatEver");
            }
        }

        #endregion

        #endregion
    }
}
