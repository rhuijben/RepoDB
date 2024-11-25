﻿using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Models;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests.Setup
{
    public static class Database
    {
        static Database()
        {
            // Get the environment variable
            var variable = Environment.GetEnvironmentVariable("REPODB_IS_IN_MEMORY", EnvironmentVariableTarget.Process);

            // Set the property
            IsInMemory = true; //string.Equals(variable, "TRUE", StringComparison.OrdinalIgnoreCase);
        }

        #region Properties

        /// <summary>
        /// Gets or sets the connection string to be used (for MDS).
        /// </summary>
        public static string ConnectionStringMDS { get; private set; } = @"Data Source=C:\SqLite\Databases\RepoDb.db;";

        /// <summary>
        /// Gets the value that indicates whether to use the in-memory database.
        /// </summary>
        public static bool IsInMemory { get; private set; }

        #endregion

        #region Methods

        public static void Initialize()
        {
            // Initialize SqLite
            GlobalConfiguration
                .Setup()
                .UseSqlite();

            // Check the type of database
            if (IsInMemory == true)
            {
                // Memory
                ConnectionStringMDS = @"Data Source=:memory:;";
            }
            else
            {
                // Local
                ConnectionStringMDS = @"Data Source=C:\SqLite\Databases\RepoDb.db;";

                // Create tables
                CreateMdsTables();
            }
        }

        public static void Cleanup()
        {
            if (IsInMemory == true)
            {
                return;
            }
            using (var connection = new SqliteConnection(ConnectionStringMDS))
            {
                connection.DeleteAll<MdsCompleteTable>();
                connection.DeleteAll<MdsNonIdentityCompleteTable>();
            }
        }

        #endregion

        #region MdsCompleteTable

        public static IEnumerable<MdsCompleteTable> CreateMdsCompleteTables(int count,
            SqliteConnection connection = null)
        {
            var hasConnection = (connection != null);
            if (hasConnection == false)
            {
                connection = new SqliteConnection(ConnectionStringMDS);
            }
            try
            {
                var tables = Helper.CreateMdsCompleteTables(count);
                CreateMdsCompleteTable(connection);
                connection.InsertAll(tables);
                return tables;
            }
            finally
            {
                if (hasConnection == false)
                {
                    connection.Dispose();
                }
            }
        }

        #endregion

        #region MdsNonIdentityCompleteTable

        public static IEnumerable<MdsNonIdentityCompleteTable> CreateMdsNonIdentityCompleteTables(int count,
            SqliteConnection connection = null)
        {
            var hasConnection = (connection != null);
            if (hasConnection == false)
            {
                connection = new SqliteConnection(ConnectionStringMDS);
            }
            try
            {
                var tables = Helper.CreateMdsNonIdentityCompleteTables(count);
                CreateMdsNonIdentityCompleteTable(connection);
                connection.InsertAll(tables);
                return tables;
            }
            finally
            {
                if (hasConnection == false)
                {
                    connection.Dispose();
                }
            }
        }

        #endregion

        #region CreateMdsTables

        public static void CreateMdsTables(SqliteConnection connection = null)
        {
            CreateMdsCompleteTable(connection);
            CreateMdsNonIdentityCompleteTable(connection);
        }

        public static void CreateMdsCompleteTable(SqliteConnection connection = null)
        {
            var hasConnection = (connection != null);
            if (hasConnection == false)
            {
                connection = new SqliteConnection(ConnectionStringMDS);
            }
            try
            {
                /*
                 * Stated here: If the type if 'INTEGER PRIMARY KEY', it is automatically an identity table.
                 * No need to explicity specify the 'AUTOINCREMENT' keyword to avoid extra CPU and memory space.
                 * Link: https://sqlite.org/autoinc.html
                 */
                connection.ExecuteNonQuery(@"CREATE TABLE IF NOT EXISTS [MdsCompleteTable]
                    (
                        Id INTEGER PRIMARY KEY
                        , ColumnBigInt BIGINT
                        , ColumnBlob BLOB
                        , ColumnBoolean BOOLEAN
                        , ColumnChar CHAR
                        , ColumnDate DATE
                        , ColumnDateTime DATETIME
                        , ColumnDecimal DECIMAL
                        , ColumnDouble DOUBLE
                        , ColumnInteger INTEGER
                        , ColumnInt INT
                        , ColumnNone NONE
                        , ColumnNumeric NUMERIC
                        , ColumnReal REAL
                        , ColumnString STRING
                        , ColumnText TEXT
                        , ColumnTime TIME
                        , ColumnVarChar VARCHAR
                    );");
            }
            finally
            {
                if (hasConnection == false)
                {
                    connection.Dispose();
                }
            }
        }

        public static void CreateMdsNonIdentityCompleteTable(SqliteConnection connection = null)
        {
            var hasConnection = (connection != null);
            if (hasConnection == false)
            {
                connection = new SqliteConnection(ConnectionStringMDS);
            }
            try
            {
                connection.ExecuteNonQuery(@"CREATE TABLE IF NOT EXISTS [MdsNonIdentityCompleteTable]
                    (
                        Id VARCHAR PRIMARY KEY
                        , ColumnBigInt BIGINT
                        , ColumnBlob BLOB
                        , ColumnBoolean BOOLEAN
                        , ColumnChar CHAR
                        , ColumnDate DATE
                        , ColumnDateTime DATETIME
                        , ColumnDecimal DECIMAL
                        , ColumnDouble DOUBLE
                        , ColumnInteger INTEGER
                        , ColumnInt INT
                        , ColumnNone NONE
                        , ColumnNumeric NUMERIC
                        , ColumnReal REAL
                        , ColumnString STRING
                        , ColumnText TEXT
                        , ColumnTime TIME
                        , ColumnVarChar VARCHAR
                    );");
            }
            finally
            {
                if (hasConnection == false)
                {
                    connection.Dispose();
                }
            }
        }

        static string GetDbPath(TestContext tc)
        {
            return Path.Combine(tc.TestRunDirectory, "sqlite.db");
        }

        internal static void Initialize(TestContext testContext)
        {
            Initialize();
            //throw new NotImplementedException();
            using (var db = new SqliteConnection(GetConnectionString(testContext)))
            {
                db.EnsureOpen();
            }
        }

        internal static string GetConnectionString(TestContext testContext)
        {
            return "Datasource=" + GetDbPath(testContext).Replace(Path.DirectorySeparatorChar, '/');
        }

        #endregion
    }
}
