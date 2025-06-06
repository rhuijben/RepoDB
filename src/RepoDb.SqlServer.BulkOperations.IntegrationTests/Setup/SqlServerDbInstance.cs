﻿using System.Data.Common;
using Microsoft.Data.SqlClient;
using RepoDb.TestCore;

namespace RepoDb.SqlServer.BulkOperations.IntegrationTests;

public class SqlServerDbInstance : DbInstance<SqlConnection>
{
    static SqlServerDbInstance()
    {
        GlobalConfiguration.Setup(GlobalConfiguration.Options).UseSqlServer();

        TypeMapper.Add(typeof(DateTime), System.Data.DbType.DateTime2, true);
    }

    public SqlServerDbInstance()
    {
        // Master connection
        AdminConnectionString =
            Environment.GetEnvironmentVariable("REPODB_SQLSERVER_CONSTR_MASTER")
            ?? @"Server=tcp:127.0.0.1,41433;Database=master;User ID=sa;Password=ddd53e85-b15e-4da8-91e5-a7d3b00a0ab2;TrustServerCertificate=True;"; // Docker Test Configuration

        // RepoDb connection
        ConnectionString =
            Environment.GetEnvironmentVariable("REPODB_SQLSERVER_CONSTR_REPODBBULK")
            ?? new SqlConnectionStringBuilder(AdminConnectionString) { InitialCatalog = DatabaseName }.ToString();
    }

    protected override async Task CreateUserDatabase(DbConnection sql)
    {
        await sql.ExecuteNonQueryAsync($@"IF (NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{DatabaseName}'))
                BEGIN
                    CREATE DATABASE [{DatabaseName}];
                END");
    }
}
