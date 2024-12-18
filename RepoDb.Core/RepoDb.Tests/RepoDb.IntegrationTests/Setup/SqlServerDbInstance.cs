using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using RepoDb.TestCore;

namespace RepoDb.IntegrationTests.Setup
{
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
                Environment.GetEnvironmentVariable("REPODB_SQLSERVER_CONSTR_REPODB")
                ?? "Server=tcp:127.0.0.1,41433;Database=RepoDb;User ID=sa;Password=ddd53e85-b15e-4da8-91e5-a7d3b00a0ab2;TrustServerCertificate=True;"; // Docker test configuration
        }

        protected override async Task CreateUserDatabase(DbConnection sql)
        {
            await sql.ExecuteNonQueryAsync(@"IF (NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'RepoDbTest'))
                BEGIN
                    CREATE DATABASE [RepoDbTest];
                END");
        }
    }
}
