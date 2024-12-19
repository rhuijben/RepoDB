using System.Data.Common;
using MySqlConnector;
using RepoDb.TestCore;

namespace RepoDb.MySqlConnector.IntegrationTests.Setup;

public class MysqlDbInstance : DbInstance<MySqlConnection>
{
    static MysqlDbInstance()
    {
        GlobalConfiguration.Setup(GlobalConfiguration.Options).UseMySqlConnector();
    }

    public MysqlDbInstance()
    {
        AdminConnectionString =
            Environment.GetEnvironmentVariable("REPODB_MYSQL_CONSTR_SYS")
            ?? @"Server=127.0.0.1;Port=43306;Database=sys;User ID=root;Password=ddd53e85-b15e-4da8-91e5-a7d3b00a0ab2;"; // Docker test configuration

        ConnectionString =
            Environment.GetEnvironmentVariable("REPODB_MYSQL_CONSTR_REPODBTEST")
            ?? @"Server=127.0.0.1;Port=43306;Database=RepoDbTest;User ID=root;Password=ddd53e85-b15e-4da8-91e5-a7d3b00a0ab2;"; // Docker test configuration
    }

    protected async override Task CreateUserDatabase(DbConnection sql)
    {
        await sql.ExecuteNonQueryAsync(@"CREATE DATABASE IF NOT EXISTS `RepoDb`;");
        await sql.ExecuteNonQueryAsync(@"GRANT ALL Privileges on RepoDb.* to 'root'@'%';");
    }
}
