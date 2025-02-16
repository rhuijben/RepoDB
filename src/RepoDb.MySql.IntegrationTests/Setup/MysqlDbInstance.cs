using System.Data.Common;
using MySql.Data.MySqlClient;
using RepoDb.TestCore;

namespace RepoDb.MySql.IntegrationTests.Setup;

public class MysqlDbInstance : DbInstance<MySqlConnection>
{
    static MysqlDbInstance()
    {
        GlobalConfiguration.Setup().UseMySql();
    }

    public MysqlDbInstance()
    {
        AdminConnectionString =
            Environment.GetEnvironmentVariable("REPODB_MYSQL_CONSTR_SYS")
            ?? @"Server=127.0.0.1;Port=43306;Database=sys;User ID=root;Password=ddd53e85-b15e-4da8-91e5-a7d3b00a0ab2;";

        ConnectionString =
            Environment.GetEnvironmentVariable("REPODB_MYSQL_CONSTR_REPODB")
            ?? new MySqlConnectionStringBuilder(AdminConnectionString) { Database = "RepoDb" }.ToString();
    }

    protected override async Task CreateUserDatabase(DbConnection sql)
    {
        await sql.ExecuteNonQueryAsync(@"CREATE DATABASE IF NOT EXISTS `RepoDb`;");
        await sql.ExecuteNonQueryAsync(@"GRANT ALL Privileges on RepoDb.* to 'root'@'%';");
    }
}
