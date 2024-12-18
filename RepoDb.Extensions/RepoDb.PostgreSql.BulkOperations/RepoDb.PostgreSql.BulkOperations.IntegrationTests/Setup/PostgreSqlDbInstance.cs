using System;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;
using RepoDb.TestCore;

namespace RepoDb.IntegrationTests.Setup;

public class PostgreSqlDbInstance : DbInstance<NpgsqlConnection>
{
    static PostgreSqlDbInstance()
    {
        GlobalConfiguration.Setup(GlobalConfiguration.Options).UsePostgreSql();
    }

    public PostgreSqlDbInstance()
    {
        // Master connection
        AdminConnectionString =
            Environment.GetEnvironmentVariable("REPODB_POSTGRESQL_CONSTR_POSTGRESDB")
            ?? "Server=127.0.0.1;Port=45432;Database=postgres;User Id=postgres;Password=ddd53e85-b15e-4da8-91e5-a7d3b00a0ab2;"; // Docker test configuration

        // RepoDb connection
        ConnectionString =
            Environment.GetEnvironmentVariable("REPODB_POSTGRESQL_CONSTR")
            ?? "Server=127.0.0.1;Port=45432;Database=RepoDbBulk;User Id=postgres;Password=ddd53e85-b15e-4da8-91e5-a7d3b00a0ab2;"; // Docker test configuration; // Docker test configuration
    }

    protected override async Task CreateUserDatabase(DbConnection sql)
    {
        var recordCount = await sql.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM pg_database WHERE datname = 'RepoDb';");
        if (recordCount <= 0)
        {
            await sql.ExecuteNonQueryAsync(@"CREATE DATABASE ""RepoDb""
                        WITH OWNER = ""postgres""
                        ENCODING = ""UTF8""
                        CONNECTION LIMIT = -1;");
        }
    }
}
