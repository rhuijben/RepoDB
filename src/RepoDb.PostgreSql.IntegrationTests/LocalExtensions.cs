using Npgsql;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests;
internal static class LocalExtensions
{
    public static NpgsqlConnection CreateTestConnection(this object q)
    {
        return new NpgsqlConnection(Database.ConnectionString);
    }
}
