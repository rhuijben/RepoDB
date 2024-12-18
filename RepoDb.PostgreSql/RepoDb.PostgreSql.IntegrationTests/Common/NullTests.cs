using System.Data.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.PostgreSql.IntegrationTests.Setup;

namespace RepoDb.PostgreSql.IntegrationTests.Common;

[TestClass]
public class NullTests : RepoDb.TestCore.NullTestsBase<PostgreSqlDbInstance>
{
    protected override void InitializeCore() => Database.Initialize();

    public override DbConnection CreateConnection() => new Npgsql.NpgsqlConnection(Database.ConnectionString);
}
