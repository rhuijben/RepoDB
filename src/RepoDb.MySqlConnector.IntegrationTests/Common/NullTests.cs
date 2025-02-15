using System.Data.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySqlConnector;
using RepoDb.MySqlConnector.IntegrationTests.Setup;

namespace RepoDb.MySqlConnector.IntegrationTests.Common;

[TestClass]
public class NullTests : RepoDb.TestCore.NullTestsBase<MysqlDbInstance>
{
    protected override void InitializeCore() => Database.Initialize();

    public override DbConnection CreateConnection() => new MySqlConnection(Database.ConnectionString);

    public override string UuidDbType => "CHAR(38)";
}
