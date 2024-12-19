using System.Data.Common;
using System.Data.SQLite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.SQLite.System.IntegrationTests.Setup;

namespace RepoDb.SQLite.System.IntegrationTests.Common;

[TestClass]
public class NullTests : RepoDb.TestCore.NullTestsBase<SQLiteDbInstance>
{
    protected override void InitializeCore() => Database.Initialize(TestContext);

    public override DbConnection CreateConnection() => new SQLiteConnection(Database.GetConnectionString(TestContext));
}
