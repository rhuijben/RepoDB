using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Sqlite.Microsoft.IntegrationTests.Setup;

namespace RepoDb.Sqlite.Microsoft.IntegrationTests.Common
{
    [TestClass]
    public class NullTests : RepoDb.TestCore.NullTestsBase<SqliteDbInstance>
    {
        protected override void InitializeCore() => Database.Initialize(TestContext);

        public override DbConnection CreateConnection() => new SqliteConnection(Database.GetConnectionString(TestContext));
    }
}
