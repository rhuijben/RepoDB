using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.SqlServer.IntegrationTests.Setup;

namespace RepoDb.SqlServer.IntegrationTests.Common
{
    [TestClass]
    public class NullTests : RepoDb.TestCore.NullTestsBase<SqlServerDbInstance>
    {
        protected override void InitializeCore() => Database.Initialize();

        public override DbConnection CreateConnection() => new SqlConnection(Database.ConnectionString);
    }
}
