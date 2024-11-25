using System.Data.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;
using RepoDb.MySql.IntegrationTests.Setup;

namespace RepoDb.MySql.IntegrationTests.Common
{
    [TestClass]
    public class EnumTests : RepoDb.TestCore.EnumTestsBase<MysqlDbInstance>
    {
        protected override void InitializeCore() => Database.Initialize();

        public override DbConnection CreateConnection() => new MySqlConnection(Database.ConnectionString);
    }
}
