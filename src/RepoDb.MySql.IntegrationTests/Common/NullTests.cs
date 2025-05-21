﻿using System.Data.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;
using RepoDb.MySql.IntegrationTests.Setup;

namespace RepoDb.MySql.IntegrationTests.Common;

[TestClass]
public class NullTests : RepoDb.TestCore.NullTestsBase<MysqlDbInstance>
{
    protected override void InitializeCore() => Database.Initialize();

    public override DbConnection CreateConnection() => new MySqlConnection(Database.ConnectionString);

    public override string UuidDbType => "CHAR(38)";
    public override string DateTimeOffsetDbType => "varchar(64)"; // DateTimeOffset is not supported in MySQL

    public override string GeneratedColumnDefinition(string expression, string type) => $"{type} {base.GeneratedColumnDefinition(expression, type)} STORED";

    protected override string IdentityDefinition => "INT NOT NULL AUTO_INCREMENT";
}
