﻿using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Attributes.Parameter.SqlServer;
using RepoDb.DbSettings;
using RepoDb.Extensions;

namespace RepoDb.SqlServer.UnitTests.Attributes.Parameter.SqlServer;

[TestClass]
public class ForceColumnEncryptionAttributeTest
{
    [TestInitialize]
    public void Initialize()
    {
        DbSettingMapper.Add<SqlConnection>(new SqlServerDbSetting(), true);
    }

    #region Classes

    private class ForceColumnEncryptionAttributeTestClass
    {
        [ForceColumnEncryption(true)]
        public object ColumnName { get; set; }
    }

    #endregion

    [TestMethod]
    public void TestForceColumnEncryptionAttributeViaEntityViaCreateParameters()
    {
        // Act
        using (var connection = new SqlConnection())
        {
            using (var command = connection.CreateCommand())
            {
                DbCommandExtension
                    .CreateParameters(command, new ForceColumnEncryptionAttributeTestClass
                    {
                        ColumnName = "Test"
                    });

                // Assert
                Assert.AreEqual(1, command.Parameters.Count);

                // Assert
                var parameter = command.Parameters["@ColumnName"];
                Assert.IsTrue(parameter.ForceColumnEncryption);
            }
        }
    }

    [TestMethod]
    public void TestForceColumnEncryptionAttributeViaAnonymousViaCreateParameters()
    {
        // Act
        using (var connection = new SqlConnection())
        {
            using (var command = connection.CreateCommand())
            {
                DbCommandExtension
                    .CreateParameters(command, new
                    {
                        ColumnName = "Test"
                    },
                    typeof(ForceColumnEncryptionAttributeTestClass));

                // Assert
                Assert.AreEqual(1, command.Parameters.Count);

                // Assert
                var parameter = command.Parameters["@ColumnName"];
                Assert.IsTrue(parameter.ForceColumnEncryption);
            }
        }
    }
}
