﻿using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Attributes.Parameter.SqlServer;
using RepoDb.DbSettings;
using RepoDb.Extensions;

namespace RepoDb.SqlServer.UnitTests.Attributes.Parameter.SqlServer;

[TestClass]
public class TypeNameAttributeTest
{
    [TestInitialize]
    public void Initialize()
    {
        DbSettingMapper.Add<SqlConnection>(new SqlServerDbSetting(), true);
    }

    #region Classes

    private class TypeNameAttributeTestClass
    {
        [TypeName("TypeName")]
        public object ColumnName { get; set; }
    }

    #endregion

    [TestMethod]
    public void TestTypeNameAttributeViaEntityViaCreateParameters()
    {
        // Act
        using (var connection = new SqlConnection())
        {
            using (var command = connection.CreateCommand())
            {
                DbCommandExtension
                    .CreateParameters(command, new TypeNameAttributeTestClass
                    {
                        ColumnName = "Test"
                    });

                // Assert
                Assert.AreEqual(1, command.Parameters.Count);

                // Assert
                var parameter = command.Parameters["@ColumnName"];
                Assert.AreEqual("TypeName", parameter.TypeName);
            }
        }
    }

    [TestMethod]
    public void TestTypeNameAttributeViaAnonymousViaCreateParameters()
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
                    typeof(TypeNameAttributeTestClass));

                // Assert
                Assert.AreEqual(1, command.Parameters.Count);

                // Assert
                var parameter = command.Parameters["@ColumnName"];
                Assert.AreEqual("TypeName", parameter.TypeName);
            }
        }
    }
}
