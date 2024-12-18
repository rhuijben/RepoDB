﻿using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Attributes.Parameter.SqlServer;
using RepoDb.DbSettings;
using RepoDb.Extensions;

namespace RepoDb.SqlServer.UnitTests.Attributes.Parameter.SqlServer;

[TestClass]
public class XmlSchemaCollectionNameAttributeTest
{
    [TestInitialize]
    public void Initialize()
    {
        DbSettingMapper.Add<SqlConnection>(new SqlServerDbSetting(), true);
    }

    #region Classes

    private class XmlSchemaCollectionNameAttributeTestClass
    {
        [XmlSchemaCollectionName("XmlSchemaCollectionName")]
        public object ColumnName { get; set; }
    }

    #endregion

    [TestMethod]
    public void TestXmlSchemaCollectionNameAttributeViaEntityViaCreateParameters()
    {
        // Act
        using (var connection = new SqlConnection())
        {
            using (var command = connection.CreateCommand())
            {
                DbCommandExtension
                    .CreateParameters(command, new XmlSchemaCollectionNameAttributeTestClass
                    {
                        ColumnName = "Test"
                    });

                // Assert
                Assert.AreEqual(1, command.Parameters.Count);

                // Assert
                var parameter = command.Parameters["@ColumnName"];
                Assert.AreEqual("XmlSchemaCollectionName", parameter.XmlSchemaCollectionName);
            }
        }
    }

    [TestMethod]
    public void TestXmlSchemaCollectionNameAttributeViaAnonymousViaCreateParameters()
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
                    typeof(XmlSchemaCollectionNameAttributeTestClass));

                // Assert
                Assert.AreEqual(1, command.Parameters.Count);

                // Assert
                var parameter = command.Parameters["@ColumnName"];
                Assert.AreEqual("XmlSchemaCollectionName", parameter.XmlSchemaCollectionName);
            }
        }
    }
}
