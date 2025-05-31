﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.UnitTests.CustomObjects;

namespace RepoDb.UnitTests.StatementBuilders;

[TestClass]
public class BaseStatementBuilderCreateSumAllTest
{
    [TestInitialize]
    public void Initialize()
    {
        StatementBuilderMapper.Add<BaseStatementBuilderDbConnection>(new CustomBaseStatementBuilder(), true);
        StatementBuilderMapper.Add<NonHintsSupportingBaseStatementBuilderDbConnection>(new CustomNonHintsSupportingBaseStatementBuilder(), true);
    }

    #region SubClasses

    private class BaseStatementBuilderDbConnection : CustomDbConnection { }

    private class NonHintsSupportingBaseStatementBuilderDbConnection : CustomDbConnection { }

    #endregion

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumAll()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateSumAll(field: field,
            tableName: tableName,
            hints: null);
        var expected = "SELECT SUM ([Value]) AS [SumValue] FROM [Table];";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumAllWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var hints = "WITH (NOLOCK)";

        // Act
        var actual = statementBuilder.CreateSumAll(tableName: tableName,
            field: field,
            hints: hints);
        var expected = "SELECT SUM ([Value]) AS [SumValue] FROM [Table] WITH (NOLOCK);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumAllWithQuotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "[dbo].[Table]";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateSumAll(tableName: tableName,
            field: field,
            hints: null);
        var expected = "SELECT SUM ([Value]) AS [SumValue] FROM [dbo].[Table];";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumAllWithUnquotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "dbo.Table";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateSumAll(tableName: tableName,
            field: field,
            hints: null);
        var expected = "SELECT SUM ([Value]) AS [SumValue] FROM [dbo].[Table];";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumAllIfTheTableIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        string? tableName = null;
        var field = new Field("Value");

        // Act
        statementBuilder.CreateSumAll(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumAllIfTheTableIsEmpty()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateSumAll(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumAllIfTheTableIsWhitespace()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateSumAll(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumAllIfTheFieldIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";

        // Act
        statementBuilder.CreateSumAll(tableName: tableName,
            field: null,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumAllIIfTheHintsAreNotSupported()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<NonHintsSupportingBaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateSumAll(tableName: tableName,
            field: field,
            hints: "Hints");
    }
}
