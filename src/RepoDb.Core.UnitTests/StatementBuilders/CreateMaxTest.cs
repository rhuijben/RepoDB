﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.UnitTests.CustomObjects;

namespace RepoDb.UnitTests.StatementBuilders;

[TestClass]
public class BaseStatementBuilderCreateMaxTest
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
    public void TestBaseStatementBuilderCreateMax()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateMax(field: field,
            tableName: tableName,
            hints: null);
        var expected = "SELECT MAX ([Value]) AS [MaxValue] FROM [Table];";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMaxWithWhereExpression()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var where = new QueryGroup(new QueryField("Id", 1));

        // Act
        var actual = statementBuilder.CreateMax(tableName: tableName,
            field: field,
            where: where);
        var expected = $"" +
            $"SELECT MAX ([Value]) AS [MaxValue] " +
            $"FROM [Table] " +
            $"WHERE ([Id] = @Id);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMaxWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var hints = "WITH (NOLOCK)";

        // Act
        var actual = statementBuilder.CreateMax(tableName: tableName,
            field: field,
            hints: hints);
        var expected = "SELECT MAX ([Value]) AS [MaxValue] FROM [Table] WITH (NOLOCK);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMaxWithWhereExpressionAndWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var where = new QueryGroup(new QueryField("Id", 1));
        var hints = "WITH (NOLOCK)";

        // Act
        var actual = statementBuilder.CreateMax(tableName: tableName,
            field: field,
            where: where,
            hints: hints);
        var expected = $"" +
            $"SELECT MAX ([Value]) AS [MaxValue] " +
            $"FROM [Table] WITH (NOLOCK) " +
            $"WHERE ([Id] = @Id);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMaxWithQuotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "[dbo].[Table]";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateMax(tableName: tableName,
            field: field,
            hints: null);
        var expected = "SELECT MAX ([Value]) AS [MaxValue] FROM [dbo].[Table];";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMaxWithUnquotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "dbo.Table";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateMax(tableName: tableName,
            field: field,
            hints: null);
        var expected = "SELECT MAX ([Value]) AS [MaxValue] FROM [dbo].[Table];";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMaxIfTheTableIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        string? tableName = null;
        var field = new Field("Value");

        // Act
        statementBuilder.CreateMax(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMaxIfTheTableIsEmpty()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateMax(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMaxIfTheTableIsWhitespace()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateMax(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMaxIfTheFieldIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";

        // Act
        statementBuilder.CreateMax(tableName: tableName,
            field: null,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMaxIIfTheHintsAreNotSupported()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<NonHintsSupportingBaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateMax(tableName: tableName,
            field: field,
            hints: "Hints");
    }
}
