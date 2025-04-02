﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.UnitTests.CustomObjects;

namespace RepoDb.UnitTests.StatementBuilders;

[TestClass]
public class BaseStatementBuilderCreateSumTest
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
    public void TestBaseStatementBuilderCreateSum()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateSum(field: field,
            tableName: tableName,
            hints: null);
        var expected = "SELECT SUM ([Value]) AS [SumValue] FROM [Table] ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumWithWhereExpression()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var where = new QueryGroup(new QueryField("Id", 1));

        // Act
        var actual = statementBuilder.CreateSum(tableName: tableName,
            field: field,
            where: where);
        var expected = $"" +
            $"SELECT SUM ([Value]) AS [SumValue] " +
            $"FROM [Table] " +
            $"WHERE ([Id] = @Id) ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var hints = "WITH (NOLOCK)";

        // Act
        var actual = statementBuilder.CreateSum(tableName: tableName,
            field: field,
            hints: hints);
        var expected = "SELECT SUM ([Value]) AS [SumValue] FROM [Table] WITH (NOLOCK) ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumWithWhereExpressionAndWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var where = new QueryGroup(new QueryField("Id", 1));
        var hints = "WITH (NOLOCK)";

        // Act
        var actual = statementBuilder.CreateSum(tableName: tableName,
            field: field,
            where: where,
            hints: hints);
        var expected = $"" +
            $"SELECT SUM ([Value]) AS [SumValue] " +
            $"FROM [Table] WITH (NOLOCK) " +
            $"WHERE ([Id] = @Id) ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumWithQuotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "[dbo].[Table]";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateSum(tableName: tableName,
            field: field,
            hints: null);
        var expected = "SELECT SUM ([Value]) AS [SumValue] FROM [dbo].[Table] ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateSumWithUnquotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "dbo.Table";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateSum(tableName: tableName,
            field: field,
            hints: null);
        var expected = "SELECT SUM ([Value]) AS [SumValue] FROM [dbo].[Table] ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumIfTheTableIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = (string?)null;
        var field = new Field("Value");

        // Act
        statementBuilder.CreateSum(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumIfTheTableIsEmpty()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateSum(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumIfTheTableIsWhitespace()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateSum(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumIfTheFieldIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";

        // Act
        statementBuilder.CreateSum(tableName: tableName,
            field: null,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateSumIIfTheHintsAreNotSupported()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<NonHintsSupportingBaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateSum(tableName: tableName,
            field: field,
            hints: "Hints");
    }
}
