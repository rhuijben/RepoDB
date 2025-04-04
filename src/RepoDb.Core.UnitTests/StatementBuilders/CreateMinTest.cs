﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.UnitTests.CustomObjects;

namespace RepoDb.UnitTests.StatementBuilders;

[TestClass]
public class BaseStatementBuilderCreateMinTest
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
    public void TestBaseStatementBuilderCreateMin()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateMin(field: field,
            tableName: tableName,
            hints: null);
        var expected = "SELECT MIN ([Value]) AS [MinValue] FROM [Table] ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMinWithWhereExpression()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var where = new QueryGroup(new QueryField("Id", 1));

        // Act
        var actual = statementBuilder.CreateMin(tableName: tableName,
            field: field,
            where: where);
        var expected = $"" +
            $"SELECT MIN ([Value]) AS [MinValue] " +
            $"FROM [Table] " +
            $"WHERE ([Id] = @Id) ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMinWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var hints = "WITH (NOLOCK)";

        // Act
        var actual = statementBuilder.CreateMin(tableName: tableName,
            field: field,
            hints: hints);
        var expected = "SELECT MIN ([Value]) AS [MinValue] FROM [Table] WITH (NOLOCK) ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMinWithWhereExpressionAndWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");
        var where = new QueryGroup(new QueryField("Id", 1));
        var hints = "WITH (NOLOCK)";

        // Act
        var actual = statementBuilder.CreateMin(tableName: tableName,
            field: field,
            where: where,
            hints: hints);
        var expected = $"" +
            $"SELECT MIN ([Value]) AS [MinValue] " +
            $"FROM [Table] WITH (NOLOCK) " +
            $"WHERE ([Id] = @Id) ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMinWithQuotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "[dbo].[Table]";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateMin(tableName: tableName,
            field: field,
            hints: null);
        var expected = "SELECT MIN ([Value]) AS [MinValue] FROM [dbo].[Table] ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateMinWithUnquotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "dbo.Table";
        var field = new Field("Value");

        // Act
        var actual = statementBuilder.CreateMin(tableName: tableName,
            field: field,
            hints: null);
        var expected = "SELECT MIN ([Value]) AS [MinValue] FROM [dbo].[Table] ;";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMinIfTheTableIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        string? tableName = null;
        var field = new Field("Value");

        // Act
        statementBuilder.CreateMin(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMinIfTheTableIsEmpty()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateMin(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMinIfTheTableIsWhitespace()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateMin(tableName: tableName,
            field: field,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMinIfTheFieldIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";

        // Act
        statementBuilder.CreateMin(tableName: tableName,
            field: null,
            hints: null);
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateMinIIfTheHintsAreNotSupported()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<NonHintsSupportingBaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var field = new Field("Value");

        // Act
        statementBuilder.CreateMin(tableName: tableName,
            field: field,
            hints: "Hints");
    }
}
