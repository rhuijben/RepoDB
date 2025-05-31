﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Exceptions;
using RepoDb.UnitTests.CustomObjects;

namespace RepoDb.UnitTests.StatementBuilders;

[TestClass]
public class BaseStatementBuilderCreateUpdateAllTest
{
    [TestInitialize]
    public void Initialize()
    {
        StatementBuilderMapper.Add<BaseStatementBuilderDbConnection>(new CustomBaseStatementBuilder(), true);
        StatementBuilderMapper.Add<SingleStatementSupportBaseStatementBuilderDbConnection>(new CustomSingleStatementSupportBaseStatementBuilder(), true);
    }

    #region SubClasses

    private class BaseStatementBuilderDbConnection : CustomDbConnection { }

    private class SingleStatementSupportBaseStatementBuilderDbConnection : CustomDbConnection { }

    #endregion

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAll()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null);
        var expected = $"" +
            $"UPDATE [Table] " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllDualPK()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1", "Field2");

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null);
        var expected = $"" +
            $"UPDATE [Table] " +
            $"SET [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1 AND [Field2] = @Field2);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllWithQuotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "[dbo].[Table]";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null);
        var expected = $"" +
            $"UPDATE [dbo].[Table] " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllWithUnquotedTableSchema()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "dbo.Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null);
        var expected = $"" +
            $"UPDATE [dbo].[Table] " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllWithCoveredPrimaryField()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var field = new DbField("Field1", true, true, false, typeof(int), null, null, null, null);

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: null,
            batchSize: 1,
            primaryField: field,
            identityField: null);
        var expected = $"" +
            $"UPDATE [Table] " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllWithCoveredIdentityField()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");
        var field = new DbField("Field1", true, true, false, typeof(int), null, null, null, null);

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: field);
        var expected = $"" +
            $"UPDATE [Table] " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllWithCoveredPrimaryAsIdentity()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");
        var field = new DbField("Field1", true, true, false, typeof(int), null, null, null, null);

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: field,
            identityField: field);
        var expected = $"" +
            $"UPDATE [Table] " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllForThreeBatches()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 3,
            primaryField: null,
            identityField: null);
        var expected = $"" +
            $"UPDATE [Table] " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1); " +
            $"UPDATE [Table] " +
            $"SET [Field2] = @Field2_1, [Field3] = @Field3_1 " +
            $"WHERE ([Field1] = @Field1_1); " +
            $"UPDATE [Table] " +
            $"SET [Field2] = @Field2_2, [Field3] = @Field3_2 " +
            $"WHERE ([Field1] = @Field1_2);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null,
            hints: "WITH (TABLOCK)");
        var expected = $"" +
            $"UPDATE [Table] WITH (TABLOCK) " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod]
    public void TestBaseStatementBuilderCreateUpdateAllForThreeBatchesWithHints()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        var actual = statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 3,
            primaryField: null,
            identityField: null,
            hints: "WITH (TABLOCK)");
        var expected = $"" +
            $"UPDATE [Table] WITH (TABLOCK) " +
            $"SET [Field2] = @Field2, [Field3] = @Field3 " +
            $"WHERE ([Field1] = @Field1); " +
            $"UPDATE [Table] WITH (TABLOCK) " +
            $"SET [Field2] = @Field2_1, [Field3] = @Field3_1 " +
            $"WHERE ([Field1] = @Field1_1); " +
            $"UPDATE [Table] WITH (TABLOCK) " +
            $"SET [Field2] = @Field2_2, [Field3] = @Field3_2 " +
            $"WHERE ([Field1] = @Field1_2);";

        // Assert
        Assert.AreEqual(expected, actual);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateUpdateAllIfTheTableIsNull()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        string? tableName = null;
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateUpdateAllIfTheTableIsEmpty()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateUpdateAllIfTheTableIsWhitespace()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = " ";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null);
    }

    [TestMethod, ExpectedException(typeof(InvalidOperationException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateUpdateAllIfThePrimaryIsNotReallyAPrimary()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");
        var primaryField = new DbField("Field1", false, false, false, typeof(int), null, null, null, null);

        // Act
        statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: primaryField,
            identityField: null);
    }

    [TestMethod, ExpectedException(typeof(InvalidOperationException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateUpdateAllIfTheIdentityIsNotReallyAnIdentity()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");
        var identifyField = new DbField("Field2", false, false, false, typeof(int), null, null, null, null);

        // Act
        statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: identifyField);
    }

    [TestMethod, ExpectedException(typeof(InvalidQualifiersException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateUpdateAllIfAnyOfTheQualifierIsNotCovered()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Id");

        // Act
        statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 1,
            primaryField: null,
            identityField: null);
    }

    [TestMethod, ExpectedException(typeof(ArgumentNullException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateUpdateAllIfThereAreNoQualifiers()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<BaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });

        // Act
        statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: null,
            batchSize: 1,
            primaryField: null,
            identityField: null);
    }

    [TestMethod, ExpectedException(typeof(NotSupportedException))]
    public void ThrowExceptionOnBaseStatementBuilderCreateUpdateAllIfTheBatchSizeIsGreaterThan1AndTheMultipleStatementExecutionIsNotSupported()
    {
        // Setup
        var statementBuilder = StatementBuilderMapper.Get<SingleStatementSupportBaseStatementBuilderDbConnection>();
        var tableName = "Table";
        var fields = Field.From(new[] { "Field1", "Field2", "Field3" });
        var qualifiers = Field.From("Field1");

        // Act
        statementBuilder.CreateUpdateAll(tableName: tableName,
            fields: fields,
            qualifiers: qualifiers,
            batchSize: 10,
            primaryField: null,
            identityField: null);
    }
}
