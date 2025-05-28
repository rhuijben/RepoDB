using RepoDb.Exceptions;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb.StatementBuilders;

/// <summary>
/// A class used to build a SQL Statement for SQL Server. This is the default statement builder used by the library.
/// </summary>
public sealed class SqlServerStatementBuilder : BaseStatementBuilder
{
    /// <summary>
    /// Creates a new instance of <see cref="SqlServerStatementBuilder"/> object.
    /// </summary>
    /// <param name="dbSetting">The database settings object currently in used.</param>
    public SqlServerStatementBuilder(IDbSetting dbSetting)
        : this(dbSetting,
            new SqlServerConvertFieldResolver(),
            new ClientTypeToAverageableClientTypeResolver())
    { }

    /// <summary>
    /// Creates a new instance of <see cref="SqlServerStatementBuilder"/> class.
    /// </summary>
    /// <param name="dbSetting">The database settings object currently in used.</param>
    /// <param name="convertFieldResolver">The resolver used when converting a field in the database layer.</param>
    /// <param name="averageableClientTypeResolver">The resolver used to identity the type for average.</param>
    public SqlServerStatementBuilder(IDbSetting dbSetting,
        IResolver<Field, IDbSetting, string>? convertFieldResolver = null,
        IResolver<Type, Type> averageableClientTypeResolver = null)
        : base(dbSetting,
              (convertFieldResolver ?? new SqlServerConvertFieldResolver()),
              (averageableClientTypeResolver ?? new ClientTypeToAverageableClientTypeResolver()))
    { }

    #region CreateBatchQuery

    /// <summary>
    /// Creates a SQL Statement for batch query operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be queried.</param>
    /// <param name="page">The page of the batch.</param>
    /// <param name="rowsPerBatch">The number of rows per batch.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for batch query operation.</returns>
    public override string CreateBatchQuery(string tableName,
        IEnumerable<Field> fields,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy = null,
        QueryGroup? where = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // There should be fields
        if (fields?.Any() != true)
        {
            throw new MissingFieldsException(fields?.Select(f => f.Name));
        }

        // Validate order by
        if (orderBy == null || orderBy.Any() != true)
        {
            throw new EmptyException(nameof(orderBy), "The argument 'orderBy' is required.");
        }

        // Validate the page
        if (page < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(page), "The page must be equals or greater than 0.");
        }

        // Validate the page
        if (rowsPerBatch < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(rowsPerBatch), "The rows per batch must be equals or greater than 1.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .Select()
            .FieldsFrom(fields, DbSetting)
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .OrderByFrom(orderBy, DbSetting)
            .WriteText(string.Concat("OFFSET ", page * rowsPerBatch))
            .WriteText(string.Concat("ROWS FETCH NEXT " + rowsPerBatch + " ROWS ONLY"))
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateCount

    /// <summary>
    /// Creates a SQL Statement for count operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for count operation.</returns>
    public override string CreateCount(string tableName,
        QueryGroup? where = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .Select()
            .CountBig(null, DbSetting)
            .WriteText("AS [CountValue]")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateCountAll

    /// <summary>
    /// Creates a SQL Statement for count-all operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for count-all operation.</returns>
    public override string CreateCountAll(string tableName,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .Select()
            .CountBig(null, DbSetting)
            .WriteText("AS [CountValue]")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateInsert

    /// <summary>
    /// Creates a SQL Statement for insert operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be inserted.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for insert operation.</returns>
    public override string CreateInsert(string tableName,
        IEnumerable<Field>? fields,
        IEnumerable<DbField> keyFields,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);
        GuardHints(hints);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new EmptyException(nameof(fields), $"The list of insertable fields must not be null or empty for '{tableName}'.");
        }

        foreach (var keyField in keyFields)
        {
            if (keyField.IsGenerated || keyField.IsIdentity || keyField.IsNullable)
                continue;

            if (fields.GetByName(keyField.Name) is null)
            {
                throw new PrimaryFieldNotFoundException($"Primary field '{keyField.Name}' must be present in the field list.");
            }
        }

        // Insertable fields
        var insertableFields = fields
            .Where(f => keyFields.GetByName(f.Name) is not { } x || !(x.IsGenerated || x.IsIdentity));

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .Insert()
            .Into()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .OpenParen()
            .FieldsFrom(insertableFields, DbSetting)
            .CloseParen();

        if (keyFields?.Any() == true)
        {
            builder
                .Output()
                .AsAliasFieldsFrom(keyFields.AsFields().Take(1), "INSERTED", DbSetting);
        }

        builder
            .Values()
            .OpenParen()
            .ParametersFrom(insertableFields, 0, DbSetting)
            .CloseParen();

        builder.End(DbSetting);
        return builder.GetString();
    }

    #endregion

    #region CreateInsertAll

    /// <summary>
    /// Creates a SQL Statement for insert-all operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be inserted.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for insert operation.</returns>
    public override string CreateInsertAll(string tableName,
        IEnumerable<Field> fields,
        int batchSize,
        IEnumerable<DbField> keyFields,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);
        GuardHints(hints);

        // Validate the multiple statement execution
        ValidateMultipleStatementExecution(batchSize);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new EmptyException(nameof(fields), "The list of fields cannot be null or empty.");
        }

        // Primary Key
        foreach (var keyField in keyFields)
        {
            if (!keyField.IsPrimary || keyField.IsGenerated || keyField.IsIdentity || keyField.IsNullable)
                continue;

            if (fields.GetByName(keyField.Name) is null)
            {
                throw new PrimaryFieldNotFoundException($"Primary field '{keyField.Name}' must be present in the field list.");
            }
        }

        // Insertable fields
        var insertableFields = fields
            .Where(f => keyFields.GetByName(f.Name) is not { } x || !(x.IsGenerated || x.IsIdentity));

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .Insert()
            .Into()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .OpenParen()
            .FieldsFrom(insertableFields, DbSetting)
            .CloseParen();

        if (keyFields?.Any() == true)
        {
            builder
                .Output()
                .AsAliasFieldsFrom(keyFields.AsFields().Take(1), "INSERTED", DbSetting);
        }

        builder
            .Values();

        // Iterate the indexes
        for (var index = 0; index < batchSize; index++)
        {
            if (index > 0)
                builder.WriteText(",");

            builder
                .OpenParen()
                .ParametersFrom(insertableFields, index, DbSetting)
                .CloseParen();
        }

        builder.End(DbSetting);

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateMerge

    /// <summary>
    /// Creates a SQL Statement for merge operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be merged.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for merge operation.</returns>
    public override string CreateMerge(string tableName,
        IEnumerable<Field> fields,
        IEnumerable<Field>? qualifiers,
        IEnumerable<DbField> keyFields,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);
        GuardHints(hints);
        var primaryField = keyFields.FirstOrDefault(f => f.IsPrimary);
        var identityField = keyFields.FirstOrDefault(f => f.IsIdentity);
        GuardPrimary(primaryField);
        GuardIdentity(identityField);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new MissingFieldsException();
        }

        // Check the qualifiers
        if (qualifiers?.Any() == true)
        {
            // Check if the qualifiers are present in the given fields
            var unmatchesQualifiers = qualifiers.Where(field =>
                fields.FirstOrDefault(f =>
                    string.Equals(field.Name, f.Name, StringComparison.OrdinalIgnoreCase)) == null);

            // Throw an error we found any unmatches
            if (unmatchesQualifiers.Any() == true)
            {
                throw new InvalidQualifiersException($"The qualifiers '{unmatchesQualifiers.Select(field => field.Name).Join(", ")}' are not " +
                    $"present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
            }
        }
        else
        {
            if (primaryField != null)
            {
                // Make sure that primary is present in the list of fields before qualifying to become a qualifier
                var isPresent = fields.FirstOrDefault(f => string.Equals(f.Name, primaryField.Name, StringComparison.OrdinalIgnoreCase)) != null;

                // Throw if not present
                if (isPresent == false)
                {
                    throw new InvalidQualifiersException($"There are no qualifier field objects found for '{tableName}'. Ensure that the " +
                        $"primary field is present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
                }

                // The primary is present, use it as a default if there are no qualifiers given
                qualifiers = primaryField.AsField().AsEnumerable();
            }
            else
            {
                // Throw exception, qualifiers are not defined
                throw new MissingQualifierFieldsException($"There are no qualifier fields found for '{tableName}'.");
            }
        }

        // Get the insertable and updateable fields
        var insertableFields = fields
            .Where(field => !string.Equals(field.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase));
        var updateableFields = fields
            .Where(field => !string.Equals(field.Name, primaryField?.Name, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(field.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase));

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            // MERGE T USING S
            .Merge()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .As("T")
            .Using()
            .OpenParen()
            .Select()
            .ParametersAsFieldsFrom(fields, 0, DbSetting)
            .CloseParen()
            .As("S")
            // QUALIFIERS
            .On()
            .OpenParen()
            .WriteText(qualifiers?
                .Select(
                    field => field.AsJoinQualifier("S", "T", true, DbSetting))
                        .Join(" AND "))
            .CloseParen()
            // WHEN NOT MATCHED THEN INSERT VALUES
            .When()
            .Not()
            .Matched()
            .Then()
            .Insert()
            .OpenParen()
            .FieldsFrom(insertableFields, DbSetting)
            .CloseParen()
            .Values()
            .OpenParen()
            .AsAliasFieldsFrom(insertableFields, "S", DbSetting)
            .CloseParen()
            // WHEN MATCHED THEN UPDATE SET
            .When()
            .Matched()
            .Then()
            .Update()
            .Set()
            .FieldsAndAliasFieldsFrom(updateableFields, "T", "S", DbSetting);

        // Variables needed
        var keyColumn = GetReturnKeyColumnAsDbField(primaryField, identityField);

        // Set the output
        if (keyColumn != null)
        {
            builder
                .WriteText(string.Concat("OUTPUT INSERTED.", keyColumn.Name.AsField(DbSetting)))
                .As("[Result]");
        }

        // End the builder
        builder.End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateMergeAll

    /// <summary>
    /// Creates a SQL Statement for merge-all operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be merged.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for merge operation.</returns>
    public override string CreateMergeAll(string tableName,
        IEnumerable<Field> fields,
        IEnumerable<Field>? qualifiers,
        int batchSize,
        IEnumerable<DbField> keyFields,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);
        GuardHints(hints);
        var primaryField = keyFields.FirstOrDefault(f => f.IsPrimary);
        var identityField = keyFields.FirstOrDefault(f => f.IsIdentity);
        GuardPrimary(primaryField);
        GuardIdentity(identityField);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new MissingFieldsException();
        }

        // Check the qualifiers
        if (qualifiers?.Any() == true)
        {
            // Check if the qualifiers are present in the given fields
            var unmatchesQualifiers = qualifiers.Where(field =>
                fields.FirstOrDefault(f =>
                    string.Equals(field.Name, f.Name, StringComparison.OrdinalIgnoreCase)) == null);

            // Throw an error we found any unmatches
            if (unmatchesQualifiers.Any() == true)
            {
                throw new InvalidQualifiersException($"The qualifiers '{unmatchesQualifiers.Select(field => field.Name).Join(", ")}' are not " +
                    $"present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
            }
        }
        else
        {
            if (primaryField != null)
            {
                // Make sure that primary is present in the list of fields before qualifying to become a qualifier
                var isPresent = fields.FirstOrDefault(f => string.Equals(f.Name, primaryField.Name, StringComparison.OrdinalIgnoreCase)) != null;

                // Throw if not present
                if (isPresent == false)
                {
                    throw new InvalidQualifiersException($"There are no qualifier field objects found for '{tableName}'. Ensure that the " +
                        $"primary field is present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
                }

                // The primary is present, use it as a default if there are no qualifiers given
                qualifiers = primaryField.AsField().AsEnumerable();
            }
            else
            {
                // Throw exception, qualifiers are not defined
                throw new MissingQualifierFieldsException($"There are no qualifier fields found for '{tableName}'.");
            }
        }

        // Get the insertable and updateable fields
        var insertableFields = fields
            .Where(field => !string.Equals(field.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase));
        var updateableFields = fields
            .Where(field => !string.Equals(field.Name, primaryField?.Name, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(field.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase));

        // Initialize the builder
        var keyColumn = GetReturnKeyColumnAsDbField(primaryField, identityField);
        var builder = new QueryBuilder();

        // Iterate the indexes
        for (var index = 0; index < batchSize; index++)
        {
            // MERGE T USING S
            builder.Merge()
                .TableNameFrom(tableName, DbSetting)
                .HintsFrom(hints)
                .As("T")
                .Using()
                .OpenParen()
                .Select()
                .ParametersAsFieldsFrom(fields, index, DbSetting)
                .CloseParen()
                .As("S")
                // QUALIFIERS
                .On()
                .OpenParen()
                .WriteText(qualifiers?
                    .Select(
                        field => field.AsJoinQualifier("S", "T", true, DbSetting))
                            .Join(" AND "))
                .CloseParen()
                // WHEN NOT MATCHED THEN INSERT VALUES
                .When()
                .Not()
                .Matched()
                .Then()
                .Insert()
                .OpenParen()
                .FieldsFrom(insertableFields, DbSetting)
                .CloseParen()
                .Values()
                .OpenParen()
                .AsAliasFieldsFrom(insertableFields, "S", DbSetting)
                .CloseParen()
                // WHEN MATCHED THEN UPDATE SET
                .When()
                .Matched()
                .Then()
                .Update()
                .Set()
                .FieldsAndAliasFieldsFrom(updateableFields, "T", "S", DbSetting);

            // Set the output
            if (keyColumn != null)
            {
                builder
                    .WriteText(string.Concat("OUTPUT INSERTED.", keyColumn.Name.AsField(DbSetting)))
                        .As("[Result],")
                    .WriteText($"{DbSetting.ParameterPrefix}__RepoDb_OrderColumn_{index}")
                        .As("[__RepoDb_OrderColumn]");
            }

            // End the builder
            builder.End();
        }

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateSkipQuery

    /// <summary>
    /// Creates a SQL Statement for 'BatchQuery' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="skip">The number of rows to skip.</param>
    /// <param name="take">The number of rows per batch.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for batch query operation.</returns>
    public override string CreateSkipQuery(string tableName,
        IEnumerable<Field> fields,
        int skip,
        int take,
        IEnumerable<OrderField>? orderBy = null,
        QueryGroup? where = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // There should be fields
        if (fields?.Any() != true)
        {
            throw new MissingFieldsException(fields?.Select(f => f.Name));
        }

        // Validate order by
        if (orderBy == null || orderBy.Any() != true)
        {
            throw new EmptyException(nameof(orderBy), "The argument 'orderBy' is required.");
        }

        // Validate the skip
        if (skip < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(skip), "The rows skipped must be equals or greater than 0.");
        }

        // Validate the take
        if (take < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(take), "The rows per batch must be equals or greater than 1.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .With()
            .WriteText("CTE")
            .As()
            .OpenParen()
            .Select()
            .TopFrom(take + skip)
            .RowNumber()
            .Over()
            .OpenParen()
            .OrderByFrom(orderBy, DbSetting)
            .CloseParen()
            .As("[RowNumber],")
            .FieldsFrom(fields, DbSetting)
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .OrderByFrom(orderBy, DbSetting)
            .CloseParen()
            .Select()
            .FieldsFrom(fields, DbSetting)
            .From()
            .WriteText("CTE")
            .WriteText(string.Concat("WHERE ([RowNumber] BETWEEN ", skip + 1, " AND ", take + skip, ")"))
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion
}
