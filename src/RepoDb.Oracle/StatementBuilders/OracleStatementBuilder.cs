﻿using RepoDb.Exceptions;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb.StatementBuilders;

/// <summary>
/// A class used to build a SQL Statement for PostgreSql.
/// </summary>
public sealed class OracleStatementBuilder : BaseStatementBuilder
{
    /// <summary>
    /// Creates a new instance of <see cref="OracleStatementBuilder"/> object.
    /// </summary>
    public OracleStatementBuilder(IDbSetting setting)
        : this(setting,
              new OracleConvertFieldResolver(),
              new ClientTypeToAverageableClientTypeResolver())
    { }

    /// <summary>
    /// Creates a new instance of <see cref="OracleStatementBuilder"/> class.
    /// </summary>
    /// <param name="dbSetting">The database settings object currently in used.</param>
    /// <param name="convertFieldResolver">The resolver used when converting a field in the database layer.</param>
    /// <param name="averageableClientTypeResolver">The resolver used to identity the type for average.</param>
    public OracleStatementBuilder(IDbSetting dbSetting,
        IResolver<Field, IDbSetting, string>? convertFieldResolver = null,
        IResolver<Type, Type?>? averageableClientTypeResolver = null)
        : base(dbSetting,
              convertFieldResolver,
              averageableClientTypeResolver)
    { }

    public override string CreateBatchQuery(string tableName, IEnumerable<Field> fields, int page, int rowsPerBatch, IEnumerable<OrderField>? orderBy = null, QueryGroup? where = null, string? hints = null)
    {
        throw new NotImplementedException();
    }

    public override string CreateMerge(string tableName,
                                   IEnumerable<Field> fields,
                                   IEnumerable<Field>? qualifiers,
                                   IEnumerable<DbField> keyFields,
                                   string? hints = null)
    {
        if (tableName == null)
            throw new ArgumentNullException(nameof(tableName));
        if (fields == null)
            throw new ArgumentNullException(nameof(fields));

        var primaryField = keyFields.FirstOrDefault(f => f.IsPrimary);
        var identityField = keyFields.FirstOrDefault(f => f.IsIdentity);

        var fieldList = fields.ToList();
        if (fieldList.Count == 0)
            throw new InvalidOperationException("No fields to merge.");

        var qualifierList = qualifiers?.ToList() ?? new List<Field>();

        // Ensure qualifiers exist
        if (qualifierList.Count == 0)
            throw new InvalidOperationException("Qualifiers must be specified for MERGE operation in Oracle.");

        // Create SELECT :param AS Col1, :param AS Col2 ...
        var sourceColumns = string.Join(", ", fieldList.Select(f => $"{f.Name.AsParameter(true, DbSetting)} AS {f.Name.AsQuoted(DbSetting)}"));

        // ON condition
        var onConditions = string.Join(" AND ", qualifierList.Select(q =>
            $"T.{q.Name.AsQuoted(DbSetting)} = S.{q.Name.AsQuoted(DbSetting)}"));

        // UPDATE SET T.ColX = S.ColX (exclude qualifiers and identity fields)
        var updateFields = fieldList
            .Where(f => !qualifierList.Any(q => q.Name == f.Name) &&
                        (identityField == null || identityField.Name != f.Name))
            .ToList();

        // INSERT clause
        var insertColumns = fieldList
            .Where(f => identityField == null || identityField.Name != f.Name)
            .ToList();

        var builder = new QueryBuilder();
        builder
            .Merge()
            .Into()
            .TableNameFrom(tableName, DbSetting)
            .WriteText("T")
            .Using()
            .OpenParen()
                .Select()
                .WriteText(sourceColumns)
                .From()
                .WriteText("DUAL")
            .CloseParen()
            .WriteText("S")
            .On()
            .OpenParen()
            .WriteText(onConditions)
            .CloseParen();

        if (updateFields.Any())
        {
            builder
                .When()
                .Matched()
                .Then()
                .Update()
                .Set()
                .WriteText(string.Join(", ", updateFields.Select(f => $"T.{f.Name.AsQuoted(DbSetting)} = S.{f.Name.AsQuoted(DbSetting)}")));
        }

        if (insertColumns.Any())
        {
            builder
                .When()
                .Not()
                .Matched()
                .Then()
                .Insert()
                .OpenParen()
                .WriteText(string.Join(", ", insertColumns.Select(f => f.Name.AsQuoted(DbSetting))))
                .CloseParen()
                .Values()
                .OpenParen()
                .WriteText(string.Join(", ", insertColumns.Select(f => $"S.{f.Name.AsQuoted(DbSetting)}")))
                .CloseParen();
        }

        return builder.ToString();
    }

    public override string CreateMergeAll(string tableName, IEnumerable<Field> fields, IEnumerable<Field>? qualifiers, int batchSize, IEnumerable<DbField> keyFields, string? hints = null)
    {
        throw new NotImplementedException();
    }

    public override string CreateSkipQuery(string tableName, IEnumerable<Field> fields, int skip, int take, IEnumerable<OrderField>? orderBy = null, QueryGroup? where = null, string? hints = null)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc cref="BaseStatementBuilder.CreateInsert"/>
    public override string CreateInsert(string tableName,
     IEnumerable<Field> fields,
     IEnumerable<DbField> keyFields,
     string? hints = null)
    {
        // Initialize the builder
        var builder = new QueryBuilder();

        // Call the base method (assumes it creates INSERT INTO ... VALUES (...);)
        builder.WriteText(
            base.CreateInsert(tableName,
                fields,
                keyFields,
                hints));

        var primaryField = keyFields.FirstOrDefault(f => f.IsPrimary);
        var identityField = keyFields.FirstOrDefault(f => f.IsIdentity);

        // If an identityField is present, add output handling
        if (identityField is { })
        {
            // Oracle requires RETURNING <column> INTO :outParam
            builder
                .Returning()
                .FieldFrom(identityField.AsField(), DbSetting)
                .Into()
                .WriteText(":RepoDb_Result");
        }

        return builder.ToString();
    }

    public override string CreateInsertAll(string tableName, IEnumerable<Field>? fields, int batchSize, IEnumerable<DbField> keyFields, string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);
        GuardHints(hints);

        var primaryField = keyFields.FirstOrDefault(f => f.IsPrimary);
        var identityField = keyFields.FirstOrDefault(f => f.IsIdentity);

        GuardPrimary(primaryField);
        GuardIdentity(identityField);

        // Validate the multiple statement execution
        ValidateMultipleStatementExecution(batchSize);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new EmptyException(nameof(fields), "The list of fields cannot be null or empty.");
        }

        // Primary Key
        if (primaryField != null &&
            primaryField.HasDefaultValue == false &&
            !string.Equals(primaryField.Name, identityField?.Name, StringComparison.OrdinalIgnoreCase))
        {
            var isPresent = fields
                .FirstOrDefault(f =>
                    string.Equals(f.Name, primaryField.Name, StringComparison.OrdinalIgnoreCase)) != null;

            if (isPresent == false)
            {
                throw new PrimaryFieldNotFoundException($"As the primary field '{primaryField.Name}' is not an identity nor has a default value, it must be present on the insert operation.");
            }
        }

        return "/*FORALL*/" + CreateInsert(tableName, fields, keyFields, hints);
    }

    /// <inheritdoc cref="BaseStatementBuilder.CreateQuery"/>
    public override string CreateQuery(string tableName,
        IEnumerable<Field> fields,
        QueryGroup? where = null,
        IEnumerable<OrderField>? orderBy = null,
        int top = 0,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // There should be fields
        if (fields?.Any() != true)
        {
            throw new EmptyException($"The list of queryable fields must not be null for '{tableName}'.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .Select()
            .FieldsFrom(fields, DbSetting)
            .From()
            .TableNameFrom(tableName, DbSetting)
            .WhereFrom(where, DbSetting)
            .OrderByFrom(orderBy, DbSetting);

        if (top > 0)
        {
            builder
                .FetchFirstRowsOnly(top);
        }

        // Return the query
        return builder.ToString();
    }

    public override string CreateExists(string tableName, QueryGroup? where = null, string? hints = null)
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
            .WriteText($"1 AS {("ExistsValue").AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .FetchFirstRowsOnly(1);

        // Return the query
        return builder.ToString();
    }
}
