using Oracle.ManagedDataAccess.Client;
using RepoDb.Exceptions;
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
    public OracleStatementBuilder()
        : this(DbSettingMapper.Get<OracleConnection>(),
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
        IResolver<Type, Type> averageableClientTypeResolver = null)
        : base(dbSetting,
              convertFieldResolver,
              averageableClientTypeResolver)
    { }

    public override string CreateBatchQuery(string tableName, IEnumerable<Field> fields, int page, int rowsPerBatch, IEnumerable<OrderField>? orderBy = null, QueryGroup? where = null, string? hints = null)
    {
        throw new NotImplementedException();
    }

    public override string CreateMerge(string tableName, IEnumerable<Field> fields, IEnumerable<Field>? qualifiers = null, DbField? primaryField = null, DbField? identityField = null, string? hints = null)
    {
        throw new NotImplementedException();
    }

    public override string CreateMergeAll(string tableName, IEnumerable<Field> fields, IEnumerable<Field>? qualifiers = null, int batchSize = 10, DbField? primaryField = null, DbField? identityField = null, string? hints = null)
    {
        throw new NotImplementedException();
    }

    public override string CreateSkipQuery(string tableName, IEnumerable<Field> fields, int skip, int take, IEnumerable<OrderField>? orderBy = null, QueryGroup? where = null, string? hints = null)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc cref="BaseStatementBuilder.CreateInsert"/>
    public override string CreateInsert(string tableName,
     IEnumerable<Field>? fields = null,
     DbField? primaryField = null,
     DbField? identityField = null,
     string? hints = null)
    {
        // Initialize the builder
        var builder = new QueryBuilder();

        // Call the base method (assumes it creates INSERT INTO ... VALUES (...);)
        builder.WriteText(
            base.CreateInsert(tableName,
                fields,
                primaryField,
                identityField,
                hints));

        // Get the key column to return
        var keyColumn = GetReturnKeyColumnAsDbField(primaryField, identityField);

        // If no return value is needed, return the base SQL
        if (keyColumn is { })
        {
            // Oracle requires RETURNING <column> INTO :outParam
            builder
                .Returning()
                .FieldFrom(keyColumn.AsField(), DbSetting)
                .Into()
                .WriteText(":RepoDb_Result");
        }

        return builder.GetString();
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
        builder.Clear()
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
        return builder.GetString();
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
        builder.Clear()
            .Select()
            .WriteText($"1 AS {("ExistsValue").AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .FetchFirstRowsOnly(1);

        // Return the query
        return builder.GetString();
    }
}
