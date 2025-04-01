using RepoDb.Enumerations;
using RepoDb.Exceptions;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb.StatementBuilders;

/// <summary>
/// A base class for all <see cref="IStatementBuilder"/>-based objects.
/// </summary>
public abstract class BaseStatementBuilder : IStatementBuilder
{
    /// <summary>
    /// Creates a new instance of <see cref="BaseStatementBuilder"/> class.
    /// </summary>
    /// <param name="dbSetting">The database settings object currently in used.</param>
    /// <param name="convertFieldResolver">The resolver used when converting a field in the database layer.</param>
    /// <param name="averageableClientTypeResolver">The resolver used to identity the type for average.</param>
    public BaseStatementBuilder(IDbSetting dbSetting,
        IResolver<Field, IDbSetting, string>? convertFieldResolver = null,
        IResolver<Type, Type> averageableClientTypeResolver = null)
    {
        DbSetting = dbSetting ?? throw new ArgumentNullException("The database setting cannot be null.");
        ConvertFieldResolver = convertFieldResolver;
        AverageableClientTypeResolver = averageableClientTypeResolver;
    }

    #region Properties

    /// <summary>
    /// Gets the database setting object that is currently in used.
    /// </summary>
    protected IDbSetting DbSetting { get; }

    /// <summary>
    /// Gets the resolver used to convert the <see cref="Field"/> object.
    /// </summary>
    protected IResolver<Field, IDbSetting, string> ConvertFieldResolver { get; }

    /// <summary>
    /// Gets the resolver that is being used to resolve the type to be averageable type.
    /// </summary>
    protected IResolver<Type, Type> AverageableClientTypeResolver { get; }

    #endregion

    #region Virtual/Common

    #region CreateAverage

    /// <summary>
    /// Creates a SQL Statement for 'Average' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be averaged.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for average operation.</returns>
    public virtual string CreateAverage(string tableName,
        DbFieldCollection dbFields,
        Field field,
        QueryGroup? where = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Check the field
        if (field == null)
        {
            throw new ArgumentNullException("The field cannot be null.");
        }
        else
        {
            field = new Field(field.Name, AverageableClientTypeResolver?.Resolve(field.Type ?? DbSetting.AverageableType));
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Average(field, DbSetting, ConvertFieldResolver)
            .WriteText($"AS {"AverageValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateAverageAll

    /// <summary>
    /// Creates a SQL Statement for 'AverageAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be averaged.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for average-all operation.</returns>
    public virtual string CreateAverageAll(string tableName,
        DbFieldCollection dbFields,
        Field field,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Check the field
        if (field == null)
        {
            throw new ArgumentNullException("The field cannot be null.");
        }
        else
        {
            field = new(field.Name, AverageableClientTypeResolver?.Resolve(field.Type ?? DbSetting.AverageableType));
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Average(field, DbSetting, ConvertFieldResolver)
            .WriteText($"AS {"AverageValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateCount

    /// <summary>
    /// Creates a SQL Statement for 'Count' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for count operation.</returns>
    public virtual string CreateCount(string tableName,
        DbFieldCollection dbFields,
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
        builder.Clear()
            .Select()
            .Count(null, DbSetting)
            .WriteText($"AS {"CountValue".AsQuoted(DbSetting)}")
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
    /// Creates a SQL Statement for 'CountAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for count-all operation.</returns>
    public virtual string CreateCountAll(string tableName,
        DbFieldCollection dbFields,
        string? hints = null)
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
            .Count(null, DbSetting)
            .WriteText($"AS {"CountValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateDelete

    /// <summary>
    /// Creates a SQL Statement for 'Delete' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for delete operation.</returns>
    public virtual string CreateDelete(string tableName,
        DbFieldCollection dbFields,
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
        builder.Clear()
            .Delete()
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateDeleteAll

    /// <summary>
    /// Creates a SQL Statement for 'DeleteAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for delete-all operation.</returns>
    public virtual string CreateDeleteAll(string tableName,
        DbFieldCollection dbFields,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Delete()
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateExists

    /// <summary>
    /// Creates a SQL Statement for 'Exists' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for exists operation.</returns>
    public virtual string CreateExists(
        string tableName,
        DbFieldCollection dbFields,
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
        builder.Clear()
            .Select()
            .TopFrom(1)
            .WriteText($"1 AS {("ExistsValue").AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateInsert

    /// <summary>
    /// Creates a SQL Statement for 'Insert' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be inserted.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for insert operation.</returns>
    public virtual string CreateInsert(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field>? fields = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);
        GuardHints(hints);
        //GuardPrimary(primaryField);
        //GuardIdentity(identityField);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new EmptyException($"The list of insertable fields must not be null or empty for '{tableName}'.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Insert()
            .Into()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .OpenParen()
            .FieldsFrom(fields, DbSetting)
            .CloseParen()
            .Values()
            .OpenParen()
            .ParametersFrom(fields, 0, DbSetting)
            .CloseParen()
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateInsertAll

    /// <summary>
    /// Creates a SQL Statement for 'InsertAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be inserted.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for insert operation.</returns>
    public virtual string CreateInsertAll(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field>? fields,
        int batchSize = Constant.DefaultBatchOperationSize,
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
            throw new EmptyException("The list of fields cannot be null or empty.");
        }

        // Primary Key
        if (dbFields.GetPrimaryFields() is { } primary)
        {
            foreach (var primaryField in primary)
            {
                if (!primaryField.HasDefaultValue && !primaryField.IsReadOnly
                    && !fields.Any(f => string.Equals(f.Name, primaryField.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new PrimaryFieldNotFoundException($"As the primary field '{primaryField.Name}' is not an identity nor has a default value, it must be present on the insert operation.");
                }
            }
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear();

        // Compose
        builder
            .Insert()
            .Into()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .OpenParen()
            .FieldsFrom(fields, DbSetting)
            .CloseParen()
            .Select()
            .FieldsFrom(fields, DbSetting)
            .From()
            .OpenParen()
            .Values();

        // Iterate the indexes
        for (var index = 0; index < batchSize; index++)
        {
            builder
                .OpenParen()
                .ParametersFrom(fields, index, DbSetting)
                .WriteText(
                    string.Concat(", ",
                        $"{DbSetting.ParameterPrefix}__RepoDb_OrderColumn_{index}"))
                .CloseParen();

            if (index < batchSize - 1)
            {
                builder
                    .WriteText(",");
            }
        }

        // Close
        builder
            .CloseParen()
            .As("T")
            .OpenParen()
            .FieldsFrom(fields, DbSetting)
            .WriteText(string.Concat(", ", "__RepoDb_OrderColumn".AsQuoted(DbSetting)))
            .CloseParen()
            .OrderBy()
            .WriteText("__RepoDb_OrderColumn".AsQuoted(DbSetting))
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateMax

    /// <summary>
    /// Creates a SQL Statement for 'Max' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be maximized.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for maximum operation.</returns>
    public virtual string CreateMax(string tableName,
        DbFieldCollection dbFields,
        Field field,
        QueryGroup? where = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Check the field
        if (field == null)
        {
            throw new ArgumentNullException("The field cannot be null.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Max(field, DbSetting)
            .WriteText($"AS {"MaxValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateMaxAll

    /// <summary>
    /// Creates a SQL Statement for 'MaxAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be maximized.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for maximum-all operation.</returns>
    public virtual string CreateMaxAll(string tableName,
        DbFieldCollection dbFields,
        Field field,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Check the field
        if (field == null)
        {
            throw new ArgumentNullException("The field cannot be null.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Max(field, DbSetting)
            .WriteText($"AS {"MaxValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateMin

    /// <summary>
    /// Creates a SQL Statement for 'Min' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be minimized.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for minimum operation.</returns>
    public virtual string CreateMin(string tableName,
        DbFieldCollection dbFields,
        Field field,
        QueryGroup? where = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Check the field
        if (field == null)
        {
            throw new ArgumentNullException("The field cannot be null.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Min(field, DbSetting)
            .WriteText($"AS {"MinValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateMinAll

    /// <summary>
    /// Creates a SQL Statement for 'MinAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be minimized.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for minimum-all operation.</returns>
    public virtual string CreateMinAll(string tableName,
        DbFieldCollection dbFields,
        Field field,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Check the field
        if (field == null)
        {
            throw new ArgumentNullException("The field cannot be null.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Min(field, DbSetting)
            .WriteText($"AS {"MinValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateQuery

    /// <summary>
    /// Creates a SQL Statement for 'Query' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="top">The number of rows to be returned.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for query operation.</returns>
    public virtual string CreateQuery(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        QueryGroup? where = null,
        IEnumerable<OrderField>? orderBy = null,
        int? top = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // There should be fields
        if (fields?.Any() != true)
        {
            throw new ArgumentNullException($"The list of queryable fields must not be null for '{tableName}'.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .TopFrom(top)
            .FieldsFrom(fields, DbSetting)
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .OrderByFrom(orderBy, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateQueryAll

    /// <summary>
    /// Creates a SQL Statement for 'QueryAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for query operation.</returns>
    public virtual string CreateQueryAll(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IEnumerable<OrderField>? orderBy = null,
        string? hints = null)
    {
        // Guard the target table
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // There should be fields
        if (fields?.Any() != true)
        {
            throw new ArgumentNullException($"The list of queryable fields must not be null for '{tableName}'.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .FieldsFrom(fields, DbSetting)
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .OrderByFrom(orderBy, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateSum

    /// <summary>
    /// Creates a SQL Statement for 'Sum' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be summarized.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for sum operation.</returns>
    public virtual string CreateSum(string tableName,
        DbFieldCollection dbFields,
        Field field,
        QueryGroup? where = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Check the field
        if (field == null)
        {
            throw new ArgumentNullException("The field cannot be null.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Sum(field, DbSetting)
            .WriteText($"AS {"SumValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateSumAll

    /// <summary>
    /// Creates a SQL Statement for 'SumAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be summarized.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for sum-all operation.</returns>
    public virtual string CreateSumAll(string tableName,
        DbFieldCollection dbFields,
        Field field,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);

        // Validate the hints
        GuardHints(hints);

        // Check the field
        if (field == null)
        {
            throw new ArgumentNullException("The field cannot be null.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Sum(field, DbSetting)
            .WriteText($"AS {"SumValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateTruncate

    /// <summary>
    /// Creates a SQL Statement for 'Truncate' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <returns>A sql statement for truncate operation.</returns>
    public virtual string CreateTruncate(string tableName,
        DbFieldCollection dbFields)
    {
        // Guard the target table
        GuardTableName(tableName);

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Truncate()
            .Table()
            .TableNameFrom(tableName, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateUpdate

    /// <summary>
    /// Creates a SQL Statement for 'Update' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be updated.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for update operation.</returns>
    public virtual string CreateUpdate(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        QueryGroup? where = null,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);
        GuardHints(hints);

        // Gets the updatable fields
        var updatableFields = fields
            .Where(f => dbFields.GetByName(f.Name) is { } dbf && !(dbf.IsReadOnly || dbf.IsPrimary));

        // Check if there are updatable fields
        if (updatableFields.Any() != true)
        {
            throw new EmptyException("The list of updatable fields cannot be null or empty.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear()
            .Update()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .Set()
            .FieldsAndParametersFrom(updatableFields, 0, DbSetting)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    #endregion

    #region CreateUpdateAll

    /// <summary>
    /// Creates a SQL Statement for 'UpdateAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be updated.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for update-all operation.</returns>
    public virtual string CreateUpdateAll(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IEnumerable<Field> qualifiers,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null)
    {
        // Ensure with guards
        GuardTableName(tableName);
        GuardHints(hints);

        // Validate the multiple statement execution
        ValidateMultipleStatementExecution(batchSize);

        // Ensure the fields
        if (fields?.Any() != true)
        {
            throw new EmptyException($"The list of fields cannot be null or empty.");
        }

        // Check the qualifiers
        if (qualifiers?.Any() == true)
        {
            // Check if the qualifiers are present in the given fields
            var unmatchesQualifiers = qualifiers.Where(field =>
                fields?.FirstOrDefault(f =>
                    string.Equals(field.Name, f.Name, StringComparison.OrdinalIgnoreCase)) == null);

            // Throw an error we found any unmatches
            if (unmatchesQualifiers.Any() == true)
            {
                throw new InvalidQualifiersException($"The qualifiers '{unmatchesQualifiers.Select(field => field.Name).Join(", ")}' are not " +
                    $"present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
            }
        }
        else if (dbFields.GetPrimaryFields() is { } primaryFields)
        {
            foreach (var primaryField in primaryFields)
            {
                if (!fields.Any(f => string.Equals(f.Name, primaryField.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new InvalidQualifiersException($"There are no qualifier field objects found for '{tableName}'. Ensure that the " +
                        $"primary field is present at the given fields '{fields.Select(field => field.Name).Join(", ")}'.");
                }
            }

            // The primary is present, use it as a default if there are no qualifiers given
            qualifiers = primaryFields.AsFields();
        }
        else
        {
            // Throw exception, qualifiers are not defined
            throw new ArgumentNullException($"There are no qualifier field objects found for '{tableName}'.");
        }

        // Gets the updatable fields
        fields = fields
            .Where(f => dbFields.GetByName(f.Name) is { } dbf && !dbf.IsReadOnly && !qualifiers.Any(q => string.Equals(q.Name, f.Name, StringComparison.OrdinalIgnoreCase)));

        // Check if there are updatable fields
        if (fields.Any() != true)
        {
            throw new EmptyException("The list of updatable fields cannot be null or empty.");
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder.Clear();

        // Iterate the indexes
        for (var index = 0; index < batchSize; index++)
        {
            builder
                .Update()
                .TableNameFrom(tableName, DbSetting)
                .HintsFrom(hints)
                .Set()
                .FieldsAndParametersFrom(fields, index, DbSetting)
                .WhereFrom(qualifiers, index, DbSetting)
                .End();
        }

        // Return the query
        return builder.GetString();
    }

    #endregion

    #endregion

    #region Abstract

    #region CreateBatchQuery

    /// <summary>
    /// Creates a SQL Statement for 'BatchQuery' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="page">The page of the batch.</param>
    /// <param name="rowsPerBatch">The number of rows per batch.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for batch query operation.</returns>
    public abstract string CreateBatchQuery(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy = null,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateMerge

    /// <summary>
    /// Creates a SQL Statement for 'Merge' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be merged.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for merge operation.</returns>
    public abstract string CreateMerge(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IEnumerable<Field>? qualifiers = null,
        string? hints = null);

    #endregion

    #region CreateMergeAll

    /// <summary>
    /// Creates a SQL Statement for 'MergeAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be merged.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for merge operation.</returns>
    public abstract string CreateMergeAll(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IEnumerable<Field>? qualifiers = null,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null);

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
    public abstract string CreateSkipQuery(string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        int skip,
        int take,
        IEnumerable<OrderField>? orderBy = null,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #endregion

    #region Helpers

    /// <summary>
    ///
    /// </summary>
    /// <param name="tableName"></param>
    /// <exception cref="ArgumentNullException"></exception>
    protected void GuardTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentNullException("The name of the table could be null.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="field"></param>
    /// <exception cref="InvalidOperationException"></exception>
    protected void GuardPrimary(DbField field)
    {
        if (field?.IsPrimary == false)
        {
            throw new InvalidOperationException("The field is not defined as primary.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="field"></param>
    /// <exception cref="InvalidOperationException"></exception>
    protected void GuardIdentity(DbField field)
    {
        if (field?.IsIdentity == false)
        {
            throw new InvalidOperationException("The field is not defined as identity.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="hints"></param>
    /// <exception cref="NotSupportedException"></exception>
    protected void GuardHints(string? hints = null)
    {
        if (!string.IsNullOrWhiteSpace(hints) && !DbSetting.AreTableHintsSupported)
        {
            throw new NotSupportedException("The table hints are not supported on this database provider statement builder.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="batchSize"></param>
    /// <exception cref="NotSupportedException"></exception>
    protected void ValidateMultipleStatementExecution(int batchSize = Constant.DefaultBatchOperationSize)
    {
        if (DbSetting.IsMultiStatementExecutable == false && batchSize > 1)
        {
            throw new NotSupportedException($"Multiple execution is not supported based on the current database setting '{DbSetting.GetType().FullName}'. Consider setting the batchSize to 1.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="primaryDbField"></param>
    /// <param name="identityDbField"></param>
    protected DbField GetReturnKeyColumnAsDbField(DbFieldCollection dbFields)
    {
        switch (GlobalConfiguration.Options.KeyColumnReturnBehavior)
        {
            case KeyColumnReturnBehavior.Primary:
                return dbFields.GetPrimary();
            case KeyColumnReturnBehavior.Identity:
                return dbFields.GetIdentity();
            case KeyColumnReturnBehavior.PrimaryOrElseIdentity:
                return dbFields.GetPrimary() ?? dbFields.GetIdentity();
            case KeyColumnReturnBehavior.IdentityOrElsePrimary:
                return dbFields.GetIdentity() ?? dbFields.GetPrimary();
            default:
                throw new InvalidOperationException(nameof(GlobalConfiguration.Options.KeyColumnReturnBehavior));
        }
    }

    #endregion
}
