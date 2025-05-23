using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Npgsql;
using NpgsqlTypes;
using RepoDb.DbSettings;
using RepoDb.Enumerations;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb.DbHelpers;

/// <summary>
/// A helper class for database specially for the direct access. This class is only meant for PostgreSql.
/// </summary>
public sealed class PostgreSqlDbHelper : BaseDbHelper
{
    private readonly IDbSetting m_dbSetting = DbSettingMapper.Get<NpgsqlConnection>();

    /// <summary>
    /// Creates a new instance of <see cref="PostgreSqlDbHelper"/> class.
    /// </summary>
    public PostgreSqlDbHelper()
        : this(new PostgreSqlDbTypeNameToClientTypeResolver())
    { }

    /// <summary>
    /// Creates a new instance of <see cref="PostgreSqlDbHelper"/> class.
    /// </summary>
    /// <param name="dbTypeResolver">The type resolver to be used.</param>
    public PostgreSqlDbHelper(IResolver<string, Type> dbTypeResolver)
        : base(dbTypeResolver)
    {
    }

    #region Helpers

    /// <summary>
    ///
    /// </summary>
    /// <returns></returns>
    private string GetCommandText()
    {
        return @"
                SELECT C.column_name,
                       CAST((CASE WHEN C.column_name = TMP.column_name THEN 1 ELSE 0 END) AS BOOLEAN) AS IsPrimary,
                       CASE WHEN C.is_identity = 'YES' OR POSITION('NEXTVAL' IN UPPER(C.column_default)) >= 1 THEN
                            true
                            ELSE
                            false
                            END AS IsIdentity,
                       CAST(C.is_nullable AS BOOLEAN) AS IsNullable,
                       C.data_type AS DataType,
                       C.character_maximum_length AS Size,
                       CASE WHEN C.column_default IS NOT NULL THEN
                            true
                            ELSE
                            false
                            END AS HasDefaultValue,
                       CAST((CASE WHEN C.is_generated = 'ALWAYS' THEN 1 ELSE 0 END) AS BOOLEAN) AS IsComputed
                FROM information_schema.columns C
                LEFT JOIN (
                    SELECT C.table_schema,
                           C.table_name,
                           C.column_name,
                           C.column_default
                    FROM information_schema.table_constraints TC
                    JOIN information_schema.constraint_column_usage AS CCU USING (constraint_schema, constraint_name)
                    JOIN information_schema.columns AS C ON C.table_schema = TC.constraint_schema
                                                          AND TC.table_name = C.table_name
                                                          AND CCU.column_name = C.column_name
                    WHERE TC.constraint_type = 'PRIMARY KEY'
                ) TMP ON TMP.table_schema = C.table_schema
                       AND TMP.table_name = C.table_name
                       AND TMP.column_name = C.column_name
                WHERE C.table_name = @TableName
                AND C.table_schema = @Schema;";
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    private DbField ReaderToDbField(DbDataReader reader)
    {
        var dbType = reader.IsDBNull(4) ? "text" : reader.GetString(4);

        return new DbField(reader.GetString(0),
            !reader.IsDBNull(1) && reader.GetBoolean(1),
            !reader.IsDBNull(2) && reader.GetBoolean(2),
            !reader.IsDBNull(3) && reader.GetBoolean(3),
            DbTypeResolver.Resolve(dbType),
            reader.IsDBNull(5) ? null : reader.GetInt32(5),
            null,
            null,
            dbType,
            !reader.IsDBNull(6) && reader.GetBoolean(6),
            !reader.IsDBNull(7) && reader.GetBoolean(7),
            "PGSQL");
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<DbField> ReaderToDbFieldAsync(DbDataReader reader,
        CancellationToken cancellationToken = default)
    {
        var dbType = await reader.IsDBNullAsync(4, cancellationToken) ? "text" : await reader.GetFieldValueAsync<string>(4, cancellationToken);

        return new DbField(await reader.GetFieldValueAsync<string>(0, cancellationToken),
            !await reader.IsDBNullAsync(1, cancellationToken) && await reader.GetFieldValueAsync<bool>(1, cancellationToken),
            !await reader.IsDBNullAsync(2, cancellationToken) && await reader.GetFieldValueAsync<bool>(2, cancellationToken),
            !await reader.IsDBNullAsync(3, cancellationToken) && await reader.GetFieldValueAsync<bool>(3, cancellationToken),
            DbTypeResolver.Resolve(dbType),
            await reader.IsDBNullAsync(5) ? null : await reader.GetFieldValueAsync<int>(5, cancellationToken),
            null,
            null,
            dbType,
            !await reader.IsDBNullAsync(6, cancellationToken) && await reader.GetFieldValueAsync<bool>(6, cancellationToken),
            !await reader.IsDBNullAsync(7, cancellationToken) && await reader.GetFieldValueAsync<bool>(7, cancellationToken),
            "PGSQL");
    }

    #endregion

    #region Methods

    private TResult TryExecuteOnExistingConnection<TResult>(IDbConnection connection, Func<IDbConnection, TResult> func)
    {
        try
        {
            return func(connection);
        }
        catch (NpgsqlOperationInProgressException)
        {
            Debug.WriteLine("NpgsqlOperationInProgressException occurred. Retrying the operation on a new connection.");
            using var newConnection = new NpgsqlConnection(connection.ConnectionString);
            newConnection.Open();
            return func(newConnection);
        }
    }

    private async ValueTask<TResult> TryExecuteOnExistingConnectionAsync<TResult>(IDbConnection connection, Func<IDbConnection, Task<TResult>> func)
    {
        try
        {
            return await func(connection);
        }
        catch (NpgsqlOperationInProgressException)
        {
            Debug.WriteLine("NpgsqlOperationInProgressException occurred. Retrying the operation on a new connection.");
            using var newConnection = new NpgsqlConnection(connection.ConnectionString);
            await newConnection.OpenAsync();
            return await func(newConnection);
        }
    }

    #region GetFields

    /// <summary>
    /// Gets the list of <see cref="DbField"/> of the table.
    /// </summary>
    /// <param name="connection">The instance of the connection object.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="transaction">The transaction object that is currently in used.</param>
    /// <returns>A list of <see cref="DbField"/> of the target table.</returns>
    public override IEnumerable<DbField> GetFields(IDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null)

     => TryExecuteOnExistingConnection(connection, c => GetFieldsInternal(c, tableName, transaction));

    private IEnumerable<DbField> GetFieldsInternal(IDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null)
    {
        // Variables
        var commandText = GetCommandText();
        var param = new
        {
            Schema = DataEntityExtension.GetSchema(tableName, m_dbSetting).AsUnquoted(m_dbSetting),
            TableName = DataEntityExtension.GetTableName(tableName, m_dbSetting).AsUnquoted(m_dbSetting)
        };

        // Iterate and extract
        using var reader = (DbDataReader)connection.ExecuteReader(commandText, param, transaction: transaction);

        var dbFields = new List<DbField>();

        // Iterate the list of the fields
        while (reader.Read())
        {
            dbFields.Add(ReaderToDbField(reader));
        }

        // Return the list of fields
        return dbFields;
    }

    /// <summary>
    /// Gets the list of <see cref="DbField"/> of the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The instance of the connection object.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="transaction">The transaction object that is currently in used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A list of <see cref="DbField"/> of the target table.</returns>
    public override ValueTask<IEnumerable<DbField>> GetFieldsAsync(IDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)

     => TryExecuteOnExistingConnectionAsync(connection, c => GetFieldsAsyncInternal(c, tableName, transaction, cancellationToken));

    private async Task<IEnumerable<DbField>> GetFieldsAsyncInternal(IDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // Variables
        var commandText = GetCommandText();
        var param = new
        {
            Schema = DataEntityExtension.GetSchema(tableName, m_dbSetting).AsUnquoted(m_dbSetting),
            TableName = DataEntityExtension.GetTableName(tableName, m_dbSetting).AsUnquoted(m_dbSetting)
        };

        // Iterate and extract
        using var reader = (DbDataReader)await connection.ExecuteReaderAsync(commandText, param, transaction: transaction,
            cancellationToken: cancellationToken);

        var dbFields = new List<DbField>();

        // Iterate the list of the fields
        while (await reader.ReadAsync(cancellationToken))
        {
            dbFields.Add(await ReaderToDbFieldAsync(reader, cancellationToken));
        }

        // Return the list of fields
        return dbFields;
    }

    #endregion

    #region GetSchemaObjects
    const string GetSchemaQuery = @"
        SELECT
            table_type AS Type,
            table_name AS Name,
            table_schema AS Schema
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')";

    public override IEnumerable<DbSchemaObject> GetSchemaObjects(IDbConnection connection, IDbTransaction? transaction = null)
    {
        return connection.ExecuteQuery<(string Type, string Name, string Schema)>(GetSchemaQuery, transaction)
                         .Select(MapSchemaQueryResult);
    }

    public override async ValueTask<IEnumerable<DbSchemaObject>> GetSchemaObjectsAsync(IDbConnection connection, IDbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        var results = await connection.ExecuteQueryAsync<(string Type, string Name, string Schema)>(GetSchemaQuery, transaction);
        return results.Select(MapSchemaQueryResult);
    }

    private static DbSchemaObject MapSchemaQueryResult((string Type, string Name, string Schema) r) =>
        new DbSchemaObject
        {
            Type = r.Type switch
            {
                "BASE TABLE" => DbSchemaType.Table,
                "VIEW" => DbSchemaType.View,
                _ => throw new NotSupportedException($"Unsupported schema object type: {r.Type}")
            },
            Name = r.Name,
            Schema = r.Schema
        };
    #endregion

    #region GetScopeIdentity

    /// <summary>
    /// Gets the newly generated identity from the database.
    /// </summary>
    /// <typeparam name="T">The type of newly generated identity.</typeparam>
    /// <param name="connection">The instance of the connection object.</param>
    /// <param name="transaction">The transaction object that is currently in used.</param>
    /// <returns>The newly generated identity from the database.</returns>
    public override T GetScopeIdentity<T>(IDbConnection connection,
        IDbTransaction? transaction = null)

     => TryExecuteOnExistingConnection(connection, c => GetScopeIdentityInternal<T>(c, transaction));

    private T GetScopeIdentityInternal<T>(IDbConnection connection,
        IDbTransaction? transaction = null)
    {
        // TODO: May fail with trigger?
        return connection.ExecuteScalar<T>("SELECT lastval();", transaction: transaction);
    }

    /// <summary>
    /// Gets the newly generated identity from the database in an asynchronous way.
    /// </summary>
    /// <typeparam name="T">The type of newly generated identity.</typeparam>
    /// <param name="connection">The instance of the connection object.</param>
    /// <param name="transaction">The transaction object that is currently in used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The newly generated identity from the database.</returns>
    public override ValueTask<T> GetScopeIdentityAsync<T>(IDbConnection connection,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)

     => TryExecuteOnExistingConnectionAsync(connection, c => GetScopeIdentityAsyncInternal<T>(c, transaction, cancellationToken));

    private Task<T> GetScopeIdentityAsyncInternal<T>(IDbConnection connection,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // TODO: May fail with trigger?
        return connection.ExecuteScalarAsync<T>("SELECT lastval();", transaction: transaction,
            cancellationToken: cancellationToken);
    }

    #endregion

    #region DynamicHandler

    /// <summary>
    /// A backdoor access from the core library used to handle an instance of an object to whatever purpose within the extended library.
    /// </summary>
    /// <typeparam name="TEventInstance">The type of the event instance to handle.</typeparam>
    /// <param name="instance">The instance of the event object to handle.</param>
    /// <param name="key">The key of the event to handle.</param>
    public override void DynamicHandler<TEventInstance>(TEventInstance instance,
        string key)
    {
        if (key == "RepoDb.Internal.Compiler.Events[AfterCreateDbParameter]")
        {
            HandleDbParameterPostCreation(instance as NpgsqlParameter);
        }
    }

    #region Handlers

    /// <summary>
    ///
    /// </summary>
    /// <param name="parameter"></param>
    private void HandleDbParameterPostCreation(NpgsqlParameter parameter)
    {
        if (parameter?.Value is Enum)
        {
            parameter.NpgsqlDbType = NpgsqlDbType.Unknown;
        }
    }

    #endregion

    #endregion

    #endregion
}
