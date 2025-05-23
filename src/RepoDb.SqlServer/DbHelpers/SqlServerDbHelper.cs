using System.Data;
using System.Data.Common;
using RepoDb.DbSettings;
using RepoDb.Enumerations;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb.DbHelpers;

/// <summary>
/// A helper class for database specially for the direct access. This class is only meant for SQL Server.
/// </summary>
public sealed class SqlServerDbHelper : BaseDbHelper
{
    /// <summary>
    /// Creates a new instance of <see cref="SqlServerDbHelper"/> class.
    /// </summary>
    public SqlServerDbHelper()
        : this(new SqlServerDbTypeNameToClientTypeResolver())
    { }

    /// <summary>
    /// Creates a new instance of <see cref="SqlServerDbHelper"/> class.
    /// </summary>
    /// <param name="dbTypeResolver">The type resolver to be used.</param>
    public SqlServerDbHelper(IResolver<string, Type> dbTypeResolver)
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
                SELECT C.COLUMN_NAME AS ColumnName
                    , CONVERT(BIT, COALESCE(TC.is_primary, 0)) AS IsPrimary
                    , CONVERT(BIT, COALESCE(TMP.is_identity, 1)) AS IsIdentity
                    , CONVERT(BIT, COALESCE(TMP.is_nullable, 1)) AS IsNullable
                    , C.DATA_TYPE AS DataType
                    , COALESCE(C.CHARACTER_MAXIMUM_LENGTH, TMP.max_length)  AS Size
                    , CONVERT(TINYINT, COALESCE(TMP.precision, 1)) AS Precision
                    , CONVERT(TINYINT, COALESCE(TMP.scale, 1)) AS Scale
                    , CONVERT(BIT, IIF(C.COLUMN_DEFAULT IS NOT NULL, 1, 0)) AS DefaultValue
                    , CONVERT(BIT, COALESCE(TMP.is_computed, 0)) AS IsComputed
                FROM INFORMATION_SCHEMA.COLUMNS C
                OUTER APPLY
                (
                    SELECT 1 AS is_primary
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
                    LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
                        ON TC.TABLE_SCHEMA = C.TABLE_SCHEMA
                        AND TC.TABLE_NAME = C.TABLE_NAME
                        AND TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
                    WHERE KCU.TABLE_SCHEMA = C.TABLE_SCHEMA
                        AND KCU.TABLE_NAME = C.TABLE_NAME
                        AND KCU.COLUMN_NAME = C.COLUMN_NAME
                        AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) TC 
                OUTER APPLY
                (
                    SELECT SC.name
                        , SC.is_identity
                        , SC.is_nullable
                        ,  SC.max_length
                        , SC.scale
                        , SC.precision
                        , SC.is_computed
                    FROM [sys].[columns] SC
                    INNER JOIN [sys].[tables] ST ON ST.object_id = SC.object_id
                    INNER JOIN [sys].[schemas] S ON S.schema_id = ST.schema_id
                    WHERE SC.name = C.COLUMN_NAME
                        AND ST.name = C.TABLE_NAME
                        AND S.name = C.TABLE_SCHEMA
                ) TMP
                WHERE
                    C.TABLE_SCHEMA = @Schema
                    AND C.TABLE_NAME = @TableName;";
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    private DbField ReaderToDbField(DbDataReader reader)
    {
        return new DbField(reader.GetString(0),
            !reader.IsDBNull(1) && reader.GetBoolean(1),
            !reader.IsDBNull(2) && reader.GetBoolean(2),
            !reader.IsDBNull(3) && reader.GetBoolean(3),
            reader.IsDBNull(4) ? DbTypeResolver.Resolve("text") : DbTypeResolver.Resolve(reader.GetString(4)),
            reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
            reader.IsDBNull(6) ? (byte?)0 : reader.GetByte(6),
            reader.IsDBNull(7) ? (byte?)0 : reader.GetByte(7),
            reader.IsDBNull(7) ? "text" : reader.GetString(4),
            !reader.IsDBNull(8) && reader.GetBoolean(8),
            !reader.IsDBNull(9) && reader.GetBoolean(9),
            "MSSQL");
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
        return new DbField(await reader.GetFieldValueAsync<string>(0, cancellationToken),
            !await reader.IsDBNullAsync(1, cancellationToken) && await reader.GetFieldValueAsync<bool>(1, cancellationToken),
            !await reader.IsDBNullAsync(2, cancellationToken) && await reader.GetFieldValueAsync<bool>(2, cancellationToken),
            !await reader.IsDBNullAsync(3, cancellationToken) && await reader.GetFieldValueAsync<bool>(3, cancellationToken),
            await reader.IsDBNullAsync(4, cancellationToken) ? DbTypeResolver.Resolve("text") : DbTypeResolver.Resolve(await reader.GetFieldValueAsync<string>(4, cancellationToken)),
            await reader.IsDBNullAsync(5, cancellationToken) ? 0 : await reader.GetFieldValueAsync<int>(5, cancellationToken),
            await reader.IsDBNullAsync(6, cancellationToken) ? (byte?)0 : await reader.GetFieldValueAsync<byte>(6, cancellationToken),
            await reader.IsDBNullAsync(7, cancellationToken) ? (byte?)0 : await reader.GetFieldValueAsync<byte>(7, cancellationToken),
            await reader.IsDBNullAsync(7, cancellationToken) ? "text" : await reader.GetFieldValueAsync<string>(4, cancellationToken),
            !await reader.IsDBNullAsync(8, cancellationToken) && await reader.GetFieldValueAsync<bool>(8, cancellationToken),
            !await reader.IsDBNullAsync(9, cancellationToken) && await reader.GetFieldValueAsync<bool>(9, cancellationToken),
            "MSSQL");
    }

    #endregion

    #region Methods

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
    {
        // Variables
        var commandText = GetCommandText();
        var setting = connection.GetDbSetting();
        var param = new
        {
            Schema = DataEntityExtension.GetSchema(tableName, setting).AsUnquoted(setting),
            TableName = DataEntityExtension.GetTableName(tableName, setting).AsUnquoted(setting)
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
    public override async ValueTask<IEnumerable<DbField>> GetFieldsAsync(IDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // Variables
        var commandText = GetCommandText();
        var setting = connection.GetDbSetting();
        var param = new
        {
            Schema = DataEntityExtension.GetSchema(tableName, setting).AsUnquoted(setting),
            TableName = DataEntityExtension.GetTableName(tableName, setting).AsUnquoted(setting)
        };

        // Iterate and extract
        using var reader = (DbDataReader)await connection.ExecuteReaderAsync(commandText, param,
            transaction: transaction, cancellationToken: cancellationToken);

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
            o.type AS [Type],
            o.name AS [Name],
            s.name AS [Schema]
        FROM sys.objects o
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type IN ('U', 'V') AND is_ms_shipped = 0";

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
                "U" or "U " => DbSchemaType.Table,
                "V" or "V " => DbSchemaType.View,
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
    {
        return connection.ExecuteScalar<T>("SELECT COALESCE(SCOPE_IDENTITY(), @@IDENTITY);",
            transaction: transaction);
    }

    /// <summary>
    /// Gets the newly generated identity from the database in an asynchronous way.
    /// </summary>
    /// <typeparam name="T">The type of newly generated identity.</typeparam>
    /// <param name="connection">The instance of the connection object.</param>
    /// <param name="transaction">The transaction object that is currently in used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The newly generated identity from the database.</returns>
    public override async ValueTask<T> GetScopeIdentityAsync<T>(IDbConnection connection,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        return await connection.ExecuteScalarAsync<T>("SELECT COALESCE(SCOPE_IDENTITY(), @@IDENTITY);",
            transaction: transaction,
            cancellationToken: cancellationToken);
    }

    #endregion

    #endregion
}
