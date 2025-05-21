﻿using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using RepoDb.Enumerations;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb.DbHelpers;

/// <summary>
/// A helper class for database specially for the direct access. This class is only meant for SqLite.
/// </summary>
public sealed partial class SqLiteDbHelper : IDbHelper
{
    private const string doubleQuote = "\"";

    /// <summary>
    /// Creates a new instance of <see cref="SqLiteDbHelper"/> class.
    /// </summary>
    /// <param name="dbTypeResolver">The type resolver to be used.</param>
    /// <param name="dbSetting">The instance of the <see cref="IDbSetting"/> object to be used.</param>
    public SqLiteDbHelper(IDbSetting dbSetting,
        IResolver<string, Type> dbTypeResolver)
    {
        DbSetting = dbSetting;
        DbTypeResolver = dbTypeResolver;
    }

    #region Properties

    /// <summary>
    /// Gets the database setting used by this <see cref="SqLiteDbHelper"/> instance.
    /// </summary>
    public IDbSetting DbSetting { get; }

    /// <summary>
    /// Gets the type resolver used by this <see cref="SqLiteDbHelper"/> instance.
    /// </summary>
    public IResolver<string, Type> DbTypeResolver { get; }

    #endregion

    #region Helpers

    /// <summary>
    ///
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    private string GetCommandText(string tableName) =>
        $"pragma table_xinfo({DataEntityExtension.GetTableName(tableName, DbSetting).AsUnquoted(DbSetting)});";

    /// <summary>
    ///
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="identityFieldName"></param>
    /// <returns></returns>
    private DbField ReaderToDbField(DbDataReader reader,
        string identityFieldName)
    {
        var dbType = SplitDbType(reader.IsDBNull(2) ? null : reader.GetString(2), out var size);

        return new DbField(reader.GetString(1),
            !reader.IsDBNull(5) && reader.GetBoolean(5),
            string.Equals(reader.GetString(1), identityFieldName, StringComparison.OrdinalIgnoreCase),
            reader.IsDBNull(3) || reader.GetBoolean(3) == false,
            DbTypeResolver.Resolve(dbType ?? "text"),
            size,
            null,
            null,
            dbType,
            !reader.IsDBNull(4),
            reader.GetInt32(reader.FieldCount - 1) is 2 /* dynamic generated */ or 3 /* stored generated */,
            "MSSQLITE");
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="identityFieldName"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<DbField> ReaderToDbFieldAsync(DbDataReader reader,
        string identityFieldName,
        CancellationToken cancellationToken = default)
    {
        var rawDbType = await reader.IsDBNullAsync(2, cancellationToken) ? null : await reader.GetFieldValueAsync<string>(2, cancellationToken);
        var dbType = SplitDbType(rawDbType, out var size);

        return new DbField(await reader.GetFieldValueAsync<string>(1, cancellationToken),
            !await reader.IsDBNullAsync(5, cancellationToken) && Convert.ToBoolean(await reader.GetFieldValueAsync<long>(5, cancellationToken)),
            string.Equals(await reader.GetFieldValueAsync<string>(1, cancellationToken), identityFieldName, StringComparison.OrdinalIgnoreCase),
            await reader.IsDBNullAsync(3, cancellationToken) || Convert.ToBoolean(await reader.GetFieldValueAsync<long>(3, cancellationToken)) == false,
            DbTypeResolver.Resolve(dbType ?? "text"),
            size,
            null,
            null,
            dbType,
            !await reader.IsDBNullAsync(4, cancellationToken),
            await reader.GetFieldValueAsync<long>(reader.FieldCount - 1, cancellationToken) is 2 /* dynamic generated */ or 3 /* stored generated */,
            "MSSQLITE");
    }

#if NET
    [GeneratedRegex(@"\((\d+)(,(\d+))*\)$")]
    private static partial Regex FieldTypeRegex();
#else
    static readonly Regex re = new Regex(@"\((\d+)(,(\d+))*\)$", RegexOptions.Compiled);
    private static Regex FieldTypeRegex() => re;
#endif

    private string? SplitDbType(string v, out int? size)
    {
        size = null;

        if (FieldTypeRegex().Match(v) is { } r && r.Success)
        {
            // Get the size
            if (int.TryParse(r.Groups[1].Value, out var s))
            {
                size = s;
            }
            v = v.Substring(0, r.Index);
        }

        return v;
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TDbConnection"></typeparam>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="transaction"></param>
    /// <returns></returns>
    private string GetIdentityFieldName<TDbConnection>(TDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null)
        where TDbConnection : IDbConnection
    {
        // Sql text
        var commandText = "SELECT sql FROM [sqlite_master] WHERE name = @TableName AND type = 'table';";
        var tableDefinition = connection.ExecuteScalar<string>(commandText: commandText,
            param: new { TableName = DataEntityExtension.GetTableName(tableName, DbSetting).AsUnquoted(DbSetting) },
            transaction: transaction);

        // Return
        return GetIdentityFieldNameInternal(tableDefinition)?
            .AsUnquoted(connection.GetDbSetting())?
            .Replace(doubleQuote, string.Empty);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TDbConnection"></typeparam>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="transaction"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<string> GetIdentityFieldNameAsync<TDbConnection>(TDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TDbConnection : IDbConnection
    {
        // Sql text
        var commandText = "SELECT sql FROM [sqlite_master] WHERE name = @TableName AND type = 'table';";
        var tableDefinition = await connection.ExecuteScalarAsync<string>(commandText: commandText,
            param: new { TableName = DataEntityExtension.GetTableName(tableName, DbSetting).AsUnquoted(DbSetting) },
            transaction: transaction,
            cancellationToken: cancellationToken);

        // Return
        return GetIdentityFieldNameInternal(tableDefinition)?
            .AsUnquoted(connection.GetDbSetting())?
            .Replace(doubleQuote, string.Empty);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="sql"></param>
    /// <returns></returns>
    private string GetIdentityFieldNameInternal(string sql)
    {
        // Get fieldname
        var identityField = TokenizeSchema(sql.AsMemory()).FirstOrDefault(def => IsIdentity(def.Definition));

        if (identityField.FieldName?.Equals("PRIMARY", StringComparison.OrdinalIgnoreCase) == true)
        {
            // Issue #802
            //
            // CREATE TABLE "Articles" (
            //  "ID" INTEGER NOT NULL UNIQUE,
            //  "ArticleID" TEXT,
            //  "Title" TEXT NOT NULL,
            //  "Description" TEXT,
            //  "Date_Added" INTEGER NOT NULL,
            //  "Date_Fetched" INTEGER,
            //  PRIMARY KEY("ID" AUTOINCREMENT)
            //  )

            var def = identityField.FieldName + " " + identityField.Definition;

            def = def.Replace("PRIMARY KEY", string.Empty)
                    .Trim()
                    .Replace("(", string.Empty);

            return def.Substring(0, def.IndexOf(' ')).Replace("\"", "");
        }
        else
            return identityField.FieldName; // May be null as valuetuple is never null
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="field"></param>
    /// <returns></returns>
    private bool IsIdentity(string field)
    {
        return field.Contains("AUTOINCREMENT", StringComparison.OrdinalIgnoreCase) ||
               (field.Contains("INTEGER", StringComparison.OrdinalIgnoreCase)
                && field.Contains("PRIMARY KEY", StringComparison.OrdinalIgnoreCase));
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
    public IEnumerable<DbField> GetFields(IDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null)
    {
        // Variables
        var commandText = GetCommandText(tableName);

        // Iterate and extract
        using var reader = (DbDataReader)connection.ExecuteReader(commandText, transaction: transaction);

        var dbFields = new List<DbField>();
        var identity = GetIdentityFieldName(connection, tableName, transaction);

        // Iterate the list of the fields
        while (reader.Read())
        {
            dbFields.Add(ReaderToDbField(reader, identity));
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
    public async Task<IEnumerable<DbField>> GetFieldsAsync(IDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // Variables
        var commandText = GetCommandText(tableName);

        // Iterate and extract
        using var reader = (DbDataReader)await connection.ExecuteReaderAsync(commandText, transaction: transaction, cancellationToken: cancellationToken);

        var dbFields = new List<DbField>();
        var identity = await GetIdentityFieldNameAsync(connection, tableName, transaction, cancellationToken);

        // Iterate the list of the fields
        while (await reader.ReadAsync(cancellationToken))
        {
            dbFields.Add(await ReaderToDbFieldAsync(reader, identity, cancellationToken));
        }

        // Return the list of fields
        return dbFields;
    }

    #endregion

    #region GetScopeIdentity

    /// <summary>
    /// Gets the newly generated identity from the database.
    /// </summary>
    /// <typeparam name="T">The type of newly generated identity.</typeparam>
    /// <param name="connection">The instance of the connection object.</param>
    /// <param name="transaction">The transaction object that is currently in used.</param>
    /// <returns>The newly generated identity from the database.</returns>
    public T GetScopeIdentity<T>(IDbConnection connection,
        IDbTransaction? transaction = null)
    {
        return connection.ExecuteScalar<T>("SELECT last_insert_rowid();", transaction: transaction);
    }

    /// <summary>
    /// Gets the newly generated identity from the database in an asynchronous way.
    /// </summary>
    /// <typeparam name="T">The type of newly generated identity.</typeparam>
    /// <param name="connection">The instance of the connection object.</param>
    /// <param name="transaction">The transaction object that is currently in used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The newly generated identity from the database.</returns>
    public Task<T> GetScopeIdentityAsync<T>(IDbConnection connection,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        return connection.ExecuteScalarAsync<T>("SELECT last_insert_rowid();", transaction: transaction,
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
    public void DynamicHandler<TEventInstance>(TEventInstance instance,
        string key)
    {
        if (key == "RepoDb.Internal.Compiler.Events[AfterCreateDbParameter]")
        {
            HandleDbParameterPostCreation((SqliteParameter)(object)instance);
        }
    }

    #region Handlers

    /// <summary>
    /// 
    /// </summary>
    /// <param name="parameter"></param>
    private void HandleDbParameterPostCreation(SqliteParameter parameter)
    {
        // Do nothing for now
    }

    #endregion

    #endregion

    #endregion

    #region Table Definition Parser

    static IEnumerable<(string FieldName, string Definition)> TokenizeSchema(ReadOnlyMemory<char> schema)
    {
        {
            int start = schema.Span.IndexOf('(');
            if (start < 0)
            {
                yield break; // No valid schema content to process
            }

            schema = schema.Slice(start + 1);
        }

        int depth = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        int lastSplit = 0;

        for (int i = 0; i < schema.Length; i++)
        {
            var span = schema.Span;
            char c = span[i];

            // Handle quotes
            if (c == '\'' && !inDoubleQuote)
                inSingleQuote = !inSingleQuote;
            else if (c == '\"' && !inSingleQuote)
                inDoubleQuote = !inDoubleQuote;

            // Handle parentheses and brackets
            if ((c == '(' || c == '[') && !inSingleQuote && !inDoubleQuote)
                depth++;
            else if ((c == ')' || c == ']') && !inSingleQuote && !inDoubleQuote)
                depth--;

            // Check for closing parenthesis of the initial group
            if (depth == -1)
            {
                if (c == ')')
                {
                    var field = span.Slice(lastSplit, i - lastSplit);
                    yield return ParseField(field);
                }
                yield break; // End processing when the matching closing parenthesis is found
            }

            // Split on commas outside nested structures and quotes
            if (c == ',' && depth == 0 && !inSingleQuote && !inDoubleQuote)
            {
                var field = span.Slice(lastSplit, i - lastSplit);
                yield return ParseField(field);
                lastSplit = i + 1;
            }
        }
    }

    static (string FieldName, string Definition) ParseField(ReadOnlySpan<char> field)
    {
        // Trim the span directly to avoid extra allocations
        field = field.Trim();

        // Handle escaped field names (quoted with " or [ ])
        bool isQuoted = field.Length > 0 && field[0] is '\'' or '[';
        int nameEnd;

        if (isQuoted)
        {
            char closingChar = field[0] == '"' ? '"' : ']';
            nameEnd = field.Slice(1).IndexOf(closingChar); // Find the closing quote/bracket
            if (nameEnd != -1)
            {
                nameEnd += 1; // Adjust for the opening quote/bracket
            }
        }
        else
        {
            // Find the first space outside of quotes
            nameEnd = field.IndexOf(' ');
        }

        if (nameEnd == -1)
        {
            // If no valid separator is found, the entire field is the name with no definition
            return (field.ToString(), string.Empty);
        }

        ReadOnlySpan<char> fieldNameSpan;
        if (isQuoted)
        {
            // Remove enclosing quotes/brackets by slicing
            fieldNameSpan = field.Slice(1, nameEnd - 1).Trim(); // Skip opening and closing quotes/brackets
        }
        else
        {
            fieldNameSpan = field.Slice(0, nameEnd).Trim();
        }

        var fieldName = fieldNameSpan.ToString();
        var definition = field.Slice(nameEnd + 1).Trim().ToString();

        return (fieldName, definition);
    }

    const string GetSchemaQuery = "SELECT type, name FROM sqlite_master WHERE (type = 'table' OR type = 'view')";

    public IEnumerable<DbSchemaObject> GetSchemaObjects(IDbConnection connection, IDbTransaction? transaction = null)
    {
        // Using tuple helper as that doesn't call the helper to fetch columns
        return connection.ExecuteQuery<(string Type, string Name)>(GetSchemaQuery).Select(MapSchemaQueryResult);
    }

    public async Task<IEnumerable<DbSchemaObject>> GetSchemaObjectsAsync(IDbConnection connection, IDbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        // Using tuple helper as that doesn't call the helper to fetch columns
        return (await connection.ExecuteQueryAsync<(string Type, string Name)>(GetSchemaQuery)).Select(MapSchemaQueryResult);
    }

    private static DbSchemaObject MapSchemaQueryResult((string Type, string Name) r) =>
    new DbSchemaObject()
    {
        Type = r.Type switch
        {
            "table" => DbSchemaType.Table,
            "view" => DbSchemaType.View,
            _ => throw new NotSupportedException($"Unsupported schema object type: {r.Type}")
        },
        Name = r.Name,
        Schema = null,
    };

    #endregion
}
