﻿using System.Data;
using RepoDb.Contexts.Cachers;
using RepoDb.Contexts.Execution;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Requests;

namespace RepoDb.Contexts.Providers;

/// <summary>
/// 
/// </summary>
internal static class MergeExecutionContextProvider
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="tableName"></param>
    /// <param name="qualifiers"></param>
    /// <param name="fields"></param>
    /// <param name="hints"></param>
    /// <returns></returns>
    private static string GetKey(Type entityType,
        string tableName,
        IEnumerable<Field>? qualifiers,
        IEnumerable<Field> fields,
        string hints)
    {
        return string.Concat(entityType.FullName,
            ";",
            tableName,
            ";",
            qualifiers?.Select(f => f.Name).Join(","),
            ";",
            fields?.Select(f => f.Name).Join(","),
            ";",
            hints);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="qualifiers"></param>
    /// <param name="fields"></param>
    /// <param name="hints"></param>
    /// <param name="transaction"></param>
    /// <param name="statementBuilder"></param>
    /// <returns></returns>
    public static MergeExecutionContext Create(Type entityType,
        IDbConnection connection,
        string tableName,
        IEnumerable<Field>? qualifiers,
        IEnumerable<Field> fields,
        string? hints = null,
        IDbTransaction? transaction = null,
        IStatementBuilder? statementBuilder = null)
    {
        var key = GetKey(entityType, tableName, qualifiers, fields, hints);

        // Get from cache
        var context = MergeExecutionContextCache.Get(key);
        if (context != null)
        {
            return context;
        }

        // Create
        var dbFields = DbFieldCache.Get(connection, tableName, transaction);

        if (dbFields?.Any(x => x.IsGenerated) == true)
        {
            fields = fields.Where(f => dbFields.GetByName(f.Name)?.IsGenerated != true);
        }

        var request = new MergeRequest(tableName,
            connection,
            transaction,
            fields,
            qualifiers,
            hints,
            statementBuilder);
        var commandText = CommandTextCache.GetMergeText(request);

        // Call
        context = CreateInternal(entityType,
            connection,
            dbFields,
            tableName,
            fields,
            commandText);

        // Add to cache
        MergeExecutionContextCache.Add(key, context);

        // Return
        return context;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="qualifiers"></param>
    /// <param name="fields"></param>
    /// <param name="hints"></param>
    /// <param name="transaction"></param>
    /// <param name="statementBuilder"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task<MergeExecutionContext> CreateAsync(Type entityType,
        IDbConnection connection,
        string tableName,
        IEnumerable<Field> qualifiers,
        IEnumerable<Field> fields,
        string? hints = null,
        IDbTransaction? transaction = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(entityType, tableName, qualifiers, fields, hints);

        // Get from cache
        var context = MergeExecutionContextCache.Get(key);
        if (context != null)
        {
            return context;
        }

        // Create
        var dbFields = await DbFieldCache.GetAsync(connection, tableName, transaction, cancellationToken).ConfigureAwait(false);

        if (dbFields?.Any(x => x.IsGenerated) == true)
        {
            fields = fields.Where(f => dbFields.GetByName(f.Name)?.IsGenerated != true);
        }

        var request = new MergeRequest(tableName,
            connection,
            transaction,
            fields,
            qualifiers,
            hints,
            statementBuilder);
        var commandText = await CommandTextCache.GetMergeTextAsync(request, cancellationToken).ConfigureAwait(false);

        // Call
        context = CreateInternal(entityType,
            connection,
            dbFields,
            tableName,
            fields,
            commandText);

        // Add to cache
        MergeExecutionContextCache.Add(key, context);

        // Return
        return context;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="connection"></param>
    /// <param name="dbFields"></param>
    /// <param name="tableName"></param>
    /// <param name="fields"></param>
    /// <param name="commandText"></param>
    /// <returns></returns>
    private static MergeExecutionContext CreateInternal(Type entityType,
        IDbConnection connection,
        DbFieldCollection dbFields,
        string tableName,
        IEnumerable<Field> fields,
        string commandText)
    {
        var dbSetting = connection.GetDbSetting();
        var dbHelper = connection.GetDbHelper();
        var inputFields = dbFields
            .Where(dbField =>
                fields.FirstOrDefault(field =>
                    string.Equals(field.Name.AsUnquoted(true, dbSetting), dbField.Name, StringComparison.OrdinalIgnoreCase)) != null)
            .AsList();

        // Variables for the entity action
        Action<object, object> keyPropertySetterFunc = null;
        var keyField = ExecutionContextProvider
            .GetTargetReturnColumnAsField(entityType, dbFields);

        // Get the key setter
        if (keyField != null)
        {
            keyPropertySetterFunc = FunctionCache
                .GetDataEntityPropertySetterCompiledFunction(entityType, keyField);
        }

        // Return the value
        return new MergeExecutionContext
        {
            CommandText = commandText,
            InputFields = inputFields,
            ParametersSetterFunc = FunctionCache
                .GetDataEntityDbParameterSetterCompiledFunction(entityType,
                    string.Concat(entityType.FullName, ".", tableName, ".Merge"),
                    inputFields?.AsList(),
                    null,
                    dbSetting,
                    dbHelper),
            KeyPropertySetterFunc = keyPropertySetterFunc
        };
    }
}
