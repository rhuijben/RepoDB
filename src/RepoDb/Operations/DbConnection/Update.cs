﻿#nullable enable
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using RepoDb.Contexts.Providers;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb;

/// <summary>
/// Contains the extension methods for <see cref="IDbConnection"/> object.
/// </summary>
public static partial class DbConnectionExtension
{
    #region Update<TEntity>

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be used for update.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        var key = GetAndGuardPrimaryKeyOrIdentityKey(connection, tableName, transaction,
            GetEntityType<TEntity>(entity));
        return UpdateInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(key, entity),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TWhat">The type of the expression or the key value.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity, TWhat>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        TWhat what,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
        where TWhat : notnull
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: WhatToQueryGroup(connection, tableName, what, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        object what,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: WhatToQueryGroup(connection, tableName, what, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        Expression<Func<TEntity, bool>> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: connection.ToQueryGroup(where, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        QueryField where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        IEnumerable<QueryField> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        QueryGroup where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be used for update.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        TEntity entity,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        var key = GetAndGuardPrimaryKeyOrIdentityKey(GetEntityType<TEntity>(entity), connection, transaction);
        return UpdateInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: ToQueryGroup<TEntity>(key, entity),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TWhat">The type of the expression or the key value.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity, TWhat>(this IDbConnection connection,
        TEntity entity,
        TWhat what,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
        where TWhat : notnull
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: WhatToQueryGroup(GetEntityType<TEntity>(entity), connection, what, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        TEntity entity,
        object what,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: WhatToQueryGroup(GetEntityType<TEntity>(entity), connection, what, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        TEntity entity,
        Expression<Func<TEntity, bool>> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: connection.ToQueryGroup(where, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        TEntity entity,
        QueryField where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: where != null ? new QueryGroup(where.AsEnumerable()) : null,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        TEntity entity,
        IEnumerable<QueryField> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update<TEntity>(this IDbConnection connection,
        TEntity entity,
        QueryGroup where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return UpdateInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    internal static int UpdateInternal<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        QueryGroup? where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        if (TypeCache.Get(GetEntityType(entity)).IsDictionaryStringObject() == true)
        {
            return UpdateInternalBase<IDictionary<string, object>>(connection: connection,
                tableName: tableName,
                entity: (IDictionary<string, object>)entity,
                where: where,
                fields: fields ?? GetQualifiedFields<TEntity>(entity),
                hints: hints,
                commandTimeout: commandTimeout,
            traceKey: traceKey,
                transaction: transaction,
                trace: trace,
                statementBuilder: statementBuilder);
        }
        else
        {
            return UpdateInternalBase<TEntity>(connection: connection,
                tableName: tableName,
                entity: entity,
                where: where,
                fields: fields ?? GetQualifiedFields<TEntity>(entity),
                hints: hints,
                commandTimeout: commandTimeout,
            traceKey: traceKey,
                transaction: transaction,
                trace: trace,
                statementBuilder: statementBuilder);
        }
    }

    #endregion

    #region UpdateAsync<TEntity>

    /// <summary>
    /// Updates an existing row in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        var key = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(connection, tableName, transaction,
            GetEntityType(entity), cancellationToken).ConfigureAwait(false);
        return await UpdateAsyncInternal(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(key, entity),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TWhat">The type of the expression or the key value.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity, TWhat>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        TWhat what,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
        where TWhat : notnull
    {
        return await UpdateAsyncInternal(connection: connection,
            tableName: tableName,
            entity: entity,
            where: await WhatToQueryGroupAsync(connection, tableName, what, transaction, cancellationToken).ConfigureAwait(false),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        object what,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal(connection: connection,
            tableName: tableName,
            entity: entity,
            where: await WhatToQueryGroupAsync(connection, tableName, what, transaction, cancellationToken).ConfigureAwait(false),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        Expression<Func<TEntity, bool>> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: connection.ToQueryGroup(where, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        QueryField where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: where != null ? new QueryGroup(where.AsEnumerable()) : null,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        IEnumerable<QueryField> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        QueryGroup where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal<TEntity>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        TEntity entity,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        var key = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(GetEntityType(entity), connection, transaction, cancellationToken).ConfigureAwait(false);
        return await UpdateAsyncInternal(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: ToQueryGroup(key, entity),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TWhat">The type of the expression or the key value.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity, TWhat>(this IDbConnection connection,
        TEntity entity,
        TWhat what,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
        where TWhat : notnull
    {
        return await UpdateAsyncInternal(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: await WhatToQueryGroupAsync(GetEntityType(entity), connection, what, transaction, cancellationToken).ConfigureAwait(false),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        TEntity entity,
        object what,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: await WhatToQueryGroupAsync(GetEntityType(entity), connection, what, transaction, cancellationToken).ConfigureAwait(false),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        TEntity entity,
        Expression<Func<TEntity, bool>> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: connection.ToQueryGroup(where, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        TEntity entity,
        QueryField where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table based on the given query expression in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        TEntity entity,
        IEnumerable<QueryField> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync<TEntity>(this IDbConnection connection,
        TEntity entity,
        QueryGroup where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await UpdateAsyncInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entity),
            entity: entity,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    internal static ValueTask<int> UpdateAsyncInternal<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        QueryGroup? where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        if (TypeCache.Get(GetEntityType(entity)).IsDictionaryStringObject() == true)
        {
            return UpdateAsyncInternalBase<IDictionary<string, object>>(connection: connection,
                tableName: tableName,
                entity: (IDictionary<string, object>)entity,
                where: where,
                fields: fields ?? GetQualifiedFields<TEntity>(entity),
                hints: hints,
                commandTimeout: commandTimeout,
            traceKey: traceKey,
                transaction: transaction,
                trace: trace,
                statementBuilder: statementBuilder,
                cancellationToken: cancellationToken);
        }
        else
        {
            return UpdateAsyncInternalBase<TEntity>(connection: connection,
                tableName: tableName,
                entity: entity,
                where: where,
                fields: fields ?? GetQualifiedFields<TEntity>(entity),
                hints: hints,
                commandTimeout: commandTimeout,
            traceKey: traceKey,
                transaction: transaction,
                trace: trace,
                statementBuilder: statementBuilder,
                cancellationToken: cancellationToken);
        }
    }

    #endregion

    #region Update(TableName)

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update(this IDbConnection connection,
        string tableName,
        object entity,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        var key = GetAndGuardPrimaryKeyOrIdentityKey(connection, tableName, transaction, entity.GetType());
        return UpdateInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(key, entity),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            trace: trace,
            statementBuilder: statementBuilder,
            transaction: transaction);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update(this IDbConnection connection,
        string tableName,
        object entity,
        object where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        return UpdateInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            trace: trace,
            statementBuilder: statementBuilder,
            transaction: transaction);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update(this IDbConnection connection,
        string tableName,
        object entity,
        QueryField where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        return UpdateInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update(this IDbConnection connection,
        string tableName,
        object entity,
        IEnumerable<QueryField> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        return UpdateInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static int Update(this IDbConnection connection,
        string tableName,
        object entity,
        QueryGroup where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        return UpdateInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    #endregion

    #region UpdateAsync(TableName)

    /// <summary>
    /// Updates an existing row in the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync(this IDbConnection connection,
        string tableName,
        object entity,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        var key = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(connection, tableName, transaction,
            entity.GetType(), cancellationToken).ConfigureAwait(false);
        return await UpdateAsyncInternal(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(key, entity),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates an existing row in the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync(this IDbConnection connection,
        string tableName,
        object entity,
        object where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        return await UpdateAsyncInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync(this IDbConnection connection,
        string tableName,
        object entity,
        QueryField where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        return await UpdateAsyncInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync(this IDbConnection connection,
        string tableName,
        object entity,
        IEnumerable<QueryField> where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        return await UpdateAsyncInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Updates an existing row in the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The dynamic object to be used for update.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    public static async Task<int> UpdateAsync(this IDbConnection connection,
        string tableName,
        object entity,
        QueryGroup where,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        return await UpdateAsyncInternal<object>(connection: connection,
            tableName: tableName,
            entity: entity,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    #endregion

    #region UpdateInternalBase<TEntity>

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <typeparam name="TEntity">The type of the object (whether a data entity or a dynamic).</typeparam>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity or dynamic object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    internal static int UpdateInternalBase<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        QueryGroup? where,
        IEnumerable<Field> fields,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        // Set the flags
        where?.IsForUpdate();

        // Get the context
        var entityType = GetEntityType<TEntity>(entity);
        var context = UpdateExecutionContextProvider.Create(entityType,
            connection,
            tableName,
            where,
            fields,
            hints,
            transaction,
            statementBuilder);
        var result = 0;

        // Create the command
        using (var command = (DbCommand)connection.EnsureOpen().CreateCommand(context.CommandText,
            CommandType.Text, commandTimeout, transaction))
        {
            // Set the values
            context.ParametersSetterFunc(command, entity);

            // Add the fields from the query group
            WhereToCommandParameters(command, where, entity.GetType(),
                DbFieldCache.Get(connection, tableName, transaction));

            // Before Execution
            var traceResult = Tracer
                .InvokeBeforeExecution(traceKey, trace, command);

            // Silent cancellation
            if (traceResult?.CancellableTraceLog?.IsCancelled == true)
            {
                return result;
            }

            // Actual Execution
            result = command.ExecuteNonQuery();

            // After Execution
            Tracer
                .InvokeAfterExecution(traceResult, trace, result);
        }

        // Return the result
        return result;
    }

    #endregion

    #region UpdateAsyncInternalBase<TEntity>

    /// <summary>
    /// Updates an existing row in the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <typeparam name="TEntity">The type of the object (whether a data entity or a dynamic).</typeparam>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entity">The data entity or dynamic object to be updated.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of affected rows during the update process.</returns>
    internal static async ValueTask<int> UpdateAsyncInternalBase<TEntity>(this IDbConnection connection,
        string tableName,
        TEntity entity,
        QueryGroup? where,
        IEnumerable<Field> fields,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.Update,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Set the flags
        where?.IsForUpdate();

        // Get the context
        var entityType = GetEntityType<TEntity>(entity);
        var context = await UpdateExecutionContextProvider.CreateAsync(entityType,
            connection,
            tableName,
            where,
            fields,
            hints,
            transaction,
            statementBuilder,
            cancellationToken).ConfigureAwait(false);
        var result = 0;

        // Create the command
        using (var command = (DbCommand)(await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false)).CreateCommand(context.CommandText,
            CommandType.Text, commandTimeout, transaction))
        {
            // Set the values
            context.ParametersSetterFunc(command, entity);

            // Add the fields from the query group
            WhereToCommandParameters(command, where, entity.GetType(),
                await DbFieldCache.GetAsync(connection, tableName, transaction, cancellationToken).ConfigureAwait(false));

            // Before Execution
            var traceResult = await Tracer
                .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

            // Silent cancellation
            if (traceResult?.CancellableTraceLog?.IsCancelled == true)
            {
                return result;
            }

            // Actual Execution
            result = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // After Execution
            await Tracer
                .InvokeAfterExecutionAsync(traceResult, trace, result, cancellationToken).ConfigureAwait(false);
        }

        // Return the result
        return result;
    }

    #endregion
}
