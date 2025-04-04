#nullable enable
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Dynamic;
using System.Globalization;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using RepoDb.Enumerations;
using RepoDb.Exceptions;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Reflection;

namespace RepoDb;

/// <summary>
/// Contains the extension methods for <see cref="IDbConnection"/> object.
/// </summary>
public static partial class DbConnectionExtension
{
    #region Other Methods

    /// <summary>
    /// Creates a command object.
    /// </summary>
    /// <param name="connection">The connection to be used when creating a command object.</param>
    /// <param name="commandText">The value of the <see cref="IDbCommand.CommandText"/> property.</param>
    /// <param name="commandType">The value of the <see cref="IDbCommand.CommandType"/> property.</param>
    /// <param name="commandTimeout">The value of the <see cref="IDbCommand.CommandTimeout"/> property.</param>
    /// <param name="transaction">The value of the <see cref="IDbCommand.Transaction"/> property.</param>
    /// <returns>A command object instance containing the defined property values passed.</returns>
    public static IDbCommand CreateCommand(this IDbConnection connection,
        string commandText,
        CommandType commandType = default,
        int commandTimeout = 0,
        IDbTransaction? transaction = null)
    {
        var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.Connection = connection;
        if (commandType > 0)
        {
            command.CommandType = commandType;
        }
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }
        if (transaction != null)
        {
            command.Transaction = transaction;
        }
        return command;
    }

    /// <summary>
    /// Ensures the connection object is open.
    /// </summary>
    /// <param name="connection">The connection to be opened.</param>
    /// <returns>The instance of the current connection object.</returns>
    public static IDbConnection EnsureOpen(this IDbConnection connection)
    {
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
        }
        return connection;
    }

    /// <summary>
    /// Ensures the connection object is open in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection to be opened.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The instance of the current connection object.</returns>
    public static async Task<IDbConnection> EnsureOpenAsync(this IDbConnection connection,
        CancellationToken cancellationToken = default)
    {
        if (connection.State != ConnectionState.Open)
        {
            await ((DbConnection)connection).OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        return connection;
    }

    #endregion

    #region ExecuteQuery

    /// <summary>
    /// Executes a SQL statement from the database. It uses the underlying method of <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/> and
    /// converts the result back to an enumerable list of dynamic objects.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The dynamic object to be used as parameter. This object must contain all the values for all the parameters
    /// defined in the <see cref="IDbCommand.CommandText"/> property.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <returns>
    /// An enumerable list of dynamic objects containing the converted results of the underlying <see cref="IDataReader"/> object.
    /// </returns>
    public static IEnumerable<dynamic> ExecuteQuery(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null)
    {
        return ExecuteQueryInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            tableName: null,
            transaction: transaction,
            trace: trace,
            traceKey: traceKey,
            cache: cache,
            cacheKey: cacheKey,
            cacheItemExpiration: cacheItemExpiration,
            skipCommandArrayParametersCheck: false);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="tableName"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    internal static IEnumerable<dynamic> ExecuteQueryInternal(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        int commandTimeout = 0,
        string? tableName = null,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        string? traceKey = TraceKeys.ExecuteQuery,
        ICache? cache = null,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        bool skipCommandArrayParametersCheck = true)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = cache.Get<IEnumerable<dynamic>>(cacheKey, false);
            if (item != null)
            {
                return item.Value;
            }
        }

        // DB Fields
        var dbFields = !string.IsNullOrWhiteSpace(tableName) ?
            DbFieldCache.Get(connection, tableName, transaction, false) : null;

        // Execute the actual method
        using var command = CreateDbCommandForExecution(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: null,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);

        // Variables
        IEnumerable<dynamic>? result = null;

        // Before Execution
        var traceResult = Tracer
            .InvokeBeforeExecution(traceKey, trace, command);

        // Silent cancellation
        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
        {
            return result!;
        }

        // Execute
        using (var reader = command.ExecuteReader())
        {
            result = DataReader.ToEnumerable(reader, dbFields, connection.GetDbSetting()).AsList();

            // After Execution
            Tracer
                .InvokeAfterExecution(traceResult, trace, result);

            // Set Cache
            if (cache != null && cacheKey != null)
            {
                cache.Add(cacheKey, result, cacheItemExpiration, false);
            }
        }

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    #endregion

    #region ExecuteQueryAsync

    /// <summary>
    /// Executes a SQL statement from the database in an asynchronous way. It uses the underlying method of <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/> and
    /// converts the result back to an enumerable list of dynamic objects.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The dynamic object to be used as parameter. This object must contain all the values for all the parameters
    /// defined in the <see cref="IDbCommand.CommandText"/> property.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>
    /// An enumerable list of dynamic objects containing the converted results of the underlying <see cref="IDataReader"/> object.
    /// </returns>
    public static async Task<IEnumerable<dynamic>> ExecuteQueryAsync(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteQueryAsyncInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            tableName: null,
            commandTimeout: commandTimeout,
            transaction: transaction,
            trace: trace,
            traceKey: traceKey,
            cache: cache,
            cacheKey: cacheKey,
            cacheItemExpiration: cacheItemExpiration,
            skipCommandArrayParametersCheck: false,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="tableName"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    internal static async ValueTask<IEnumerable<dynamic>> ExecuteQueryAsyncInternal(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? tableName = null,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        string? traceKey = TraceKeys.ExecuteQuery,
        ICache? cache = null,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        bool skipCommandArrayParametersCheck = true,
        CancellationToken cancellationToken = default)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = await cache.GetAsync<IEnumerable<dynamic>>(cacheKey, false, cancellationToken).ConfigureAwait(false);
            if (item != null)
            {
                return item.Value;
            }
        }

        // DB Fields
        var dbFields = !string.IsNullOrWhiteSpace(tableName) ?
            await DbFieldCache.GetAsync(connection, tableName, transaction, false, cancellationToken).ConfigureAwait(false) : null;

        // Execute the actual method
        using var command = await CreateDbCommandForExecutionAsync(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: null,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // Variables
        IEnumerable<dynamic>? result = null;

        // Before Execution
        var traceResult = await Tracer
            .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

        // Silent cancellation
        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
        {
            return result!;
        }

        // Execute
        using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            result = await DataReader.ToEnumerableAsync(reader, dbFields, connection.GetDbSetting(), cancellationToken)
                .ToListAsync(cancellationToken).ConfigureAwait(false);

            // After Execution
            await Tracer
                .InvokeAfterExecutionAsync(traceResult, trace, result, cancellationToken).ConfigureAwait(false);

            // Set Cache
            if (cache != null && cacheKey != null)
            {
                await cache.AddAsync(cacheKey, result, cacheItemExpiration, false, cancellationToken).ConfigureAwait(false);
            }
        }

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    #endregion

    #region ExecuteQuery<TResult>

    /// <summary>
    /// Executes a SQL statement from the database. It uses the underlying method of <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/> and
    /// converts the result back to an enumerable list of the target result type.
    /// </summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <returns>
    /// An enumerable list of the target result type instances containing the converted results of the underlying <see cref="IDataReader"/> object.
    /// </returns>
    public static IEnumerable<TResult> ExecuteQuery<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null)
    {
        return ExecuteQueryInternal<TResult>(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            cacheKey: cacheKey,
            cacheItemExpiration: cacheItemExpiration,
            traceKey: traceKey,
            commandTimeout: commandTimeout,
            transaction: transaction,
            cache: cache,
            trace: trace,
            tableName: ClassMappedNameCache.Get<TResult>(),
            skipCommandArrayParametersCheck: false);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="tableName"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    internal static IEnumerable<TResult> ExecuteQueryInternal<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        string? tableName = null,
        bool skipCommandArrayParametersCheck = true)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = cache.Get<IEnumerable<TResult>>(cacheKey, false);
            if (item != null)
            {
                return item.Value;
            }
        }

        // Variables
        var typeOfResult = typeof(TResult);

        // Identify
        if (TypeCache.Get(typeOfResult).IsDictionaryStringObject() || typeOfResult.IsObjectType())
        {
            return ExecuteQueryInternalForDictionaryStringObject<TResult>(connection: connection,
                commandText: commandText,
                param: param,
                commandType: commandType,
                cacheKey: cacheKey,
                cacheItemExpiration: cacheItemExpiration,
                traceKey: traceKey,
                commandTimeout: commandTimeout,
                transaction: transaction,
                cache: cache,
                trace: trace,
                tableName: tableName,
                skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);
        }
        else
        {
            return ExecuteQueryInternalForType<TResult>(connection: connection,
                commandText: commandText,
                param: param,
                commandType: commandType,
                cacheKey: cacheKey,
                cacheItemExpiration: cacheItemExpiration,
                traceKey: traceKey,
                commandTimeout: commandTimeout,
                transaction: transaction,
                cache: cache,
                trace: trace,
                tableName: tableName,
                skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="tableName"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    private static IEnumerable<TResult> ExecuteQueryInternalForDictionaryStringObject<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        string? tableName = null,
        bool skipCommandArrayParametersCheck = true)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = cache.Get<IEnumerable<TResult>>(cacheKey, false);
            if (item != null)
            {
                return item.Value;
            }
        }

        // Call
        var result = ExecuteQueryInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            trace: trace,
            traceKey: traceKey,
            cacheKey: null,
            cacheItemExpiration: cacheItemExpiration,
            cache: null,
            tableName: tableName,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck).WithType<TResult>();

        // Set Cache
        if (cache != null && cacheKey != null)
        {
            cache.Add(cacheKey, result, cacheItemExpiration, false);
        }

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="tableName"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    private static IEnumerable<TResult> ExecuteQueryInternalForType<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        string? tableName = null,
        bool skipCommandArrayParametersCheck = true)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = cache.Get<IEnumerable<TResult>>(cacheKey, false);
            if (item != null)
            {
                return item.Value;
            }
        }

        // DB Fields
        var dbFields = !string.IsNullOrWhiteSpace(tableName) ?
            DbFieldCache.Get(connection, tableName, transaction, false) : null;

        // Execute the actual method
        using var command = CreateDbCommandForExecution(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: typeof(TResult),
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);

        // Variables
        IEnumerable<TResult>? result = null;

        // Before Execution
        var traceResult = Tracer
            .InvokeBeforeExecution(traceKey, trace, command);

        // Silent cancellation
        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
        {
            return null!;
        }

        // Execute
        using (var reader = command.ExecuteReader())
        {
            result = DataReader.ToEnumerable<TResult>(reader, dbFields, connection.GetDbSetting()).AsList();

            // After Execution
            Tracer
                .InvokeAfterExecution(traceResult, trace, result);

            // Set Cache
            if (cache != null && cacheKey != null)
            {
                cache.Add(cacheKey, result, cacheItemExpiration, false);
            }
        }

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    #endregion

    #region ExecuteQueryAsync<TResult>

    /// <summary>
    /// Executes a SQL statement from the database in an asynchronous way. It uses the underlying method of <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/> and
    /// converts the result back to an enumerable list of the target result type.
    /// </summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>
    /// An enumerable list of the target result type instances containing the converted results of the underlying <see cref="IDataReader"/> object.
    /// </returns>
    public static async Task<IEnumerable<TResult>> ExecuteQueryAsync<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteQueryAsyncInternal<TResult>(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            cacheKey: cacheKey,
            cacheItemExpiration: cacheItemExpiration,
            traceKey: traceKey,
            commandTimeout: commandTimeout,
            transaction: transaction,
            cache: cache,
            trace: trace,
            tableName: ClassMappedNameCache.Get<TResult>(),
            skipCommandArrayParametersCheck: false,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="tableName"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    internal static async ValueTask<IEnumerable<TResult>> ExecuteQueryAsyncInternal<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        string? tableName = null,
        bool skipCommandArrayParametersCheck = true,
        CancellationToken cancellationToken = default)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = await cache.GetAsync<IEnumerable<TResult>>(cacheKey, false, cancellationToken).ConfigureAwait(false);
            if (item != null)
            {
                return item.Value;
            }
        }

        // Variables
        var typeOfResult = typeof(TResult);

        // Identify
        if (TypeCache.Get(typeOfResult).IsDictionaryStringObject() || typeOfResult.IsObjectType())
        {
            return await ExecuteQueryAsyncInternalForDictionaryStringObject<TResult>(connection: connection,
               commandText: commandText,
               param: param,
               commandType: commandType,
               cacheKey: cacheKey,
               cacheItemExpiration: cacheItemExpiration,
               commandTimeout: commandTimeout,
               traceKey: traceKey,
               transaction: transaction,
               cache: cache,
               trace: trace,
               cancellationToken: cancellationToken,
               tableName: tableName,
               skipCommandArrayParametersCheck: skipCommandArrayParametersCheck).ConfigureAwait(false);
        }
        else
        {
            return await ExecuteQueryAsyncInternalForType<TResult>(connection: connection,
               commandText: commandText,
               param: param,
               commandType: commandType,
               cacheKey: cacheKey,
               cacheItemExpiration: cacheItemExpiration,
               commandTimeout: commandTimeout,
               traceKey: traceKey,
               transaction: transaction,
               cache: cache,
               trace: trace,
               cancellationToken: cancellationToken,
               tableName: tableName,
               skipCommandArrayParametersCheck: skipCommandArrayParametersCheck).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="tableName"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    private static async ValueTask<IEnumerable<TResult>> ExecuteQueryAsyncInternalForDictionaryStringObject<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        string? tableName = null,
        bool skipCommandArrayParametersCheck = true,
        CancellationToken cancellationToken = default)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = await cache.GetAsync<IEnumerable<TResult>>(cacheKey, false, cancellationToken).ConfigureAwait(false);
            if (item != null)
            {
                return item.Value;
            }
        }

        // Call
        var result = (await ExecuteQueryAsyncInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            trace: trace,
            traceKey: traceKey,
            cacheKey: null,
            cacheItemExpiration: cacheItemExpiration,
            cache: null,
            cancellationToken: cancellationToken,
            tableName: tableName,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck).ConfigureAwait(false)).WithType<TResult>();

        // Set Cache
        if (cache != null && cacheKey != null)
        {
            await cache.AddAsync(cacheKey, result, cacheItemExpiration, false, cancellationToken).ConfigureAwait(false);
        }

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="tableName"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    private static async ValueTask<IEnumerable<TResult>> ExecuteQueryAsyncInternalForType<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        string? tableName = null,
        bool skipCommandArrayParametersCheck = true,
        CancellationToken cancellationToken = default)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = await cache.GetAsync<IEnumerable<TResult>>(cacheKey, false, cancellationToken).ConfigureAwait(false);
            if (item != null)
            {
                return item.Value;
            }
        }

        // DB Fields
        var dbFields = !string.IsNullOrWhiteSpace(tableName) ?
            await DbFieldCache.GetAsync(connection, tableName, transaction, false, cancellationToken).ConfigureAwait(false) : null;

        // Execute the actual method
        using var command = await CreateDbCommandForExecutionAsync(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: typeof(TResult),
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        IEnumerable<TResult>? result = null;

        // Before Execution
        var traceResult = await Tracer
            .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

        // Silent cancellation
        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
        {
            return result!;
        }

        // Execute
        using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            result = await DataReader.ToEnumerableAsync<TResult>(reader, dbFields, connection.GetDbSetting(), cancellationToken)
                .ToListAsync(cancellationToken).ConfigureAwait(false);

            // After Execution
            await Tracer
                .InvokeAfterExecutionAsync(traceResult, trace, result, cancellationToken).ConfigureAwait(false);

            // Set Cache
            if (cache != null && cacheKey != null)
            {
                await cache.AddAsync(cacheKey, result, cacheItemExpiration, false, cancellationToken).ConfigureAwait(false);
            }
        }

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    #endregion

    #region ExecuteQueryMultiple(Results)

    /// <summary>
    /// Execute the multiple SQL statements from the database.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <returns>An instance of <see cref="QueryMultipleExtractor"/> used to extract the results.</returns>
    public static QueryMultipleExtractor ExecuteQueryMultiple(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQueryMultiple,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null) =>
        ExecuteQueryMultipleInternal(connection,
            commandText,
            param,
            commandType,
            cacheKey,
            cacheItemExpiration,
            traceKey,
            commandTimeout,
            transaction,
            cache,
            trace,
            false);

    /// <summary>
    /// 
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="isDisposeConnection"></param>
    /// <returns></returns>
    internal static QueryMultipleExtractor ExecuteQueryMultipleInternal(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQueryMultiple,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        bool isDisposeConnection = false)
    {
        IDataReader? reader = null;

        // Get Cache
        if (cacheKey == null || cache?.Contains(cacheKey) != true)
        {
            // Call
            reader = ExecuteReaderInternal(connection: connection,
                commandText: commandText,
                param: param,
                commandType: commandType,
                traceKey: traceKey,
                commandTimeout: commandTimeout,
                transaction: transaction,
                trace: trace,
                entityType: null,
                dbFields: null,
                skipCommandArrayParametersCheck: false);
        }

        // Return
        return new QueryMultipleExtractor((DbConnection)connection,
            (DbDataReader?)reader,
            param,
            cacheKey,
            cacheItemExpiration,
            cache,
            isDisposeConnection);
    }

    #endregion

    #region ExecuteQueryMultipleAsync(Results)

    /// <summary>
    /// Execute the multiple SQL statements from the database in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An instance of <see cref="QueryMultipleExtractor"/> used to extract the results.</returns>
    public static async Task<QueryMultipleExtractor> ExecuteQueryMultipleAsync(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQueryMultiple,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        CancellationToken cancellationToken = default) =>
        await ExecuteQueryMultipleAsyncInternal(connection,
            commandText,
            param,
            commandType,
            cacheKey,
            cacheItemExpiration,
            traceKey,
            commandTimeout,
            transaction,
            cache,
            trace,
            false,
            cancellationToken);

    /// <summary>
    /// 
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="cache"></param>
    /// <param name="trace"></param>
    /// <param name="isDisposeConnection"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<QueryMultipleExtractor> ExecuteQueryMultipleAsyncInternal(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteQueryMultiple,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        bool isDisposeConnection = false,
        CancellationToken cancellationToken = default)
    {
        IDataReader? reader = null;

        // Get Cache
        if (cacheKey == null || cache?.Contains(cacheKey) != true)
        {
            // Call
            reader = await ExecuteReaderAsyncInternal(connection: connection,
                commandText: commandText,
                param: param,
                commandType: commandType,
                traceKey: traceKey,
                commandTimeout: commandTimeout,
                transaction: transaction,
                trace: trace,
                cancellationToken: cancellationToken,
                entityType: null,
                dbFields: null,
                skipCommandArrayParametersCheck: false).ConfigureAwait(false);
        }

        // Return
        return new QueryMultipleExtractor((DbConnection)connection,
            (DbDataReader?)reader,
            param,
            cacheKey,
            cacheItemExpiration,
            cache,
            isDisposeConnection,
            cancellationToken);
    }

    #endregion

    #region ExecuteReader

    /// <summary>
    /// Executes a SQL statement from the database. It uses the underlying method of <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/> and
    /// returns the instance of the data reader.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <return>The instance of the <see cref="IDataReader"/> object.</return>
    public static IDataReader ExecuteReader(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? traceKey = TraceKeys.ExecuteReader,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ITrace? trace = null)
    {
        return ExecuteReaderInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            traceKey: traceKey,
            commandTimeout: commandTimeout,
            transaction: transaction,
            trace: trace,
            entityType: null,
            dbFields: null,
            skipCommandArrayParametersCheck: false);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="trace"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <param name="beforeExecutionCallback"></param>
    /// <returns></returns>
    internal static IDataReader ExecuteReaderInternal(this IDbConnection connection,
        string commandText,
        object? param,
        CommandType commandType,
        int commandTimeout,
        string traceKey,
        IDbTransaction? transaction,
        ITrace? trace,
        Type? entityType,
        DbFieldCollection? dbFields,
        bool skipCommandArrayParametersCheck,
        Func<DbCommand, TraceResult>? beforeExecutionCallback = null)
    {
        // Variables
        var setting = DbSettingMapper.Get(connection);
        var command = CreateDbCommandForExecution(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: entityType,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);
        var hasError = false;

        // Ensure the DbCommand disposal
        try
        {
            // A hacky solution for other operations (i.e.: QueryMultiple)
            var traceResult = beforeExecutionCallback?.Invoke(command);

            // Before Execution
            traceResult ??= Tracer
                .InvokeBeforeExecution(traceKey, trace, command);

            // Silent cancellation
            if (traceResult?.CancellableTraceLog?.IsCancelled == true)
            {
                return null!;
            }

            // Execute
            var reader = command.ExecuteReader();

            // After Execution
            Tracer
                .InvokeAfterExecution(traceResult, trace, reader);

            // Set the output parameters
            SetOutputParameters(param);

            // Return
            return reader;
        }
        catch
        {
            hasError = true;
            throw;
        }
        finally
        {
            if (setting?.IsExecuteReaderDisposable == true || hasError)
            {
                command.Dispose();
            }
        }
    }

    #endregion

    #region ExecuteReaderAsync

    /// <summary>
    /// Executes a SQL statement from the database in an asynchronous way. It uses the underlying method of <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/> and
    /// returns the instance of the data reader.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <return>The instance of the <see cref="IDataReader"/> object.</return>
    public static async Task<IDataReader> ExecuteReaderAsync(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? traceKey = TraceKeys.ExecuteReader,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteReaderAsyncInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            traceKey: traceKey,
            commandTimeout: commandTimeout,
            transaction: transaction,
            trace: trace,
            cancellationToken: cancellationToken,
            entityType: null,
            dbFields: null,
            skipCommandArrayParametersCheck: false);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="trace"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <param name="beforeExecutionCallbackAsync"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<IDataReader> ExecuteReaderAsyncInternal(this IDbConnection connection,
        string commandText,
        object? param,
        CommandType commandType,
        int commandTimeout,
        string? traceKey,
        IDbTransaction? transaction,
        ITrace? trace,
        Type? entityType,
        DbFieldCollection? dbFields,
        bool skipCommandArrayParametersCheck,
        Func<DbCommand, CancellationToken, Task<TraceResult>>? beforeExecutionCallbackAsync = null,
        CancellationToken cancellationToken = default)
    {
        // Variables
        var setting = connection.GetDbSetting();
        var command = await CreateDbCommandForExecutionAsync(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: entityType,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var hasError = false;

        // Ensure the DbCommand disposal
        try
        {
            TraceResult? traceResult = null;

            // A hacky solution for other operations (i.e.: QueryMultipleAsync)
            if (beforeExecutionCallbackAsync != null)
            {
                traceResult = await beforeExecutionCallbackAsync(command, cancellationToken).ConfigureAwait(false);
            }

            // Before Execution
            traceResult ??= await Tracer
                .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

            // Silent cancellation
            if (traceResult?.CancellableTraceLog?.IsCancelled == true)
            {
                return null!;
            }

            // Execute
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            // After Execution
            await Tracer
                .InvokeAfterExecutionAsync(traceResult, trace, reader, cancellationToken).ConfigureAwait(false);

            // Set the output parameters
            SetOutputParameters(param);

            // Return
            return reader;
        }
        catch
        {
            hasError = true;
            throw;
        }
        finally
        {
            if (setting.IsExecuteReaderDisposable || hasError)
            {
                command.Dispose();
            }
        }
    }

    #endregion

    #region ExecuteNonQuery

    /// <summary>
    /// Executes a SQL statement from the database. It uses the underlying method of <see cref="IDbCommand.ExecuteNonQuery"/> and
    /// returns the number of affected rows during the execution.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <returns>The number of rows affected by the execution.</returns>
    public static int ExecuteNonQuery(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? traceKey = TraceKeys.ExecuteNonQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ITrace? trace = null)
    {
        return ExecuteNonQueryInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            traceKey: traceKey,
            commandTimeout: commandTimeout,
            transaction: transaction,
            trace: trace,
            entityType: null,
            dbFields: null,
            skipCommandArrayParametersCheck: false);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="trace"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    internal static int ExecuteNonQueryInternal(this IDbConnection connection,
        string commandText,
        object? param,
        CommandType commandType,
        int commandTimeout,
        string? traceKey,
        IDbTransaction? transaction,
        ITrace? trace,
        Type? entityType,
        DbFieldCollection? dbFields,
        bool skipCommandArrayParametersCheck)
    {
        using var command = CreateDbCommandForExecution(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: entityType,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);

        // Before Execution
        var traceResult = Tracer
            .InvokeBeforeExecution(traceKey, trace, command);

        // Silent cancellation
        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
        {
            return default;
        }

        // Execution
        var result = command.ExecuteNonQuery();

        // After Execution
        Tracer
            .InvokeAfterExecution(traceResult, trace, result);

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    #endregion

    #region ExecuteNonQueryAsync

    /// <summary>
    /// Executes a SQL statement from the database in an asynchronous way. It uses the underlying method of <see cref="IDbCommand.ExecuteNonQuery"/> and
    /// returns the number of affected rows during the execution.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows affected by the execution.</returns>
    public static async Task<int> ExecuteNonQueryAsync(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? traceKey = TraceKeys.ExecuteNonQuery,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteNonQueryAsyncInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            traceKey: traceKey,
            commandTimeout: commandTimeout,
            transaction: transaction,
            trace: trace,
            cancellationToken: cancellationToken,
            entityType: null,
            dbFields: null,
            skipCommandArrayParametersCheck: false);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="traceKey"></param>
    /// <param name="transaction"></param>
    /// <param name="trace"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<int> ExecuteNonQueryAsyncInternal(this IDbConnection connection,
        string commandText,
        object? param,
        CommandType commandType,
        int commandTimeout,
        string? traceKey,
        IDbTransaction? transaction,
        ITrace? trace,
        Type? entityType,
        DbFieldCollection? dbFields,
        bool skipCommandArrayParametersCheck,
        CancellationToken cancellationToken)
    {
        using var command = await CreateDbCommandForExecutionAsync(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: entityType,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // Before Execution
        var traceResult = await Tracer
            .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

        // Silent cancellation
        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
        {
            return default;
        }

        // Execution
        var result = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // After Execution
        await Tracer
            .InvokeAfterExecutionAsync(traceResult, trace, result, cancellationToken).ConfigureAwait(false);

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    #endregion

    #region ExecuteScalar

    /// <summary>
    /// Executes a SQL statement from the database. It uses the underlying method of <see cref="IDbCommand.ExecuteScalar"/> and
    /// returns the first occurrence value (first column of first row) of the execution.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <returns>An object that holds the first occurrence value (first column of first row) of the execution.</returns>
    public static object? ExecuteScalar(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteScalar,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null)
    {
        return ExecuteScalarInternal<object>(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            traceKey: traceKey,
            cacheKey: cacheKey,
            cacheItemExpiration: cacheItemExpiration,
            commandTimeout: commandTimeout,
            transaction: transaction,
            cache: cache,
            trace: trace,
            entityType: null,
            dbFields: null,
            skipCommandArrayParametersCheck: false);
    }

    #endregion

    #region ExecuteScalarAsync

    /// <summary>
    /// Executes a SQL statement from the database in an asynchronous way. It uses the underlying method of <see cref="IDbCommand.ExecuteScalar"/> and
    /// returns the first occurrence value (first column of first row) of the execution.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An object that holds the first occurrence value (first column of first row) of the execution.</returns>
    public static async Task<object?> ExecuteScalarAsync(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteScalar,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteScalarAsyncInternal<object>(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            traceKey: traceKey,
            cacheKey: cacheKey,
            cacheItemExpiration: cacheItemExpiration,
            commandTimeout: commandTimeout,
            transaction: transaction,
            cache: cache,
            trace: trace,
            cancellationToken: cancellationToken,
            entityType: null,
            dbFields: null,
            skipCommandArrayParametersCheck: false);
    }

    #endregion

    #region ExecuteScalar<TResult>

    /// <summary>
    /// Executes a SQL statement from the database. It uses the underlying method of <see cref="IDbCommand.ExecuteScalar"/> and
    /// returns the first occurrence value (first column of first row) of the execution.
    /// </summary>
    /// <typeparam name="TResult">The target return type.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <returns>A first occurrence value (first column of first row) of the execution.</returns>
    public static TResult? ExecuteScalar<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteScalar,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null)
    {
        return ExecuteScalarInternal<TResult>(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            traceKey: traceKey,
            cacheKey: cacheKey,
            cacheItemExpiration: cacheItemExpiration,
            commandTimeout: commandTimeout,
            transaction: transaction,
            cache: cache,
            trace: trace,
            entityType: null,
            dbFields: null,
            skipCommandArrayParametersCheck: false);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="transaction"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <param name="trace"></param>
    /// <param name="traceKey"></param>
    /// <param name="cache"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <returns></returns>
    internal static TResult? ExecuteScalarInternal<TResult>(this IDbConnection connection,
        string commandText,
        object? param,
        CommandType commandType,
        int commandTimeout,
        IDbTransaction? transaction,
        Type? entityType,
        DbFieldCollection? dbFields,
        bool skipCommandArrayParametersCheck = true,
        ITrace? trace = null,
        string? traceKey = null,
        ICache? cache = null,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = cache.Get<TResult>(cacheKey, false);
            if (item != null)
            {
                return item.Value;
            }
        }

        using var command = CreateDbCommandForExecution(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: entityType,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);

        // Before Execution
        var traceResult = Tracer
            .InvokeBeforeExecution(traceKey, trace, command);

        // Silent cancellation
        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
        {
            return default;
        }

        // Execute
        var result = Converter.ToType<TResult>(command.ExecuteScalar());

        // After Execution
        Tracer
            .InvokeAfterExecution(traceResult, trace, result);

        // Set Cache
        if (cache != null && cacheKey != null)
        {
            cache.Add(cacheKey, result, cacheItemExpiration, false);
        }

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    #endregion

    #region ExecuteScalarAsync<TResult>

    /// <summary>
    /// Executes a SQL statement from the database in an asynchronous way. It uses the underlying method of <see cref="IDbCommand.ExecuteScalar"/> and
    /// returns the first occurrence value (first column of first row) of the execution.
    /// </summary>
    /// <typeparam name="TResult">The target return type.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="commandText">The command text to be used.</param>
    /// <param name="param">
    /// The parameters/values defined in the <see cref="IDbCommand.CommandText"/> property. Supports a dynamic object, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="ExpandoObject"/>, <see cref="QueryField"/>, <see cref="QueryGroup"/> and an enumerable of <see cref="QueryField"/> objects.
    /// </param>
    /// <param name="commandType">The command type to be used.</param>
    /// <param name="cacheKey">
    /// The key to the cache item. By setting this argument, it will return the item from the cache if present, otherwise it will query the database.
    /// This will only work if the 'cache' argument is set.
    /// </param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cache">The cache object to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A first occurrence value (first column of first row) of the execution.</returns>
    public static async Task<TResult?> ExecuteScalarAsync<TResult>(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        string? traceKey = TraceKeys.ExecuteScalar,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        ICache? cache = null,
        ITrace? trace = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteScalarAsyncInternal<TResult>(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            traceKey: traceKey,
            cacheKey: cacheKey,
            cacheItemExpiration: cacheItemExpiration,
            commandTimeout: commandTimeout,
            transaction: transaction,
            cache: cache,
            trace: trace,
            cancellationToken: cancellationToken,
            entityType: null,
            dbFields: null,
            skipCommandArrayParametersCheck: false);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="transaction"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <param name="trace"></param>
    /// <param name="traceKey"></param>
    /// <param name="cache"></param>
    /// <param name="cacheKey"></param>
    /// <param name="cacheItemExpiration"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<TResult?> ExecuteScalarAsyncInternal<TResult>(
        this IDbConnection connection,
        string commandText,
        object? param,
        CommandType commandType,
        int commandTimeout,
        IDbTransaction? transaction,
        Type? entityType,
        DbFieldCollection? dbFields,
        bool skipCommandArrayParametersCheck = true,
        ITrace? trace = null,
        string? traceKey = null,
        ICache? cache = null,
        string? cacheKey = null,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        CancellationToken cancellationToken = default)
    {
        // Get Cache
        if (cache != null && cacheKey != null)
        {
            var item = await cache.GetAsync<TResult>(cacheKey, false, cancellationToken).ConfigureAwait(false);
            if (item != null)
            {
                return item.Value;
            }
        }

        using var command = await CreateDbCommandForExecutionAsync(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: entityType,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // Before Execution
        var traceResult = await Tracer
            .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

        // Silent cancellation
        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
        {
            return default;
        }

        // Execution
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is { } v ? Converter.ToType<TResult>(v) : default;

        // After Execution
        await Tracer
            .InvokeAfterExecutionAsync(traceResult, trace, result, cancellationToken).ConfigureAwait(false);

        // Set Cache
        if (cache != null && cacheKey != null)
        {
            await cache.AddAsync(cacheKey, result, cacheItemExpiration, false, cancellationToken).ConfigureAwait(false);
        }

        // Set the output parameters
        SetOutputParameters(param);

        // Return
        return result;
    }

    #endregion

    #region Mapped Operations

    /// <summary>
    /// Gets the associated <see cref="IDbSetting"/> object that is currently mapped on the target <see cref="IDbConnection"/> object.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <returns>An instance of the mapped <see cref="IDbSetting"/> object.</returns>
    public static IDbSetting GetDbSetting(this IDbConnection connection)
    {
        // Check the connection
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        // Get the setting
        var setting = DbSettingMapper.Get(connection);

        // Check the presence
        if (setting == null)
        {
            ThrowMissingMappingException("setting", connection.GetType());
        }

        // Return the validator
        return setting!;
    }

    /// <summary>
    /// Gets the associated <see cref="IDbHelper"/> object that is currently mapped on the target <see cref="IDbConnection"/> object.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <returns>An instance of the mapped <see cref="IDbHelper"/> object.</returns>
    public static IDbHelper GetDbHelper(this IDbConnection connection)
    {
        // Check the connection
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        // Get the setting
        var helper = DbHelperMapper.Get(connection);

        // Check the presence
        if (helper == null)
        {
            ThrowMissingMappingException("helper", connection.GetType());
        }

        // Return the validator
        return helper!;
    }

    /// <summary>
    /// Retrieves a collection of database fields for a specified table from the cache.
    /// </summary>
    /// <param name="connection">Establishes the connection to the database for retrieving field information.</param>
    /// <param name="tableName">Specifies the name of the table for which the fields are being retrieved.</param>
    /// <param name="transaction">Indicates an optional transaction context for the database operation.</param>
    /// <returns>Returns a collection of database fields associated with the specified table.</returns>
    public static DbFieldCollection GetDbFields(this IDbConnection connection, string tableName, IDbTransaction? transaction = null)
    {
        return DbFieldCache.Get(connection, tableName, transaction);
    }

    /// <summary>
    /// Retrieves database fields for a specified table asynchronously.
    /// </summary>
    /// <param name="connection">Establishes the connection to the database for executing the query.</param>
    /// <param name="tableName">Specifies the name of the table from which to retrieve the fields.</param>
    /// <param name="transaction">Allows for the execution of the operation within a specific database transaction context.</param>
    /// <param name="cancellationToken">Enables the operation to be canceled if needed before completion.</param>
    /// <returns>Returns a collection of database fields for the specified table.</returns>
    public static async ValueTask<DbFieldCollection> GetDbFieldsAsync(this IDbConnection connection, string tableName, IDbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        return await DbFieldCache.GetAsync(connection, tableName, transaction, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets the associated <see cref="IStatementBuilder"/> object that is currently mapped on the target <see cref="IDbConnection"/> object.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <returns>An instance of the mapped <see cref="IStatementBuilder"/> object.</returns>
    public static IStatementBuilder GetStatementBuilder(this IDbConnection connection)
    {
        // Check the connection
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        // Get the setting
        var statementBuilder = StatementBuilderMapper.Get(connection);

        // Check the presence
        if (statementBuilder == null)
        {
            ThrowMissingMappingException("statement builder", connection.GetType());
        }

        // Return the validator
        return statementBuilder!;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="property"></param>
    /// <param name="connectionType"></param>
#if NET
    [DoesNotReturn]
#endif
    private static void ThrowMissingMappingException(string property,
        Type connectionType)
    {
        throw new MissingMappingException($"There is no database {property} mapping found for '{connectionType.FullName}'. Make sure to install the correct extension library and call the bootstrapper method. You can also visit the library's installation page (https://repodb.net/tutorial/installation).");
    }

    #endregion

    #region Helper Methods

    #region DbParameters

    /// <summary>
    ///
    /// </summary>
    /// <param name="param"></param>
    internal static void SetOutputParameters(object? param)
    {
        if (param is QueryGroup group)
        {
            SetOutputParameters(group);
        }
        else if (param is IEnumerable<QueryField> fields)
        {
            SetOutputParameters(fields);
        }
        else if (param is QueryField field)
        {
            SetOutputParameter(field);
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="queryGroup"></param>
    internal static void SetOutputParameters(QueryGroup queryGroup) =>
        SetOutputParameters(queryGroup.GetFields(true));

    /// <summary>
    ///
    /// </summary>
    /// <param name="queryFields"></param>
    internal static void SetOutputParameters(IEnumerable<QueryField> queryFields)
    {
        if (queryFields?.Any() != true)
        {
            return;
        }
        foreach (var queryField in queryFields.Where(e => e.DbParameter?.Direction != ParameterDirection.Input))
        {
            SetOutputParameter(queryField);
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="queryField"></param>
    internal static void SetOutputParameter(QueryField queryField)
    {
        if (queryField == null)
        {
            return;
        }
        queryField.Parameter.SetValue(queryField.GetValue());
    }

    #endregion

    #region Order Columns

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="command"></param>
    /// <param name="entities"></param>
    private static void AddOrderColumnParameters<TEntity>(DbCommand command,
        IEnumerable<TEntity> entities)
        where TEntity : class
    {
        var index = 0;
        foreach (var item in entities)
        {
            var parameter = command.CreateParameter($"__RepoDb_OrderColumn_{index}", index, DbType.Int32);
            parameter.Direction = ParameterDirection.Input;
            parameter.Size = 4;
            parameter.Precision = 10;
            parameter.Scale = 0;
            command.Parameters.Add(parameter);
            index++;
        }
    }

    #endregion

    #region GetAndGuardPrimaryKeyOrIdentityKey

    /// <summary>
    ///
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="connection"></param>
    /// <param name="transaction"></param>
    /// <returns></returns>
    internal static IEnumerable<Field> GetAndGuardPrimaryKeyOrIdentityKey(Type entityType,
        IDbConnection connection,
        IDbTransaction transaction) =>
        GetAndGuardPrimaryKeyOrIdentityKey(connection, ClassMappedNameCache.Get(entityType),
            transaction, entityType);

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="transaction"></param>
    /// <param name="entityType"></param>
    /// <returns></returns>
    internal static IEnumerable<Field> GetAndGuardPrimaryKeyOrIdentityKey(IDbConnection connection,
        string tableName,
        IDbTransaction transaction,
        Type entityType)
    {
        var dbFields = DbFieldCache.Get(connection, tableName, transaction);
        var key = GetAndGuardPrimaryKeyOrIdentityKey(entityType, dbFields) ?? GetPrimaryOrIdentityKey(entityType);
        return GetAndGuardPrimaryKeyOrIdentityKey(tableName, key);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="transaction"></param>
    /// <returns></returns>
    internal static IEnumerable<DbField> GetAndGuardPrimaryKeyOrIdentityKey(IDbConnection connection,
        string tableName,
        IDbTransaction transaction)
    {
        var dbFields = DbFieldCache.Get(connection, tableName, transaction);
        var keys = dbFields?.GetPrimaryFields() ?? dbFields?.GetIdentity()?.AsEnumerable();
        return GetAndGuardPrimaryKeyOrIdentityKey(tableName, keys);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="connection"></param>
    /// <param name="transaction"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static ValueTask<IEnumerable<Field>> GetAndGuardPrimaryKeyOrIdentityKeyAsync(Type entityType,
        IDbConnection connection,
        IDbTransaction transaction,
        CancellationToken cancellationToken = default) =>
        GetAndGuardPrimaryKeyOrIdentityKeyAsync(connection, ClassMappedNameCache.Get(entityType),
            transaction, entityType, cancellationToken);

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="transaction"></param>
    /// <param name="entityType"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<IEnumerable<Field>> GetAndGuardPrimaryKeyOrIdentityKeyAsync(IDbConnection connection,
        string tableName,
        IDbTransaction transaction,
        Type entityType,
        CancellationToken cancellationToken = default)
    {
        var dbFields = await DbFieldCache.GetAsync(connection, tableName, transaction, cancellationToken).ConfigureAwait(false);
        var properties = GetAndGuardPrimaryKeyOrIdentityKey(entityType, dbFields) ?? GetPrimaryOrIdentityKey(entityType);
        return GetAndGuardPrimaryKeyOrIdentityKey(tableName, properties);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="transaction"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<IEnumerable<DbField>> GetAndGuardPrimaryKeyOrIdentityKeyAsync(IDbConnection connection,
        string tableName,
        IDbTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        var dbFields = await DbFieldCache.GetAsync(connection, tableName, transaction, cancellationToken).ConfigureAwait(false);
        var dbField = dbFields?.GetPrimary() ?? dbFields?.GetIdentity();
        return GetAndGuardPrimaryKeyOrIdentityKey(tableName, dbField);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="dbField"></param>
    /// <returns></returns>
    internal static IEnumerable<DbField> GetAndGuardPrimaryKeyOrIdentityKey(string tableName,
        DbField? dbField) =>
        dbField?.AsEnumerable() ?? throw GetKeyFieldNotFoundException(tableName);


    /// <summary>
    ///
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="dbField"></param>
    /// <returns></returns>
    internal static IEnumerable<DbField> GetAndGuardPrimaryKeyOrIdentityKey(string tableName,
        IEnumerable<DbField>? dbFields) =>
        dbFields ?? throw GetKeyFieldNotFoundException(tableName);

    /// <summary>
    ///
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="field"></param>
    /// <returns></returns>
    internal static IEnumerable<Field> GetAndGuardPrimaryKeyOrIdentityKey(string tableName,
        Field? field) =>
        field?.AsEnumerable() ?? throw GetKeyFieldNotFoundException(tableName);

    /// <summary>
    ///
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="field"></param>
    /// <returns></returns>
    internal static IEnumerable<Field> GetAndGuardPrimaryKeyOrIdentityKey(string tableName,
        IEnumerable<Field>? fields) =>
        fields ?? throw GetKeyFieldNotFoundException(tableName);

    /// <summary>
    ///
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <returns></returns>
    internal static IEnumerable<Field>? GetAndGuardPrimaryKeyOrIdentityKey(Type entityType,
        DbFieldCollection dbFields) =>
        entityType == null ? null :
            TypeCache.Get(entityType).IsDictionaryStringObject() ?
            GetAndGuardPrimaryKeyOrIdentityKeyForDictionaryStringObject(entityType, dbFields) :
            GetAndGuardPrimaryKeyOrIdentityKeyForEntity(entityType, dbFields);

    /// <summary>
    /// 
    /// </summary>
    /// <param name="type"></param>
    /// <param name="dbFields"></param>
    /// <returns></returns>
    internal static IEnumerable<Field> GetAndGuardPrimaryKeyOrIdentityKeyForDictionaryStringObject(Type type,
        DbFieldCollection dbFields)
    {
        // Primary/Identity
        var dbField = (dbFields.GetPrimaryFields() ??
            dbFields.GetIdentity()?.AsEnumerable() ??
            dbFields.GetByName("Id")?.AsEnumerable())
            ?? throw GetKeyFieldNotFoundException(type);

        // Return
        return dbField.AsFields();
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="type"></param>
    /// <param name="dbFields"></param>
    /// <returns></returns>
    internal static IEnumerable<Field> GetAndGuardPrimaryKeyOrIdentityKeyForEntity(Type type,
        DbFieldCollection dbFields)
    {
        // Properties
        var properties = PropertyCache.Get(type) ?? type.GetClassProperties();

        // Primary
        if (dbFields?.GetPrimaryFields() is { } dbPrimary) // Database driven
        {
            return dbPrimary.Select(f => properties.GetByMappedName(f.Name) ?? throw GetKeyFieldNotFoundException(type)).AsFields();
        }
        else if (PrimaryCache.GetPrimaryKeys(type) is { } pcPrimary) // Model driven
        {
            return pcPrimary.AsFields();
        }

        // Identity
        if (dbFields?.GetIdentity() is { } dbIdentity)
        {
            return properties.GetByMappedName(dbIdentity.Name)?.AsField()?.AsEnumerable() ?? throw GetKeyFieldNotFoundException(type);
        }

        throw GetKeyFieldNotFoundException(type);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="context"></param>
    /// <returns></returns>
    internal static KeyFieldNotFoundException GetKeyFieldNotFoundException(string context) =>
        new KeyFieldNotFoundException($"No primary key found at the '{context}'.");

    /// <summary>
    ///
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    internal static KeyFieldNotFoundException GetKeyFieldNotFoundException(Type type) =>
        new KeyFieldNotFoundException($"No primary key found at the target table and also to the given '{type.FullName}' object.");

    #endregion

    #region WhatToQueryGroup

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="what"></param>
    /// <param name="transaction"></param>
    /// <returns></returns>
    internal static QueryGroup? WhatToQueryGroup<T>(this IDbConnection connection,
        string tableName,
        T what,
        IDbTransaction transaction) where T : notnull
    {
        if (what == null)
        {
            return null;
        }
        var queryGroup = WhatToQueryGroup<T>(what);
        if (queryGroup == null)
        {
            var whatType = what.GetType();
            var cachedType = TypeCache.Get(whatType);
            if (cachedType.IsClassType() || cachedType.IsAnonymousType())
            {
                var fields = GetAndGuardPrimaryKeyOrIdentityKey(connection, tableName, transaction, whatType);

                if (fields.OneOrDefault() is { } field)
                    queryGroup = WhatToQueryGroup<T>(field, what);
                else
                    queryGroup = WhatToQueryGroup<T>(fields, what);
            }
            else
            {
                var dbFields = GetAndGuardPrimaryKeyOrIdentityKey(connection, tableName, transaction);
                if (dbFields.OneOrDefault() is { } dbField)
                    queryGroup = WhatToQueryGroup<T>(dbField, what);
                else
                    queryGroup = WhatToQueryGroup<T>(dbFields, what);
            }
        }
        return queryGroup;
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="what"></param>
    /// <param name="transaction"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<QueryGroup?> WhatToQueryGroupAsync<T>(this IDbConnection connection,
        string tableName,
        T what,
        IDbTransaction transaction,
        CancellationToken cancellationToken = default) where T : notnull
    {
        if (what == null)
        {
            return null;
        }
        var queryGroup = WhatToQueryGroup<T>(what);
        if (queryGroup == null)
        {
            var whatType = what.GetType();
            var cachedType = TypeCache.Get(whatType);
            if (cachedType.IsClassType() || cachedType.IsAnonymousType())
            {
                var fields = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(connection, tableName, transaction, whatType, cancellationToken).ConfigureAwait(false);
                queryGroup = WhatToQueryGroup<T>(fields, what);
            }
            else
            {
                var dbField = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(connection, tableName, transaction, cancellationToken).ConfigureAwait(false);
                queryGroup = WhatToQueryGroup<T>(dbField, what);
            }
        }
        return queryGroup;
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableName"></param>
    /// <param name="what"></param>
    /// <param name="dbFields"></param>
    /// <returns></returns>
    internal static QueryGroup? WhatToQueryGroup<T>(string tableName,
        T what,
        IEnumerable<DbField> dbFields) where T : notnull
    {
        var key = dbFields?.FirstOrDefault(p => p.IsPrimary == true) ?? dbFields?.FirstOrDefault(p => p.IsIdentity == true);
        if (key == null)
        {
            throw new KeyFieldNotFoundException($"No primary key and identity key found at the table '{tableName}'.");
        }
        else
        {
            return WhatToQueryGroup(key, what);
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="connection"></param>
    /// <param name="what"></param>
    /// <param name="transaction"></param>
    /// <returns></returns>
    internal static QueryGroup? WhatToQueryGroup(Type entityType,
        IDbConnection connection,
        object what,
        IDbTransaction transaction)
    {
        if (what == null)
        {
            return null;
        }

        var queryGroup = WhatToQueryGroup(what);
        if (queryGroup != null)
        {
            return queryGroup;
        }
        var keys = GetAndGuardPrimaryKeyOrIdentityKey(entityType, connection, transaction);

        if (keys.OneOrDefault() is { } key)
        {
            return WhatToQueryGroup(key, what);
        }
        else if (keys is { })
        {
            return WhatToQueryGroup(keys, what);
        }

        return null;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="connection"></param>
    /// <param name="what"></param>
    /// <param name="transaction"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<QueryGroup?> WhatToQueryGroupAsync(Type entityType,
        IDbConnection connection,
        object what,
        IDbTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        if (what == null)
        {
            return null;
        }
        var queryGroup = WhatToQueryGroup(what);
        if (queryGroup != null)
        {
            return queryGroup;
        }
        var key = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(entityType, connection, transaction, cancellationToken).ConfigureAwait(false);
        return WhatToQueryGroup(key, what);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbField"></param>
    /// <param name="what"></param>
    /// <returns></returns>
    internal static QueryGroup? WhatToQueryGroup<T>(DbField dbField,
        T what) where T : notnull
    {
        if (what == null)
        {
            return null;
        }
        var type = typeof(T);
        var properties = PropertyCache.Get(type) ?? type.GetClassProperties();
        if (properties?.GetByMappedName(dbField.Name, StringComparison.OrdinalIgnoreCase) is { } property)
        {
            return WhatToQueryGroup<T>(property.AsField(), what);
        }
        else
        {
            return new QueryGroup(new QueryField(dbField.Name, what));
        }
    }

    internal static QueryGroup? WhatToQueryGroup<T>(IEnumerable<DbField> dbFields,
        T what) where T : notnull
    {
        return new QueryGroup(
            dbFields.Select(x => WhatToQueryGroup<T>(x, what)!).Where(x => x is { }),
            Conjunction.And);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="field"></param>
    /// <param name="what"></param>
    /// <returns></returns>
    internal static QueryGroup WhatToQueryGroup<T>(Field field,
        T what) where T : notnull
    {
        var type = typeof(T);
        if (field == null)
        {
            throw new KeyFieldNotFoundException($"No primary key and identity key found at the type '{type.FullName}'.");
        }
        if (TypeCache.Get(type).IsClassType())
        {
            var classProperty = PropertyCache.Get(typeof(T), field, true);
            return new QueryGroup(classProperty?.PropertyInfo.AsQueryField(what));
        }
        else
        {
            return new QueryGroup(new QueryField(field.Name, what));
        }
    }

    internal static QueryGroup WhatToQueryGroup<T>(IEnumerable<Field> field,
        T what) where T : notnull
    {
        return new QueryGroup(
            field.Select(x => WhatToQueryGroup<T>(x, what)!).Where(x => x is { }),
            Conjunction.And);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="what"></param>
    /// <returns></returns>
    internal static QueryGroup? WhatToQueryGroup<T>(T what)
    {
        if (what == null)
        {
            return null;
        }
        else if (what is QueryField field)
        {
            return ToQueryGroup(field);
        }
        else if (what is IEnumerable<QueryField> fields)
        {
            return ToQueryGroup(fields);
        }
        else if (what is QueryGroup group)
        {
            return group;
        }
        else
        {
            var type = TypeCache.Get(typeof(T)).GetUnderlyingType();
            if (TypeCache.Get(type).IsAnonymousType() || type == StaticType.Object)
            {
                return QueryGroup.Parse(what, false);
            }
        }
        return null;
    }

    #endregion

    #region ToQueryGroup

    /// <summary>
    ///
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    internal static QueryGroup? ToQueryGroup(object obj)
    {
        if (obj is null)
        {
            return null;
        }
        var type = obj.GetType();
        if (TypeCache.Get(type).IsClassType())
        {
            return QueryGroup.Parse(obj, true);
        }
        else
        {
            throw new Exceptions.InvalidExpressionException("Only dynamic object is supported in the 'where' expression.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbField"></param>
    /// <param name="entity"></param>
    /// <returns></returns>
    internal static QueryGroup? ToQueryGroup(DbField dbField,
        object entity)
    {
        if (entity == null)
        {
            return null;
        }
        if (dbField != null)
        {
            var type = entity.GetType();
            if (TypeCache.Get(type).IsClassType())
            {
                var properties = PropertyCache.Get(type) ?? type.GetClassProperties();
                if (properties?.GetByMappedName(dbField.Name) is { } property)
                {
                    return new QueryGroup(property.PropertyInfo.AsQueryField(entity));
                }
            }
            else
            {
                return new QueryGroup(new QueryField(dbField.Name, entity));
            }
        }
        throw new KeyFieldNotFoundException($"No primary key and identity key found.");
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="where"></param>
    /// <returns></returns>
    internal static QueryGroup? ToQueryGroup<TEntity>(this IDbConnection connection, Expression<Func<TEntity, bool>> where, IDbTransaction? transaction, string? tableName = null)
        where TEntity : class
    {
        if (where == null)
        {
            return null;
        }
        return QueryGroup.Parse<TEntity>(where, connection: connection, transaction: transaction, tableName: tableName);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="field"></param>
    /// <param name="dictionary"></param>
    /// <returns></returns>
    internal static QueryGroup? ToQueryGroup(Field field,
        IDictionary<string, object> dictionary)
    {
        if (!dictionary.TryGetValue(field.Name, out var value))
        {
            throw new MissingFieldsException(new[] { field.Name });
        }
        return ToQueryGroup(new QueryField(field.Name, value));
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="field"></param>
    /// <param name="entity"></param>
    /// <returns></returns>
    internal static QueryGroup? ToQueryGroup<TEntity>(Field field,
        TEntity entity)
        where TEntity : class
    {
        var type = entity?.GetType() ?? typeof(TEntity);
        return TypeCache.Get(type).IsDictionaryStringObject() ? ToQueryGroup(field, (IDictionary<string, object>)entity!)
            : ToQueryGroup<TEntity>(PropertyCache.Get<TEntity>(field, true) ?? PropertyCache.Get(type, field, true), entity!);
    }

    internal static QueryGroup? ToQueryGroup<TEntity>(IEnumerable<Field> fields,
        TEntity entity)
        where TEntity : class
    {
        return new QueryGroup(
            fields.Select(x => ToQueryGroup(x, entity)!).Where(x => x is { }),
            Conjunction.And);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="property"></param>
    /// <param name="entity"></param>
    /// <returns></returns>
    internal static QueryGroup? ToQueryGroup<TEntity>(ClassProperty? property,
        TEntity entity)
        where TEntity : class =>
        ToQueryGroup(property?.PropertyInfo.AsQueryField(entity));

    /// <summary>
    ///
    /// </summary>
    /// <param name="queryField"></param>
    /// <returns></returns>
    internal static QueryGroup? ToQueryGroup(QueryField? queryField)
    {
        if (queryField == null)
        {
            return null;
        }
        return new QueryGroup(queryField);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="queryFields"></param>
    /// <returns></returns>
    internal static QueryGroup? ToQueryGroup(IEnumerable<QueryField> queryFields)
    {
        if (queryFields == null)
        {
            return null;
        }
        return new QueryGroup(queryFields);
    }

    #endregion

    /// <summary>
    ///
    /// </summary>
    /// <param name="entityType"></param>
    /// <returns></returns>
    internal static IEnumerable<Field>? GetPrimaryOrIdentityKey(Type entityType) =>
        entityType != null ? (PrimaryCache.Get(entityType) ?? IdentityCache.Get(entityType))?.AsField()?.AsEnumerable() : null;

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="entities"></param>
    internal static void ThrowIfNullOrEmpty<TEntity>(IEnumerable<TEntity> entities)
        where TEntity : class
    {
        if (entities == null)
        {
            throw new ArgumentNullException(nameof(entities));
        }
        if (entities.Any() == false)
        {
            throw new EmptyException(nameof(entities), "The entities must not be empty.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="transaction"></param>
    internal static void ValidateTransactionConnectionObject(this IDbConnection connection,
        IDbTransaction? transaction)
    {
        if (transaction != null && transaction.Connection != connection)
        {
            throw new InvalidOperationException("The transaction connection object is different from the current connection object.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="command"></param>
    /// <param name="where"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    internal static void WhereToCommandParameters(DbCommand command,
        QueryGroup where,
        Type entityType,
        DbFieldCollection dbFields) =>
        DbCommandExtension.CreateParameters(command, where, null, entityType, dbFields);

    /// <summary>
    ///
    /// </summary>
    /// <param name="entity"></param>
    /// <param name="properties"></param>
    /// <param name="qualifiers"></param>
    /// <returns></returns>
    internal static QueryGroup CreateQueryGroupForUpsert(object entity,
        IEnumerable<ClassProperty> properties,
        IEnumerable<Field> qualifiers)
    {
        var queryFields = new List<QueryField>();
        foreach (var field in qualifiers)
        {
            var property = properties?.FirstOrDefault(
                p => string.Equals(p.GetMappedName(), field.Name, StringComparison.OrdinalIgnoreCase));

            if (property != null)
            {
                queryFields.Add(new QueryField(field.Name, property.PropertyInfo.GetValue(entity)));
            }
        }
        return new QueryGroup(queryFields);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dictionary"></param>
    /// <param name="qualifiers"></param>
    /// <returns></returns>
    internal static QueryGroup CreateQueryGroupForUpsert(IDictionary<string, object> dictionary,
        IEnumerable<Field>? qualifiers = null)
    {
        if (qualifiers?.Any() != true)
        {
            throw new MissingFieldsException();
        }

        var queryFields = new List<QueryField>();

        foreach (var field in qualifiers)
        {
            if (dictionary.TryGetValue(field.Name, out var value))
            {
                queryFields.Add(new QueryField(field.Name, value));
            }
        }

        if (queryFields.Any() != true)
        {
            throw new MissingFieldsException();
        }

        return new QueryGroup(queryFields);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="entities"></param>
    /// <param name="property"></param>
    /// <returns></returns>
    internal static IEnumerable<object> ExtractPropertyValues<TEntity>(IEnumerable<TEntity> entities, Field keyField) where TEntity : class
    {
        var property = PropertyCache.Get(GetEntityType<TEntity>(entities), keyField, true)!;

        return ClassExpression.GetEntitiesPropertyValues<TEntity, object>(entities, property);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="entity"></param>
    /// <returns></returns>
    internal static IEnumerable<Field> GetQualifiedFields<TEntity>(TEntity entity)
        where TEntity : class
    {
        var typeOfEntity = entity?.GetType() ?? typeof(TEntity);

        if (TypeCache.Get(typeOfEntity).IsClassType())
            return FieldCache.Get(typeOfEntity);
        else
            return Field.Parse(entity);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="fields"></param>
    /// <returns></returns>
    internal static IEnumerable<Field>? GetQualifiedFields<TEntity>()
        where TEntity : class
    {
        if (TypeCache.Get(typeof(TEntity)).IsClassType())
            return FieldCache.Get(typeof(TEntity));
        else
            return null;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="parameterName"></param>
    /// <param name="values"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    internal static string ToRawSqlWithArrayParams(string commandText,
        string parameterName,
        IEnumerable<object> values,
        IDbSetting dbSetting)
    {
        // Check for the defined parameter
        if (!commandText.Contains(parameterName, StringComparison.OrdinalIgnoreCase))
        {
            return commandText;
        }

        // Return if there is no values
        if (values?.Any() != true)
        {
            return commandText;
        }

        // Get the variables needed
        var parameters = values.Select((_, index) =>
            string.Concat(parameterName, index.ToString()).AsParameter(dbSetting));

        // Replace the target parameter when used as parameter. (Not as prefix of longer parameter)
        return Regex.Replace(commandText, Regex.Escape(parameterName.AsParameter(dbSetting)) + "\\b", parameters.Join(", "));
    }

    #region CreateDbCommandForExecution

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="transaction"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    internal static DbCommand CreateDbCommandForExecution(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        Type? entityType = null,
        DbFieldCollection? dbFields = null,
        bool skipCommandArrayParametersCheck = true)
    {
        // Validate
        ValidateTransactionConnectionObject(connection, transaction);

        // Open
        connection.EnsureOpen();

        // Call
        return CreateDbCommandForExecutionInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: entityType,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="transaction"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<DbCommand> CreateDbCommandForExecutionAsync(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        Type? entityType = null,
        DbFieldCollection? dbFields = null,
        bool skipCommandArrayParametersCheck = true,
        CancellationToken cancellationToken = default)
    {
        // Validate
        ValidateTransactionConnectionObject(connection, transaction);

        // Open
        await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);

        // Call
        return CreateDbCommandForExecutionInternal(connection: connection,
            commandText: commandText,
            param: param,
            commandType: commandType,
            commandTimeout: commandTimeout,
            transaction: transaction,
            entityType: entityType,
            dbFields: dbFields,
            skipCommandArrayParametersCheck: skipCommandArrayParametersCheck);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="commandType"></param>
    /// <param name="commandTimeout"></param>
    /// <param name="transaction"></param>
    /// <param name="entityType"></param>
    /// <param name="dbFields"></param>
    /// <param name="skipCommandArrayParametersCheck"></param>
    /// <returns></returns>
    private static DbCommand CreateDbCommandForExecutionInternal(this IDbConnection connection,
        string commandText,
        object? param = null,
        CommandType commandType = default,
        int commandTimeout = 0,
        IDbTransaction? transaction = null,
        Type? entityType = null,
        DbFieldCollection? dbFields = null,
        bool skipCommandArrayParametersCheck = true)
    {
        // Command
        var command = connection
            .CreateCommand(commandText, commandType, commandTimeout, transaction);

        // Func
        if (param != null)
        {
            var func = FunctionCache.GetPlainTypeToDbParametersCompiledFunction(param.GetType(), entityType, dbFields);
            if (func != null)
            {
                var cmd = (DbCommand)command;
                func(cmd, param);
                return cmd;
            }
        }

        // ArrayParameters
        CommandArrayParametersText? commandArrayParametersText = null;
        if (param != null && skipCommandArrayParametersCheck == false)
        {
            commandArrayParametersText = GetCommandArrayParametersText(commandText,
               param,
               DbSettingMapper.Get(connection));
        }

        // Check
        if (commandArrayParametersText != null)
        {
            // CommandText
            command.CommandText = commandArrayParametersText.CommandText;

            // Array parameters
            command.CreateParametersFromArray(commandArrayParametersText);
        }

        // Normal parameters
        if (param != null)
        {
            var propertiesToSkip = commandArrayParametersText?.CommandArrayParameters?
                .Select(cap => cap.ParameterName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            command.CreateParameters(param, propertiesToSkip, entityType, dbFields);
        }

        // Return the command
        return (DbCommand)command;
    }

    #endregion

    #region GetCommandArrayParameters

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    internal static CommandArrayParametersText? GetCommandArrayParametersText(string commandText,
        object param,
        IDbSetting dbSetting)
    {
        if (param == null)
        {
            return null;
        }

        // ExpandoObject
        if (param is IDictionary<string, object> objects)
        {
            return GetCommandArrayParametersText(commandText, objects, dbSetting);
        }

        // QueryField
        else if (param is QueryField field)
        {
            return GetCommandArrayParametersText(commandText, field, dbSetting);
        }

        // QueryFields
        else if (param is IEnumerable<QueryField> fields)
        {
            return GetCommandArrayParametersText(commandText, fields, dbSetting);
        }

        // QueryGroup
        else if (param is QueryGroup group)
        {
            return GetCommandArrayParametersText(commandText, group, dbSetting);
        }

        // Others
        else
        {
            return GetCommandArrayParametersTextInternal(commandText, param, dbSetting);
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="param"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static CommandArrayParametersText? GetCommandArrayParametersTextInternal(string commandText,
        object param,
        IDbSetting dbSetting)
    {
        if (param == null)
        {
            return null;
        }

        // Variables
        CommandArrayParametersText? commandArrayParametersText = null;

        // CommandArrayParameters
        foreach (var property in TypeCache.Get(param.GetType()).GetProperties())
        {
            var propertyHandler = PropertyHandlerCache.Get<object>(property.DeclaringType!, property);
            if (propertyHandler != null ||
                property.PropertyType == StaticType.String ||
                StaticType.IEnumerable.IsAssignableFrom(property.PropertyType) == false)
            {
                continue;
            }

            // Get
            var commandArrayParameter = GetCommandArrayParameter(
                property.Name,
                property.GetValue(param));

            // Skip
            if (commandArrayParameter == null)
            {
                continue;
            }

            // Create
            commandArrayParametersText ??= new CommandArrayParametersText();

            // CommandText
            commandText = GetRawSqlText(commandText,
                property.Name,
                commandArrayParameter.Values,
                dbSetting);

            // Add
            commandArrayParametersText.CommandArrayParameters.Add(commandArrayParameter);
        }

        // CommandText
        if (commandArrayParametersText != null)
        {
            commandArrayParametersText.CommandText = commandText;
        }

        // Return
        return commandArrayParametersText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="dictionary"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static CommandArrayParametersText? GetCommandArrayParametersText(string commandText,
        IDictionary<string, object> dictionary,
        IDbSetting dbSetting)
    {
        if (dictionary == null)
        {
            return null;
        }

        // Variables
        CommandArrayParametersText? commandArrayParametersText = null;

        // CommandArrayParameters
        foreach (var kvp in dictionary)
        {
            // Get
            var commandArrayParameter = GetCommandArrayParameter(
                kvp.Key,
                kvp.Value);

            // Skip
            if (commandArrayParameter == null)
            {
                continue;
            }

            // Create
            commandArrayParametersText ??= new CommandArrayParametersText();

            // CommandText
            commandText = GetRawSqlText(commandText,
                kvp.Key,
                commandArrayParameter.Values,
                dbSetting);

            // Add
            commandArrayParametersText.CommandArrayParameters.Add(commandArrayParameter);
        }

        // CommandText
        if (commandArrayParametersText != null)
        {
            commandArrayParametersText.CommandText = commandText;
        }

        // Return
        return commandArrayParametersText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="queryField"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static CommandArrayParametersText? GetCommandArrayParametersText(string commandText,
        QueryField queryField,
        IDbSetting dbSetting)
    {
        if (queryField == null)
        {
            return null;
        }

        // Skip
        if (IsPreConstructed(commandText, queryField))
        {
            return null;
        }

        // Get
        var commandArrayParameter = GetCommandArrayParameter(
            queryField.Field.Name,
            queryField.Parameter.Value);

        // Check
        if (commandArrayParameter == null)
        {
            return null;
        }

        // Create
        var commandArrayParametersText = new CommandArrayParametersText
        {
            CommandText = GetRawSqlText(commandText,
                queryField.Field.Name,
                commandArrayParameter.Values,
                dbSetting),
            DbType = queryField.Parameter.DbType
        };

        // CommandArrayParameters
        commandArrayParametersText.CommandArrayParameters.Add(commandArrayParameter);

        // Return
        return commandArrayParametersText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="queryFields"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static CommandArrayParametersText? GetCommandArrayParametersText(string commandText,
        IEnumerable<QueryField> queryFields,
        IDbSetting dbSetting)
    {
        if (queryFields == null)
        {
            return null;
        }

        // Variables
        CommandArrayParametersText? commandArrayParametersText = null;

        // CommandArrayParameters
        foreach (var queryField in queryFields)
        {
            // Skip
            if (IsPreConstructed(commandText, queryField))
            {
                continue;
            }

            // Get
            var commandArrayParameter = GetCommandArrayParameter(
                queryField.Field.Name,
                queryField.Parameter.Value);

            // Skip
            if (commandArrayParameter == null)
            {
                continue;
            }

            // Create
            commandArrayParametersText ??= new CommandArrayParametersText()
            {
                // TODO: First element from the array?
                DbType = queryField.Parameter.DbType
            };

            // CommandText
            commandText = GetRawSqlText(commandText,
                queryField.Field.Name,
                commandArrayParameter.Values,
                dbSetting);

            // Add
            commandArrayParametersText.CommandArrayParameters.Add(commandArrayParameter);
        }

        // CommandText
        if (commandArrayParametersText != null)
        {
            commandArrayParametersText.CommandText = commandText;
        }

        // Return
        return commandArrayParametersText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="queryGroup"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static CommandArrayParametersText? GetCommandArrayParametersText(string commandText,
        QueryGroup queryGroup,
        IDbSetting dbSetting) =>
        GetCommandArrayParametersText(commandText, queryGroup.GetFields(true), dbSetting);

    /// <summary>
    ///
    /// </summary>
    /// <param name="parameterName"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    private static CommandArrayParameter? GetCommandArrayParameter(string parameterName,
        object? value)
    {
        var valueType = value?.GetType();
        var propertyHandler = valueType != null ? PropertyHandlerCache.Get<object>(valueType) : null;

        if (value == null ||
            propertyHandler != null ||
            value is string ||
            value is IEnumerable values == false)
        {
            return null;
        }

        // Return
        return new CommandArrayParameter(parameterName, values.WithType<object>());
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="parameterName"></param>
    /// <param name="values"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    internal static string GetRawSqlText(string commandText,
        string parameterName,
        IEnumerable values,
        IDbSetting dbSetting)
    {
        if (!commandText.Contains(parameterName, StringComparison.OrdinalIgnoreCase))
        {
            return commandText;
        }

        // Items
        var items = values.WithType<object>();
        if (items.Any() != true)
        {
            var parameter = parameterName.AsParameter(dbSetting);
            return commandText.Replace(parameter, string.Concat("(SELECT ", parameter, " WHERE 1 = 0)"));
        }

        // Get the variables needed
        var parameters = items.Select((_, index) =>
            string.Concat(parameterName, index.ToString(CultureInfo.InvariantCulture)).AsParameter(dbSetting));

        // Replace the target parameter when used as parameter. (Not as prefix of longer parameter)
        return Regex.Replace(commandText, Regex.Escape(parameterName.AsParameter(dbSetting)) + "\\b", parameters.Join(", "));
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandText"></param>
    /// <param name="queryField"></param>
    /// <returns></returns>
    private static bool IsPreConstructed(string commandText,
        QueryField queryField)
    {
        // Check the IN operation parameters
        if (queryField.Operation == Operation.In || queryField.Operation == Operation.NotIn)
        {
            if (commandText.IndexOf(string.Concat(queryField.Parameter.Name, "_In_"), StringComparison.OrdinalIgnoreCase) > 0)
            {
                return true;
            }
        }

        // Check the BETWEEN operation parameters
        else if (queryField.Operation == Operation.Between || queryField.Operation == Operation.NotBetween)
        {
            if (commandText.IndexOf(string.Concat(queryField.Parameter.Name, "_Left"), StringComparison.OrdinalIgnoreCase) > 0)
            {
                return true;
            }
        }

        // Return
        return false;
    }

    #endregion

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="entities"></param>
    /// <returns></returns>
    internal static string GetMappedName<TEntity>(IEnumerable<TEntity>? entities)
        where TEntity : class =>
        GetMappedName<TEntity>(entities?.FirstOrDefault());

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="entity"></param>
    /// <returns></returns>
    internal static string GetMappedName<TEntity>(TEntity? entity)
        where TEntity : class =>
        entity != null ? ClassMappedNameCache.Get(entity.GetType()) : ClassMappedNameCache.Get<TEntity>();

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="entities"></param>
    /// <returns></returns>
    internal static Type GetEntityType<TEntity>(IEnumerable<TEntity>? entities)
        where TEntity : class =>
        GetEntityType<TEntity>(entities?.FirstOrDefault());

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="entity"></param>
    /// <returns></returns>
    internal static Type GetEntityType<TEntity>(TEntity? entity)
        where TEntity : class =>
        entity?.GetType() ?? typeof(TEntity);

    #endregion
}
