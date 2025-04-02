using System.Data;
using RepoDb.Enumerations;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Requests;

namespace RepoDb;

/// <summary>
/// Contains the extension methods for <see cref="IDbConnection"/> object.
/// </summary>
public static partial class DbConnectionExtension
{
    /*
     * The supposed maximum parameters of 2100 is not working with Microsoft.Data.SqlClient.
     * I reported this issue to SqlClient repository at Github.
     * Link: https://github.com/dotnet/SqlClient/issues/531
     */

    private const int ParameterBatchCount = 2100 - 2;

    #region DeleteAll<TEntity>

    /// <summary>
    /// Delete the rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="entities">The list of data entity objects to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        var key = GetAndGuardPrimaryKeyOrIdentityKey(connection, tableName, transaction, GetEntityType<TEntity>(entities));

        if (key.OneOrDefault() is { } one)
        {
            var keys = ExtractPropertyValues<TEntity>(entities, one).AsList();

            return DeleteAllInternal(connection: connection,
                tableName: tableName,
                keys: keys,
                hints: hints,
                commandTimeout: commandTimeout,
                traceKey: traceKey,
                transaction: transaction,
                trace: trace,
                statementBuilder: statementBuilder);
        }
        else
        {
            int chunkSize = Math.Min(1000, ParameterBatchCount) / key.Count();
            using var myTransaction = transaction is null && chunkSize < entities.Count() ? connection.EnsureOpen().BeginTransaction() : null;
            transaction ??= myTransaction;
            int deleted = 0;

            foreach (var group in entities.Split(chunkSize))
            {
                var where = new QueryGroup(group.Select(entity => ToQueryGroup<TEntity>(key, entity)), Conjunction.Or);

                deleted += DeleteInternal(
                    connection: connection,
                    tableName: tableName,
                    where: where,
                    hints: hints,
                    commandTimeout: commandTimeout,
                    traceKey: traceKey,
                    transaction: transaction,
                    trace: trace,
                    statementBuilder: statementBuilder);
            }

            myTransaction?.Commit();
            return deleted;
        }
    }

    /// <summary>
    /// Delete the rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TKey">The type of the key column.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll<TEntity, TKey>(this IDbConnection connection,
        string tableName,
        IEnumerable<TKey> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return DeleteAllInternal(connection: connection,
            tableName: tableName,
            keys: keys?.WithType<object>(),
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Delete the rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<object> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return DeleteAllInternal(connection: connection,
            tableName: tableName,
            keys: keys,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Delete the rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entities">The list of data entity objects to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll<TEntity>(this IDbConnection connection,
        IEnumerable<TEntity> entities,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        var key = GetAndGuardPrimaryKeyOrIdentityKey(GetEntityType<TEntity>(entities), connection, transaction);
        if (key.OneOrDefault() is { } one)
        {
            var keys = ExtractPropertyValues<TEntity>(entities, one).AsList();

            return DeleteAllInternal(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>(),
            keys: keys?.WithType<object>(),
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
        }
        else
        {
            int chunkSize = Math.Min(1000, ParameterBatchCount) / key.Count();
            using var myTransaction = transaction is null && chunkSize < entities.Count() ? connection.EnsureOpen().BeginTransaction() : null;
            transaction ??= myTransaction;
            int deleted = 0;

            foreach (var group in entities.Split(chunkSize))
            {
                var where = new QueryGroup(group.Select(entity => ToQueryGroup<TEntity>(key, entity)), Conjunction.Or);

                deleted += DeleteInternal(
                    connection: connection,
                    tableName: ClassMappedNameCache.Get<TEntity>(),
                    where: where,
                    hints: hints,
                    commandTimeout: commandTimeout,
                    traceKey: traceKey,
                    transaction: transaction,
                    trace: trace,
                    statementBuilder: statementBuilder);
            }

            myTransaction?.Commit();
            return deleted;
        }
    }

    /// <summary>
    /// Delete the rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TKey">The type of the key column.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll<TEntity, TKey>(this IDbConnection connection,
        IEnumerable<TKey> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return DeleteAllInternal(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>(),
            keys: keys?.WithType<object>(),
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Delete the rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll<TEntity>(this IDbConnection connection,
        IEnumerable<object> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return DeleteAllInternal(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>(),
            keys: keys,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Delete all the rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll<TEntity>(this IDbConnection connection,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return DeleteAllInternal<TEntity>(connection: connection,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Delete all the rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    internal static int DeleteAllInternal<TEntity>(this IDbConnection connection,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        // Variables
        var request = new DeleteAllRequest(typeof(TEntity),
            connection,
            transaction,
            hints,
            statementBuilder);

        // Return the result
        return DeleteAllInternalBase(connection: connection,
            request: request,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace);
    }

    #endregion

    #region DeleteAllAsync<TEntity>

    /// <summary>
    /// Delete the rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="entities">The list of data entity objects to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static async Task<int> DeleteAllAsync<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        var key = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(connection, tableName, transaction, GetEntityType(entities), cancellationToken).ConfigureAwait(false);
        if (key.OneOrDefault() is { } one)
        {
            var keys = ExtractPropertyValues<TEntity>(entities, one).AsList();

            return await DeleteAllAsyncInternal(connection: connection,
            tableName: tableName,
            keys: keys,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        else
        {
            int chunkSize = Math.Min(1000, ParameterBatchCount) / key.Count();

            await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
            using var myTransaction = transaction is null && chunkSize < entities.Count() ? await connection.BeginTransactionAsync(cancellationToken) : null;

            transaction ??= myTransaction;
            int deleted = 0;

            foreach (var group in entities.Split(chunkSize))
            {
                var where = new QueryGroup(group.Select(entity => ToQueryGroup<TEntity>(key, entity)), Conjunction.Or);

                deleted += await DeleteAsyncInternal(
                    connection: connection,
                    tableName: tableName,
                    where: where,
                    hints: hints,
                    commandTimeout: commandTimeout,
                    traceKey: traceKey,
                    transaction: transaction,
                    trace: trace,
                    statementBuilder: statementBuilder);
            }

            myTransaction?.Commit();
            return deleted;
        }
    }

    /// <summary>
    /// Delete the rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TKey">The type of the key column.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static Task<int> DeleteAllAsync<TEntity, TKey>(this IDbConnection connection,
        string tableName,
        IEnumerable<TKey> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return DeleteAllAsyncInternal(connection: connection,
            tableName: tableName,
            keys: keys?.WithType<object>(),
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Delete the rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static Task<int> DeleteAllAsync<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<object> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return DeleteAllAsyncInternal(connection: connection,
            tableName: tableName,
            keys: keys,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entities">The list of data entity objects to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static async Task<int> DeleteAllAsync<TEntity>(this IDbConnection connection,
        IEnumerable<TEntity> entities,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        var key = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(GetEntityType(entities), connection, transaction, cancellationToken).ConfigureAwait(false);
        if (key.OneOrDefault() is { } one)
        {
            var keys = ExtractPropertyValues<TEntity>(entities, one).AsList();

            return await DeleteAllAsyncInternal(connection: connection,
                tableName: ClassMappedNameCache.Get<TEntity>(),
                keys: keys?.WithType<object>(),
                hints: hints,
                commandTimeout: commandTimeout,
                traceKey: traceKey,
                transaction: transaction,
                trace: trace,
                statementBuilder: statementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        else
        {
            int chunkSize = Math.Min(1000, ParameterBatchCount) / key.Count();

            await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
            using var myTransaction = transaction is null && chunkSize < entities.Count() ? await connection.BeginTransactionAsync(cancellationToken) : null;

            transaction ??= myTransaction;
            int deleted = 0;

            foreach (var group in entities.Split(chunkSize))
            {
                var where = new QueryGroup(group.Select(entity => ToQueryGroup<TEntity>(key, entity)), Conjunction.Or);

                deleted += await DeleteAsyncInternal(
                    connection: connection,
                    tableName: ClassMappedNameCache.Get<TEntity>(),
                    where: where,
                    hints: hints,
                    commandTimeout: commandTimeout,
                    traceKey: traceKey,
                    transaction: transaction,
                    trace: trace,
                    statementBuilder: statementBuilder);
            }

            myTransaction?.Commit();
            return deleted;
        }
    }

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TKey">The type of the key column.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static Task<int> DeleteAllAsync<TEntity, TKey>(this IDbConnection connection,
        IEnumerable<TKey> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return DeleteAllAsyncInternal(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>(),
            keys: keys?.WithType<object>(),
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static Task<int> DeleteAllAsync<TEntity>(this IDbConnection connection,
        IEnumerable<object> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return DeleteAllAsyncInternal(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>(),
            keys: keys,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static Task<int> DeleteAllAsync<TEntity>(this IDbConnection connection,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return DeleteAllAsyncInternal<TEntity>(connection: connection,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    internal static Task<int> DeleteAllAsyncInternal<TEntity>(this IDbConnection connection,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Variables
        var request = new DeleteAllRequest(typeof(TEntity),
            connection,
            transaction,
            hints,
            statementBuilder);

        // Return the result
        return DeleteAllAsyncInternalBase(connection: connection,
            request: request,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            cancellationToken: cancellationToken);
    }

    #endregion

    #region DeleteAll(TableName)

    /// <summary>
    /// Delete the rows from the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll(this IDbConnection connection,
        string tableName,
        IEnumerable<object> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        return DeleteAllInternal(connection: connection,
            tableName: tableName,
            keys: keys,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Delete all the rows from the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static int DeleteAll(this IDbConnection connection,
        string tableName,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        return DeleteAllInternal(connection: connection,
            tableName: tableName,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Delete all the rows from the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    internal static int DeleteAllInternal(this IDbConnection connection,
        string tableName,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        // Variables
        var request = new DeleteAllRequest(tableName,
            connection,
            transaction,
            hints,
            statementBuilder);

        // Return the result
        return DeleteAllInternalBase(connection: connection,
            request: request,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace);
    }

    /// <summary>
    /// Delete the rows from the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    internal static int DeleteAllInternal(this IDbConnection connection,
        string tableName,
        IEnumerable<object> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        var pkeys = GetAndGuardPrimaryKeyOrIdentityKey(connection, tableName, transaction);
        var dbSetting = connection.GetDbSetting();
        var hasImplicitTransaction = false;
        var count = keys?.AsList()?.Count;
        var deletedRows = 0;

        try
        {
            // Creates a transaction (if needed)
            if (transaction == null && count > ParameterBatchCount)
            {
                transaction = connection.EnsureOpen().BeginTransaction();
                hasImplicitTransaction = true;
            }

            // Call the underlying method
            var splitted = keys?.Split(ParameterBatchCount).AsList();
            if (splitted?.Any() == true)
            {
                foreach (var keyValues in splitted)
                {
                    if (keyValues.Any() != true)
                    {
                        break;
                    }
                    var where = new QueryGroup(
                        pkeys.Select(key => new QueryGroup(new QueryField(key.Name.AsQuoted(dbSetting), Operation.In, keyValues.AsList(), null, false))),
                        Conjunction.And);
                    deletedRows += DeleteInternal(connection: connection,
                        tableName: tableName,
                        where: where,
                        hints: hints,
                        commandTimeout: commandTimeout,
            traceKey: traceKey,
                        transaction: transaction,
                        trace: trace,
                        statementBuilder: statementBuilder);
                }
            }

            // Commit the transaction
            if (hasImplicitTransaction)
            {
                transaction?.Commit();
            }

        }
        finally
        {
            // Dispose the transaction
            if (hasImplicitTransaction)
            {
                transaction?.Dispose();
            }
        }

        // Return the value
        return deletedRows;
    }

    #endregion

    #region DeleteAllAsync(TableName)

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static Task<int> DeleteAllAsync(this IDbConnection connection,
        string tableName,
        IEnumerable<object> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        return DeleteAllAsyncInternal(connection: connection,
            tableName: tableName,
            keys: keys,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static Task<int> DeleteAllAsync(this IDbConnection connection,
        string tableName,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        return DeleteAllAsyncInternal(connection: connection,
            tableName: tableName,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    internal static Task<int> DeleteAllAsyncInternal(this IDbConnection connection,
        string tableName,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        // Variables
        var request = new DeleteAllRequest(tableName,
            connection,
            transaction,
            hints,
            statementBuilder);

        // Return the result
        return DeleteAllAsyncInternalBase(connection: connection,
            request: request,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Delete all the rows from the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="keys">The list of the keys to be deleted.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    public static async Task<int> DeleteAllAsyncInternal(this IDbConnection connection,
        string tableName,
        IEnumerable<object> keys,
        string? hints = null,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        var pkeys = await GetAndGuardPrimaryKeyOrIdentityKeyAsync(connection, tableName, transaction, cancellationToken).ConfigureAwait(false);
        var dbSetting = connection.GetDbSetting();
        var hasImplicitTransaction = false;
        var count = keys?.AsList()?.Count;
        var deletedRows = 0;

        try
        {
            // Creates a transaction (if needed)
            if (transaction == null && count > ParameterBatchCount)
            {
                transaction = (await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false)).BeginTransaction();
                hasImplicitTransaction = true;
            }

            // Call the underlying method
            var splitted = keys?.Split(ParameterBatchCount).AsList();
            if (splitted?.Any() == true)
            {
                foreach (var keyValues in splitted)
                {
                    if (keyValues.Any() != true)
                    {
                        break;
                    }
                    var where = new QueryGroup(
                        pkeys.Select(key => new QueryGroup(new QueryField(key.Name.AsQuoted(dbSetting), Operation.In, keyValues.AsList(), null, false))),
                        Conjunction.And);
                    deletedRows += await DeleteAsyncInternal(connection: connection,
                        tableName: tableName,
                        where: where,
                        hints: hints,
                        commandTimeout: commandTimeout,
            traceKey: traceKey,
                        transaction: transaction,
                        trace: trace,
                        statementBuilder: statementBuilder,
                        cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            }

            // Commit the transaction
            if (hasImplicitTransaction)
            {
                transaction?.Commit();
            }

        }
        finally
        {
            // Dispose the transaction
            if (hasImplicitTransaction)
            {
                transaction?.Dispose();
            }
        }

        // Return the value
        return deletedRows;
    }

    #endregion

    #region DeleteAllInternalBase

    /// <summary>
    /// Delete all the rows from the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="request">The actual <see cref="DeleteAllRequest"/> object.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    internal static int DeleteAllInternalBase(this IDbConnection connection,
        DeleteAllRequest request,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null)
    {
        // Variables
        var commandType = CommandType.Text;
        var commandText = CommandTextCache.GetDeleteAllText(request);

        // Actual Execution
        var result = ExecuteNonQueryInternal(connection: connection,
            commandText: commandText,
            param: null,
            commandType: commandType,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            entityType: request.Type,
            dbFields: DbFieldCache.Get(connection, request.Name, transaction, true),
            skipCommandArrayParametersCheck: true);

        // Result
        return result;
    }

    #endregion

    #region DeleteAllAsyncInternalBase

    /// <summary>
    /// Delete all the rows from the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="request">The actual <see cref="DeleteAllRequest"/> object.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the table.</returns>
    internal static async Task<int> DeleteAllAsyncInternalBase(this IDbConnection connection,
        DeleteAllRequest request,
        int? commandTimeout = null,
        string traceKey = TraceKeys.DeleteAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        CancellationToken cancellationToken = default)
    {
        // Variables
        var commandType = CommandType.Text;
        var commandText = CommandTextCache.GetDeleteAllText(request);

        // Actual Execution
        var result = await ExecuteNonQueryAsyncInternal(connection: connection,
            commandText: commandText,
            param: null,
            commandType: commandType,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            entityType: request.Type,
            dbFields: await DbFieldCache.GetAsync(connection, request.Name, transaction, true, cancellationToken).ConfigureAwait(false),
            skipCommandArrayParametersCheck: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // Result
        return result;
    }

    #endregion
}
