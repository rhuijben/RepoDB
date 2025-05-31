#nullable enable
using System.Data;
using System.Data.Common;
using System.Transactions;
using RepoDb.Contexts.Providers;
using RepoDb.DbSettings;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb;

/// <summary>
/// Contains the extension methods for <see cref="IDbConnection"/> object.
/// </summary>
public static partial class DbConnectionExtension
{
    #region InsertAll<TEntity>

    /// <summary>
    /// Insert multiple rows in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity objects.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entities">The list of data entity objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    public static int InsertAll<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        int batchSize = 0,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return InsertAllInternal(connection: connection,
            tableName: tableName,
            entities: entities,
            batchSize: batchSize,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Insert multiple rows in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity objects.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entities">The list of data entity objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    public static int InsertAll<TEntity>(this IDbConnection connection,
        IEnumerable<TEntity> entities,
        int batchSize = 0,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return InsertAllInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entities),
            entities: entities,
            batchSize: batchSize,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Insert multiple rows in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entities">The list of data entity objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    internal static int InsertAllInternal<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        int batchSize = 0,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        if (TypeCache.Get(GetEntityType(entities)).IsDictionaryStringObject())
        {
            return InsertAllInternalBase<IDictionary<string, object>>(connection: connection,
                tableName: tableName,
                entities: entities.WithType<IDictionary<string, object>>(),
                batchSize: batchSize,
                fields: fields ?? GetQualifiedFields<TEntity>(entities?.FirstOrDefault()),
                hints: hints,
                commandTimeout: commandTimeout,
                traceKey: traceKey,
                transaction: transaction,
                trace: trace,
                statementBuilder: statementBuilder);
        }
        else
        {
            return InsertAllInternalBase<TEntity>(connection: connection,
                tableName: tableName,
                entities: entities,
                batchSize: batchSize,
                fields: fields ?? GetQualifiedFields<TEntity>(entities?.FirstOrDefault()),
                hints: hints,
                commandTimeout: commandTimeout,
                traceKey: traceKey,
                transaction: transaction,
                trace: trace,
                statementBuilder: statementBuilder);
        }
    }

    #endregion

    #region InsertAllAsync<TEntity>

    /// <summary>
    /// Insert multiple rows in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity objects.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entities">The list of data entity objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    public static async Task<int> InsertAllAsync<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        int batchSize = 0,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await InsertAllAsyncInternal(connection: connection,
            tableName: tableName,
            entities: entities,
            batchSize: batchSize,
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
    /// Insert multiple rows in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="entities">The list of data entity objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    public static async Task<int> InsertAllAsync<TEntity>(this IDbConnection connection,
        IEnumerable<TEntity> entities,
        int batchSize = 0,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await InsertAllAsyncInternal<TEntity>(connection: connection,
            tableName: GetMappedName(entities),
            entities: entities,
            batchSize: batchSize,
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
    /// Insert multiple rows in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entities">The list of data entity objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    internal static ValueTask<int> InsertAllAsyncInternal<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        int batchSize = 0,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        if (TypeCache.Get(GetEntityType(entities)).IsDictionaryStringObject())
        {
            return InsertAllAsyncInternalBase<IDictionary<string, object>>(connection: connection,
                tableName: tableName,
                entities: entities.WithType<IDictionary<string, object>>(),
                batchSize: batchSize,
                fields: fields ?? GetQualifiedFields<TEntity>(entities?.FirstOrDefault()),
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
            return InsertAllAsyncInternalBase<TEntity>(connection: connection,
                tableName: tableName,
                entities: entities,
                batchSize: batchSize,
                fields: fields ?? GetQualifiedFields<TEntity>(entities?.FirstOrDefault()),
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

    #region InsertAll(TableName)

    /// <summary>
    /// Insert multiple rows in the table. By default, the table fields are used unless the 'fields' argument is defined.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entities">The list of dynamic objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    public static int InsertAll(this IDbConnection connection,
        string tableName,
        IEnumerable<object> entities,
        int batchSize = 0,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        return InsertAllInternal<object>(connection: connection,
            tableName: tableName,
            entities: entities,
            batchSize: batchSize,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    #endregion

    #region InsertAllAsync(TableName)

    /// <summary>
    /// Insert multiple rows in the table in an asynchronous way. By default, the table fields are used unless the 'fields' argument is defined.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entities">The list of dynamic objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    public static async Task<int> InsertAllAsync(this IDbConnection connection,
        string tableName,
        IEnumerable<object> entities,
        int batchSize = 0,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        return await InsertAllAsyncInternal<object>(connection: connection,
            tableName: tableName,
            entities: entities,
            batchSize: batchSize,
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

    #region InsertAllInternalBase<TEntity>

    /// <summary>
    /// Insert multiple rows in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the object (whether a data entity or a dynamic).</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entities">The list of data entity or dynamic objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    internal static int InsertAllInternalBase<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        int batchSize,
        IEnumerable<Field> fields,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        // Variables needed
        var dbSetting = connection.GetDbSetting();

        // Guard the parameters
        if (entities?.Any() != true)
        {
            return default;
        }

        // Validate the batch size
        batchSize = (dbSetting.IsMultiStatementExecutable == true)
            ? Math.Min(batchSize <= 0 ? dbSetting.ParameterBatchCount / fields.Count() : batchSize, entities.Count())
            : 1;

        // Get the context
        var entityType = GetEntityType<TEntity>(entities);
        var context = InsertAllExecutionContextProvider.Create(entityType,
            connection,
            tableName,
            batchSize,
            fields,
            hints,
            transaction,
            statementBuilder);
        var result = 0;

        connection.EnsureOpen();

        using var myTransaction = (transaction is null && Transaction.Current is null) ? connection.BeginTransaction() : null;
        transaction ??= myTransaction;

        // Create the command
        using (var command = (DbCommand)connection.CreateCommand(context.CommandText,
            CommandType.Text, commandTimeout, transaction))
        {
            // Directly execute if the entities is only 1 (performance)
            if (context.BatchSize == 1)
            {
                BaseDbHelper? dbh = null;

                foreach (var entity in entities.AsList())
                {
                    // Set the values
                    context.SingleDataEntityParametersSetterFunc?.Invoke(command, entity);

                    var fetchIdentity = (dbh ??= (BaseDbHelper)GetDbHelper(connection!)).PrepareForIdentityOutput(command);
                    // Prepare the command
                    if (dbSetting.IsPreparable)
                    {
                        command.Prepare();
                    }

                    // Before Execution
                    var traceResult = Tracer
                        .InvokeBeforeExecution(traceKey, trace, command);

                    // Silent cancellation
                    if (traceResult?.CancellableTraceLog?.IsCancelled == true)
                    {
                        return result;
                    }

                    // Actual Execution
                    object? returnValue;

                    if (fetchIdentity is not { })
                        returnValue = Converter.DbNullToNull(command.ExecuteScalar());
                    else
                    {
                        command.ExecuteNonQuery();

                        returnValue = fetchIdentity();
                    }

                    // After Execution
                    Tracer
                        .InvokeAfterExecution(traceResult, trace, result);

                    // Set the return value
                    if (returnValue != null)
                    {
                        context.IdentitySetterFunc?.Invoke(entity, returnValue);
                    }

                    // Iterate the result
                    result++;
                }
            }
            else
            {
                BaseDbHelper? dbh = null;
                int? positionIndex = null;

                foreach (var batchEntities in entities.ChunkOptimally(batchSize))
                {
                    var batchItems = batchEntities.AsList();

                    // Break if there is no more records
                    if (batchItems.Count <= 0)
                    {
                        break;
                    }

                    // Check if the batch size has changed (probably the last batch on the enumerables)
                    if (batchItems.Count != batchSize)
                    {
                        // Get a new execution context from cache
                        context = InsertAllExecutionContextProvider.Create(entityType,
                            connection,
                            tableName,
                            batchItems.Count,
                            fields,
                            hints,
                            transaction,
                            statementBuilder);

                        // Set the command properties
                        command.CommandText = context.CommandText;
                    }

                    // Set the values
                    if (batchItems.Count == 1)
                    {
                        context.SingleDataEntityParametersSetterFunc?.Invoke(command, batchItems.First());
                    }
                    else
                    {
                        context.MultipleDataEntitiesParametersSetterFunc?.Invoke(command, batchItems.OfType<object>().AsList());
                        AddOrderColumnParameters(command, batchItems);
                    }

                    var fetchIdentity = (dbh ??= (BaseDbHelper)GetDbHelper(command.Connection!)).PrepareForIdentityOutput(command);

                    // Prepare the command
                    if (dbSetting.IsPreparable)
                    {
                        command.Prepare();
                    }

                    // Actual Execution
                    if (context.IdentitySetterFunc == null || fetchIdentity is { })
                    {
                        // Before Execution
                        var traceResult = Tracer
                            .InvokeBeforeExecution(traceKey, trace, command);

                        // Silent cancellation
                        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
                        {
                            return result;
                        }

                        // No identity setters
                        result += command.ExecuteNonQuery();

                        if (context.IdentitySetterFunc is { } && fetchIdentity is { })
                        {
                            var position = 0;

                            foreach (var value in fetchIdentity() as System.Collections.IEnumerable ?? Array.Empty<object>())
                            {
                                context.IdentitySetterFunc.Invoke(batchItems[position++], value);
                            }
                        }

                        // After Execution
                        Tracer
                            .InvokeAfterExecution(traceResult, trace, result);
                    }
                    else
                    {
                        // Before Execution
                        var traceResult = Tracer
                            .InvokeBeforeExecution(traceKey, trace, command);

                        // Set the identity back
                        using var reader = command.ExecuteReader();

                        // Get the results
                        var position = 0;
                        do
                        {
                            while (position < batchItems.Count && reader.Read())
                            {
                                positionIndex ??= (reader.FieldCount > 1) && string.Equals("__RepoDb_OrderColumn", reader.GetName(reader.FieldCount - 1)) ? reader.FieldCount - 1 : -1;

                                var value = Converter.DbNullToNull(reader.GetValue(0));
                                var index = positionIndex >= 0 && positionIndex < reader.FieldCount ? reader.GetInt32(positionIndex.Value) : position;
                                context.IdentitySetterFunc.Invoke(batchItems[index], value);
                                position++;
                            }
                        }
                        while (position < batchItems.Count && reader.NextResult());

                        // Set the result
                        result += batchItems.Count;

                        // After Execution
                        Tracer
                            .InvokeAfterExecution(traceResult, trace, result);
                    }
                }
            }
        }

        myTransaction?.Commit();

        // Return the result
        return result;
    }

    #endregion

    #region InsertAllAsyncInternalBase<TEntity>

    /// <summary>
    /// Insert multiple rows in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the object (whether a data entity or a dynamic).</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="entities">The list of data entity or dynamic objects to be inserted.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of inserted rows in the table.</returns>
    internal static async ValueTask<int> InsertAllAsyncInternalBase<TEntity>(this IDbConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        int batchSize,
        IEnumerable<Field> fields,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.InsertAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Variables needed
        var dbSetting = connection.GetDbSetting();

        // Guard the parameters
        if (entities?.Any() != true)
        {
            return default;
        }

        // Validate the batch size
        batchSize = (dbSetting.IsMultiStatementExecutable == true)
            ? Math.Min(batchSize <= 0 ? dbSetting.ParameterBatchCount / fields.Count() : batchSize, entities.Count())
            : 1;

        await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
        using var myTransaction = (transaction is null && Transaction.Current is null) ? await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false) : null;
        transaction ??= myTransaction;

        // Get the context
        var entityType = GetEntityType<TEntity>(entities);
        var context = await InsertAllExecutionContextProvider.CreateAsync(entityType,
            connection,
            tableName,
            batchSize,
            fields,
            hints,
            transaction,
            statementBuilder,
            cancellationToken).ConfigureAwait(false);
        var result = 0;

        // Create the command
        using (var command = (DbCommand)connection.CreateCommand(context.CommandText,
            CommandType.Text, commandTimeout, transaction))
        {
            // Directly execute if the entities is only 1 (performance)
            if (context.BatchSize == 1)
            {
                BaseDbHelper? dbh = null;
                foreach (var entity in entities.AsList())
                {
                    // Set the values
                    context.SingleDataEntityParametersSetterFunc?.Invoke(command, entity);

                    var fetchIdentity = (dbh ??= (BaseDbHelper)GetDbHelper(command.Connection!)).PrepareForIdentityOutput(command);

                    // Prepare the command
                    if (dbSetting.IsPreparable)
                    {
                        command.Prepare();
                    }

                    // Before Execution
                    var traceResult = await Tracer
                        .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

                    // Silent cancellation
                    if (traceResult?.CancellableTraceLog?.IsCancelled == true)
                    {
                        return result;
                    }

                    // Actual Execution
                    object? returnValue;

                    if (fetchIdentity is not { })
                        returnValue = Converter.DbNullToNull(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                    else
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        returnValue = Converter.DbNullToNull(fetchIdentity());
                    }

                    // After Execution
                    await Tracer
                        .InvokeAfterExecutionAsync(traceResult, trace, result, cancellationToken).ConfigureAwait(false);

                    // Set the return value
                    if (returnValue != null)
                    {
                        context.IdentitySetterFunc?.Invoke(entity, returnValue);
                    }

                    // Iterate the result
                    result++;
                }
            }
            else
            {
                BaseDbHelper? dbh = null;
                int? positionIndex = null;
                foreach (var batchEntities in entities.ChunkOptimally(batchSize))
                {
                    var batchItems = batchEntities.AsList();

                    // Break if there is no more records
                    if (batchItems.Count <= 0)
                    {
                        break;
                    }

                    // Check if the batch size has changed (probably the last batch on the enumerables)
                    if (batchItems.Count != batchSize)
                    {
                        // Get a new execution context from cache
                        context = await InsertAllExecutionContextProvider.CreateAsync(entityType,
                            connection,
                            tableName,
                            batchItems.Count,
                            fields,
                            hints,
                            transaction,
                            statementBuilder,
                            cancellationToken).ConfigureAwait(false);

                        // Set the command properties
                        command.CommandText = context.CommandText;
                    }

                    // Set the values
                    if (batchItems.Count == 1)
                    {
                        context.SingleDataEntityParametersSetterFunc?.Invoke(command, batchItems.First());
                    }
                    else
                    {
                        context.MultipleDataEntitiesParametersSetterFunc?.Invoke(command, batchItems.OfType<object>().AsList());
                        AddOrderColumnParameters<TEntity>(command, batchItems);
                    }

                    // Prepare the command

                    var fetchIdentity = (dbh ??= (BaseDbHelper)GetDbHelper(command.Connection!)).PrepareForIdentityOutput(command);

                    if (dbSetting.IsPreparable)
                    {
                        command.Prepare();
                    }

                    // Actual Execution
                    if (context.IdentitySetterFunc == null || fetchIdentity is { })
                    {
                        // Before Execution
                        var traceResult = await Tracer
                            .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

                        // Silent cancellation
                        if (traceResult?.CancellableTraceLog?.IsCancelled == true)
                        {
                            return result;
                        }

                        // No identity setters
                        result += await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                        if (context.IdentitySetterFunc is { } && fetchIdentity is { })
                        {
                            var position = 0;

                            foreach (var value in fetchIdentity() as System.Collections.IEnumerable ?? Array.Empty<object>())
                            {
                                context.IdentitySetterFunc.Invoke(batchItems[position++], value);
                            }
                        }

                        // After Execution
                        await Tracer
                            .InvokeAfterExecutionAsync(traceResult, trace, result, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        // Before Execution
                        var traceResult = await Tracer
                            .InvokeBeforeExecutionAsync(traceKey, trace, command, cancellationToken).ConfigureAwait(false);

                        // Set the identity back
                        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

                        // Get the results
                        var position = 0;
                        do
                        {
                            while (position < batchItems.Count && await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                            {
                                positionIndex ??= (reader.FieldCount > 1) && string.Equals("__RepoDb_OrderColumn", reader.GetName(reader.FieldCount - 1)) ? reader.FieldCount - 1 : -1;

                                var value = Converter.DbNullToNull(reader.GetValue(0));
                                var index = positionIndex >= 0 && positionIndex < reader.FieldCount ? reader.GetInt32(positionIndex.Value) : position;
                                context.IdentitySetterFunc.Invoke(batchItems[index], value);
                                position++;
                            }
                        }
                        while (position < batchItems.Count && await reader.NextResultAsync(cancellationToken));

                        // Set the result
                        result += batchItems.Count;

                        // After Execution
                        await Tracer
                            .InvokeAfterExecutionAsync(traceResult, trace, result, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }

        if (myTransaction is { })
            await myTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);

        // Return the result
        return result;
    }

    #endregion
}
