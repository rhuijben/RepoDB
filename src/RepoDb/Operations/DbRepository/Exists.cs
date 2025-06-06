﻿using System.Data;
using System.Data.Common;
using System.Linq.Expressions;

namespace RepoDb;

public partial class DbRepository<TDbConnection> : IDisposable
    where TDbConnection : DbConnection, new()
{
    #region Exists<TEntity>

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="what">The dynamic expression or the primary/identity key value to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists<TEntity>(object what,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists<TEntity>(what: what,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TWhat">The type of the expression or the key value.</typeparam>
    /// <param name="what">The dynamic expression or the primary/identity key value to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists<TEntity, TWhat>(TWhat what,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists<TEntity, TWhat>(what: what,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists<TEntity>(Expression<Func<TEntity, bool>> where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists<TEntity>(where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists<TEntity>(QueryField where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists<TEntity>(where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists<TEntity>(IEnumerable<QueryField> where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists<TEntity>(where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists<TEntity>(QueryGroup where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists<TEntity>(where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    #endregion

    #region ExistsAsync<TEntity>

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync<TEntity>(object what,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync<TEntity>(what: what,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <typeparam name="TWhat">The type of the expression or the key value.</typeparam>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync<TEntity, TWhat>(TWhat what,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync<TEntity, TWhat>(what: what,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync<TEntity>(Expression<Func<TEntity, bool>> where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync(where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync<TEntity>(QueryField where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync<TEntity>(where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync<TEntity>(IEnumerable<QueryField> where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync<TEntity>(where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync<TEntity>(QueryGroup where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync<TEntity>(where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    #endregion

    #region Exists(TableName)

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <typeparam name="TWhat">The type of the expression or the key value.</typeparam>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists<TWhat>(string tableName,
        TWhat what,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists<TWhat>(tableName: tableName,
                what: what,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists(string tableName,
        object what,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists(tableName: tableName,
                what: what,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists(string tableName,
        QueryField where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists(tableName: tableName,
                where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists(string tableName,
        IEnumerable<QueryField> where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists(tableName: tableName,
                where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table.
    /// </summary>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public bool Exists(string tableName,
        QueryGroup where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return connection.Exists(tableName: tableName,
                hints: hints,
                where: where,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    #endregion

    #region ExistsAsync(TableName)

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TWhat">The type of the expression or the key value.</typeparam>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync<TWhat>(string tableName,
        TWhat what,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync(tableName: tableName,
                what: what,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="what">The dynamic expression or the key value to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync(string tableName,
        object what,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync(tableName: tableName,
                what: what,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync(string tableName,
        QueryField where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync(tableName: tableName,
                where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync(string tableName,
        IEnumerable<QueryField> where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync(tableName: tableName,
                where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    /// <summary>
    /// Check whether the rows are existing in the table in an asynchronous way.
    /// </summary>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>A boolean value that indicates whether the rows are existing in the table.</returns>
    public async Task<bool> ExistsAsync(string tableName,
        QueryGroup where,
        string? hints = null,
        string? traceKey = TraceKeys.Exists,
        IDbTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        // Create a connection
        var connection = (transaction?.Connection ?? CreateConnection());

        try
        {
            // Call the method
            return await connection.ExistsAsync(tableName: tableName,
                where: where,
                hints: hints,
                commandTimeout: CommandTimeout ?? 0,
                traceKey: traceKey,
                transaction: transaction,
                trace: Trace,
                statementBuilder: StatementBuilder,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Dispose the connection
            DisposeConnectionForPerCall(connection, transaction);
        }
    }

    #endregion
}
