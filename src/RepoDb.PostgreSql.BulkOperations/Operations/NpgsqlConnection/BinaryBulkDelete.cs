﻿using Npgsql;
using RepoDb.Enumerations.PostgreSql;
using RepoDb.PostgreSql.BulkOperations;
using System.Data;
using System.Data.Common;
using System.Dynamic;

namespace RepoDb;

/// <summary>
/// Contains the extension methods for <see cref="NpgsqlConnection"/> object.
/// </summary>
public static partial class NpgsqlConnectionExtension
{
    #region Sync

    #region BinaryBulkDelete<TEntity>

    /// <summary>
    /// Delete the existing rows via entities by bulk. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="entities">The list of entities to be bulk-deleted to the target table.
    /// This can be an <see cref="IEnumerable{T}"/> of the following objects (<typeparamref name="TEntity"/> (as class/model), <see cref="ExpandoObject"/>,
    /// <see cref="IDictionary{TKey, TValue}"/> (of <see cref="string"/>/<see cref="object"/>) and Anonymous Types).</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the entities will be sent together in one-go.</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the entities will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static int BinaryBulkDelete<TEntity>(this NpgsqlConnection connection,
        IEnumerable<TEntity> entities,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction transaction = null)
        where TEntity : class =>
        BinaryBulkDelete<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>(),
            entities: entities,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            batchSize: batchSize,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction);

    /// <summary>
    /// Delete the existing rows via entities by bulk. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="tableName">The name of the target table from the database.</param>
    /// <param name="entities">The list of entities to be bulk-deleted to the target table.
    /// This can be an <see cref="IEnumerable{T}"/> of the following objects (<typeparamref name="TEntity"/> (as class/model), <see cref="ExpandoObject"/>,
    /// <see cref="IDictionary{TKey, TValue}"/> (of <see cref="string"/>/<see cref="object"/>) and Anonymous Types).</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the rows of the table will be sent together in one-go.</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the entities will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static int BinaryBulkDelete<TEntity>(this NpgsqlConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction transaction = null)
        where TEntity : class =>
        BinaryBulkDeleteBase<TEntity>(connection: connection,
            tableName: (tableName ?? ClassMappedNameCache.Get<TEntity>()),
            entities: entities,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            batchSize: batchSize,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction);

    #endregion

    #region BinaryBulkDelete<DataTable>

    /// <summary>
    /// Delete the existing rows via <see cref="DataTable"/> by bulk. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="table">The source <see cref="DataTable"/> object that contains the rows to be bulk-deleted to the target table.</param>
    /// <param name="rowState">The state of the rows to be bulk-deleted. If not specified, all the rows of the table will be used.</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the rows of the table will be sent together in one-go.</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the <see cref="DataTable"/> will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static int BinaryBulkDelete(this NpgsqlConnection connection,
        DataTable table,
        DataRowState? rowState = null,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction transaction = null) =>
        BinaryBulkDelete(connection: connection,
            tableName: table?.TableName,
            table: table,
            rowState: rowState,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            batchSize: batchSize,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction);

    /// <summary>
    /// Delete the existing rows via <see cref="DataTable"/> by bulk. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="tableName">The name of the target table from the database. If not specified, the <see cref="DataTable.TableName"/> property will be used.</param>
    /// <param name="table">The source <see cref="DataTable"/> object that contains the rows to be bulk-deleted to the target table.</param>
    /// <param name="rowState">The state of the rows to be bulk-deleted. If not specified, all the rows of the table will be used.</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the rows of the table will be sent together in one-go.</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the <see cref="DataTable"/> will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static int BinaryBulkDelete(this NpgsqlConnection connection,
        string tableName,
        DataTable table,
        DataRowState? rowState = null,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction transaction = null) =>
        BinaryBulkDeleteBase(connection: connection,
            tableName: (tableName ?? table?.TableName),
            table: table,
            rowState: rowState,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            batchSize: batchSize,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction);

    #endregion

    #region BinaryBulkDelete<DbDataReader>

    /// <summary>
    /// Delete the existing rows via <see cref="DbDataReader"/> by bulk. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="tableName">The name of the target table from the database.</param>
    /// <param name="reader">The instance of <see cref="DbDataReader"/> object that contains the rows to be bulk-deleted to the target table.</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the <see cref="DbDataReader"/> will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static int BinaryBulkDelete(this NpgsqlConnection connection,
        string tableName,
        DbDataReader reader,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction transaction = null) =>
        BinaryBulkDeleteBase(connection: connection,
            tableName: tableName,
            reader: reader,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction);

    #endregion

    #endregion

    #region Async

    #region BinaryBulkDelete<TEntity>

    /// <summary>
    /// Delete the existing rows via entities by bulk in an asynchronous way. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="entities">The list of entities to be bulk-deleted to the target table.
    /// This can be an <see cref="IEnumerable{T}"/> of the following objects (<typeparamref name="TEntity"/> (as class/model), <see cref="ExpandoObject"/>,
    /// <see cref="IDictionary{TKey, TValue}"/> (of <see cref="string"/>/<see cref="object"/>) and Anonymous Types).</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the entities will be sent together in one-go.</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the entities will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static Task<int> BinaryBulkDeleteAsync<TEntity>(this NpgsqlConnection connection,
        IEnumerable<TEntity> entities,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class =>
        BinaryBulkDeleteAsync<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>(),
            entities: entities,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            batchSize: batchSize,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction,
            cancellationToken: cancellationToken);

    /// <summary>
    /// Delete the existing rows via entities by bulk in an asynchronous way. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="tableName">The name of the target table from the database.</param>
    /// <param name="entities">The list of entities to be bulk-deleted to the target table.
    /// This can be an <see cref="IEnumerable{T}"/> of the following objects (<typeparamref name="TEntity"/> (as class/model), <see cref="ExpandoObject"/>,
    /// <see cref="IDictionary{TKey, TValue}"/> (of <see cref="string"/>/<see cref="object"/>) and Anonymous Types).</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the rows of the table will be sent together in one-go.</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the entities will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static async Task<int> BinaryBulkDeleteAsync<TEntity>(this NpgsqlConnection connection,
        string tableName,
        IEnumerable<TEntity> entities,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class =>
        await BinaryBulkDeleteBaseAsync<TEntity>(connection: connection,
            tableName: (tableName ?? ClassMappedNameCache.Get<TEntity>()),
            entities: entities,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            batchSize: batchSize,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction,
            cancellationToken: cancellationToken);

    #endregion

    #region BinaryBulkDelete<DataTable>

    /// <summary>
    /// Delete the existing rows via <see cref="DataTable"/> by bulk in an asynchronous way. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="table">The source <see cref="DataTable"/> object that contains the rows to be bulk-deleted to the target table.</param>
    /// <param name="rowState">The state of the rows to be bulk-deleted. If not specified, all the rows of the table will be used.</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the rows of the table will be sent together in one-go.</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the <see cref="DataTable"/> will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static Task<int> BinaryBulkDeleteAsync(this NpgsqlConnection connection,
        DataTable table,
        DataRowState? rowState = null,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default) =>
        BinaryBulkDeleteAsync(connection: connection,
            tableName: table?.TableName,
            table: table,
            rowState: rowState,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            batchSize: batchSize,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction,
            cancellationToken: cancellationToken);

    /// <summary>
    /// Delete the existing rows via <see cref="DataTable"/> by bulk in an asynchronous way. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="tableName">The name of the target table from the database. If not specified, the <see cref="DataTable.TableName"/> property will be used.</param>
    /// <param name="table">The source <see cref="DataTable"/> object that contains the rows to be bulk-deleted to the target table.</param>
    /// <param name="rowState">The state of the rows to be bulk-deleted. If not specified, all the rows of the table will be used.</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the rows of the table will be sent together in one-go.</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the <see cref="DataTable"/> will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static async Task<int> BinaryBulkDeleteAsync(this NpgsqlConnection connection,
        string tableName,
        DataTable table,
        DataRowState? rowState = null,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default) =>
        await BinaryBulkDeleteBaseAsync(connection: connection,
            tableName: (tableName ?? table?.TableName),
            table: table,
            rowState: rowState,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            batchSize: batchSize,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction,
            cancellationToken: cancellationToken);

    #endregion

    #region BinaryBulkDelete<DbDataReader>

    /// <summary>
    /// Delete the existing rows via <see cref="DbDataReader"/> by bulk in an asynchronous way. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <param name="connection">The current connection object in used.</param>
    /// <param name="tableName">The name of the target table from the database.</param>
    /// <param name="reader">The instance of <see cref="DbDataReader"/> object that contains the rows to be bulk-deleted to the target table.</param>
    /// <param name="qualifiers">The list of qualifier fields to be used during the operation. Ensure to target the indexed columns to make the execution more performant. If not specified, the primary key will be used.</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not the entity mappings, but is working on top of it)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="keepIdentity">A value that indicates whether the existing identity property values from the <see cref="DbDataReader"/> will be kept during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been deleted from the target table.</returns>
    public static async Task<int> BinaryBulkDeleteAsync(this NpgsqlConnection connection,
        string tableName,
        DbDataReader reader,
        IEnumerable<Field>? qualifiers = null,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        bool keepIdentity = true,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default) =>
        await BinaryBulkDeleteBaseAsync(connection: connection,
            tableName: tableName,
            reader: reader,
            qualifiers: qualifiers,
            mappings: mappings,
            bulkCopyTimeout: bulkCopyTimeout,
            keepIdentity: keepIdentity,
            pseudoTableType: pseudoTableType,
            transaction: transaction,
            cancellationToken: cancellationToken);

    #endregion

    #endregion
}
