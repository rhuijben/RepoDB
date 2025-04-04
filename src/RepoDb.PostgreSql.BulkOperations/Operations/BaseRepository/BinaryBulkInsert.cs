﻿using Npgsql;
using RepoDb.Enumerations.PostgreSql;
using RepoDb.PostgreSql.BulkOperations;
using System.Dynamic;

namespace RepoDb;

/// <summary>
/// An extension class for <see cref="BaseRepository{TEntity, TDbConnection}"/> object.
/// </summary>
public static partial class BaseRepositoryExtension
{
    #region Sync

    #region BinaryBulkInsert<TEntity>

    /// <summary>
    /// Inserts a list of entities into the target table by bulk. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    /// <param name="repository">The instance of <see cref="BaseRepository{TEntity, TDbConnection}"/> object.</param>
    /// <param name="entities">The list of entities to be bulk-inserted to the target table.
    /// This can be an <see cref="IEnumerable{T}"/> of the following objects (<typeparamref name="TEntity"/> (as class/model), <see cref="ExpandoObject"/>,
    /// <see cref="IDictionary{TKey, TValue}"/> (of <see cref="string"/>/<see cref="object"/>) and Anonymous Types).</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not an entity mapping)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the entities will be sent together in one-go.</param>
    /// <param name="identityBehavior">The behavior of how the identity column would work during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <returns>The number of rows that has been inserted into the target table.</returns>
    public static int BinaryBulkInsert<TEntity>(this BaseRepository<TEntity, NpgsqlConnection> repository,
        IEnumerable<TEntity> entities,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        BulkImportIdentityBehavior identityBehavior = default,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction transaction = null)
        where TEntity : class =>
        repository.DbRepository.BinaryBulkInsert<TEntity>(tableName: ClassMappedNameCache.Get<TEntity>(),
                entities: entities,
                mappings: mappings,
                bulkCopyTimeout: bulkCopyTimeout,
                batchSize: batchSize,
                identityBehavior: identityBehavior,
                pseudoTableType: pseudoTableType,
                transaction: transaction);

    /// <summary>
    /// Inserts a list of entities into the target table by bulk. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    /// <param name="repository">The instance of <see cref="BaseRepository{TEntity, TDbConnection}"/> object.</param>
    /// <param name="tableName">The name of the target table from the database.</param>
    /// <param name="entities">The list of entities to be bulk-inserted to the target table.
    /// This can be an <see cref="IEnumerable{T}"/> of the following objects (<typeparamref name="TEntity"/> (as class/model), <see cref="ExpandoObject"/>,
    /// <see cref="IDictionary{TKey, TValue}"/> (of <see cref="string"/>/<see cref="object"/>) and Anonymous Types).</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not an entity mapping)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the rows of the table will be sent together in one-go.</param>
    /// <param name="identityBehavior">The behavior of how the identity column would work during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <returns>The number of rows that has been inserted into the target table.</returns>
    public static int BinaryBulkInsert<TEntity>(this BaseRepository<TEntity, NpgsqlConnection> repository,
        string tableName,
        IEnumerable<TEntity> entities,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        BulkImportIdentityBehavior identityBehavior = default,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction transaction = null)
        where TEntity : class =>
        repository.DbRepository.BinaryBulkInsert<TEntity>(tableName: (tableName ?? ClassMappedNameCache.Get<TEntity>()),
                entities: entities,
                mappings: mappings,
                bulkCopyTimeout: bulkCopyTimeout,
                batchSize: batchSize,
                identityBehavior: identityBehavior,
                pseudoTableType: pseudoTableType,
                transaction: transaction);

    #endregion

    #endregion

    #region Async

    #region BinaryBulkInsert<TEntity>

    /// <summary>
    /// Inserts a list of entities into the target table by bulk in an asynchronous way. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    /// <param name="repository">The instance of <see cref="BaseRepository{TEntity, TDbConnection}"/> object.</param>
    /// <param name="entities">The list of entities to be bulk-inserted to the target table.
    /// This can be an <see cref="IEnumerable{T}"/> of the following objects (<typeparamref name="TEntity"/> (as class/model), <see cref="ExpandoObject"/>,
    /// <see cref="IDictionary{TKey, TValue}"/> (of <see cref="string"/>/<see cref="object"/>) and Anonymous Types).</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not an entity mapping)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the entities will be sent together in one-go.</param>
    /// <param name="identityBehavior">The behavior of how the identity column would work during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been inserted into the target table.</returns>
    public static async Task<int> BinaryBulkInsertAsync<TEntity>(this BaseRepository<TEntity, NpgsqlConnection> repository,
        IEnumerable<TEntity> entities,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        BulkImportIdentityBehavior identityBehavior = default,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class =>
        await repository.DbRepository.BinaryBulkInsertAsync<TEntity>(tableName: ClassMappedNameCache.Get<TEntity>(),
                entities: entities,
                mappings: mappings,
                bulkCopyTimeout: bulkCopyTimeout,
                batchSize: batchSize,
                identityBehavior: identityBehavior,
                pseudoTableType: pseudoTableType,
                transaction: transaction,
                cancellationToken: cancellationToken);

    /// <summary>
    /// Inserts a list of entities into the target table by bulk in an asynchronous way. Underneath this operation is a call directly to the existing
    /// <see cref="NpgsqlConnection.BeginBinaryExport(string)"/> method via the 'BinaryImport' extended method.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    /// <param name="repository">The instance of <see cref="BaseRepository{TEntity, TDbConnection}"/> object.</param>
    /// <param name="tableName">The name of the target table from the database.</param>
    /// <param name="entities">The list of entities to be bulk-inserted to the target table.
    /// This can be an <see cref="IEnumerable{T}"/> of the following objects (<typeparamref name="TEntity"/> (as class/model), <see cref="ExpandoObject"/>,
    /// <see cref="IDictionary{TKey, TValue}"/> (of <see cref="string"/>/<see cref="object"/>) and Anonymous Types).</param>
    /// <param name="mappings">The list of mappings to be used. If not specified, only the matching properties/columns from the target table will be used. (This is not an entity mapping)</param>
    /// <param name="bulkCopyTimeout">The timeout expiration of the operation (see <see cref="NpgsqlBinaryImporter.Timeout"/>).</param>
    /// <param name="batchSize">The size per batch to be sent to the database. If not specified, all the rows of the table will be sent together in one-go.</param>
    /// <param name="identityBehavior">The behavior of how the identity column would work during the operation.</param>
    /// <param name="pseudoTableType">The value that defines whether an actual or temporary table will be created for the pseudo-table.</param>
    /// <param name="transaction">The current transaction object in used. If not specified, an implicit transaction will be created and used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>The number of rows that has been inserted into the target table.</returns>
    public static async Task<int> BinaryBulkInsertAsync<TEntity>(this BaseRepository<TEntity, NpgsqlConnection> repository,
        string tableName,
        IEnumerable<TEntity> entities,
        IEnumerable<NpgsqlBulkInsertMapItem>? mappings = null,
        int bulkCopyTimeout = 0,
        int batchSize = 0,
        BulkImportIdentityBehavior identityBehavior = default,
        BulkImportPseudoTableType pseudoTableType = default,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default)
        where TEntity : class =>
        await repository.DbRepository.BinaryBulkInsertAsync<TEntity>(tableName: (tableName ?? ClassMappedNameCache.Get<TEntity>()),
                entities: entities,
                mappings: mappings,
                bulkCopyTimeout: bulkCopyTimeout,
                batchSize: batchSize,
                identityBehavior: identityBehavior,
                pseudoTableType: pseudoTableType,
                transaction: transaction,
                cancellationToken: cancellationToken);

    #endregion

    #endregion
}
