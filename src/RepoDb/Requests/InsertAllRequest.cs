﻿using RepoDb.Extensions;
using RepoDb.Interfaces;
using System.Data;

namespace RepoDb.Requests;

/// <summary>
/// A class that holds the value of the 'Insert' operation arguments.
/// </summary>
internal class InsertAllRequest : BaseRequest
{
    private int? hashCode = null;

    /// <summary>
    /// Creates a new instance of <see cref="InsertAllRequest"/> object.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="fields">The list of the target fields.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public InsertAllRequest(Type type,
        IDbConnection connection,
        IDbTransaction transaction,
        IEnumerable<Field> fields = null,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : this(type,
              ClassMappedNameCache.Get(type),
              connection,
              transaction,
              fields,
              batchSize,
              hints,
              statementBuilder)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="InsertAllRequest"/> object.
    /// </summary>
    /// <param name="name">The name of the request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="fields">The list of the target fields.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public InsertAllRequest(string name,
        IDbConnection connection,
        IDbTransaction transaction,
        IEnumerable<Field> fields = null,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : this(null,
              name,
              connection,
              transaction,
              fields,
              batchSize,
              hints,
              statementBuilder)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="InsertAllRequest"/> object.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="name">The name of the request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="fields">The list of the target fields.</param>
    /// <param name="batchSize">The batch size of the insertion.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public InsertAllRequest(Type type,
        string name,
        IDbConnection connection,
        IDbTransaction transaction,
        IEnumerable<Field> fields = null,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : base(name ?? ClassMappedNameCache.Get(type),
              connection,
              transaction,
              statementBuilder)
    {
        Type = type;
        Fields = fields?.AsList();
        BatchSize = batchSize;
        Hints = hints;
    }

    /// <summary>
    /// Gets the target fields.
    /// </summary>
    public IEnumerable<Field> Fields { get; set; }

    /// <summary>
    /// Gets the size batch of the insertion.
    /// </summary>
    public int BatchSize { get; set; }

    /// <summary>
    /// Gets the hints for the table.
    /// </summary>
    public string Hints { get; }

    #region Equality and comparers

    /// <summary>
    /// Returns the hashcode for this <see cref="InsertAllRequest"/>.
    /// </summary>
    /// <returns>The hashcode value.</returns>
    public override int GetHashCode()
    {
        // Make sure to return if it is already provided
        if (this.hashCode != null)
        {
            return this.hashCode.Value;
        }

        // Get first the entity hash code
        var hashCode = HashCode.Combine(base.GetHashCode(), Name, ".InsertAll");

        // Get the qualifier <see cref="Field"/> objects
        if (Fields != null)
        {
            foreach (var field in Fields)
            {
                hashCode = HashCode.Combine(hashCode, field);
            }
        }

        // Get the batch size
        if (BatchSize > 0)
        {
            hashCode = HashCode.Combine(hashCode, BatchSize);
        }

        // Add the hints
        if (!string.IsNullOrWhiteSpace(Hints))
        {
            hashCode = HashCode.Combine(hashCode, Hints);
        }

        // Set and return the hashcode
        return (this.hashCode = hashCode).Value;
    }
}

#endregion
