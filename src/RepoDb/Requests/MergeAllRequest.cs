﻿using System.Data;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb.Requests;

/// <summary>
/// A class that holds the value of the 'MergeAll' operation arguments.
/// </summary>
internal class MergeAllRequest : BaseRequest
{
    /// <summary>
    /// Creates a new instance of <see cref="MergeAllRequest"/> object.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="fields">The list of the target fields.</param>
    /// <param name="qualifiers">The list of qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the merge operation.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public MergeAllRequest(Type type,
        IDbConnection connection,
        IDbTransaction? transaction,
        IEnumerable<Field>? fields = null,
        IEnumerable<Field>? qualifiers = null,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : this(type,
            ClassMappedNameCache.Get(type),
            connection,
            transaction,
            fields,
            qualifiers,
            batchSize,
            hints,
            statementBuilder)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="MergeAllRequest"/> object.
    /// </summary>
    /// <param name="name">The name of the request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="fields">The list of the target fields.</param>
    /// <param name="qualifiers">The list of qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the merge operation.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public MergeAllRequest(string name,
        IDbConnection connection,
        IDbTransaction? transaction,
        IEnumerable<Field>? fields = null,
        IEnumerable<Field>? qualifiers = null,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : this(null,
            name,
            connection,
            transaction,
            fields,
            qualifiers,
            batchSize,
            hints,
            statementBuilder)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="MergeAllRequest"/> object.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="name">The name of the request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="fields">The list of the target fields.</param>
    /// <param name="qualifiers">The list of qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the merge operation.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public MergeAllRequest(Type type,
        string name,
        IDbConnection connection,
        IDbTransaction? transaction,
        IEnumerable<Field>? fields = null,
        IEnumerable<Field>? qualifiers = null,
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
        Qualifiers = qualifiers?.AsList();
        BatchSize = batchSize;
        Hints = hints;
    }

    /// <summary>
    /// Gets the list of the target fields.
    /// </summary>
    public IEnumerable<Field> Fields { get; init; }

    /// <summary>
    /// Gets the qualifier <see cref="Field"/> objects.
    /// </summary>
    public IEnumerable<Field>? Qualifiers { get; init; }

    /// <summary>
    /// Gets the size batch of the update operation.
    /// </summary>
    public int BatchSize { get; init; }

    /// <summary>
    /// Gets the hints for the table.
    /// </summary>
    public string Hints { get; }

    #region Equality and comparers

    /// <summary>
    /// Returns the hashcode for this <see cref="MergeAllRequest"/>.
    /// </summary>
    /// <returns>The hashcode value.</returns>
    public override int GetHashCode()
    {
        if (this.HashCode is not { } hashCode)
        {
            // Get first the entity hash code
            hashCode = System.HashCode.Combine(
                typeof(MergeAllRequest),
                Name,
                BatchSize,
                Hints);

            // Get the qualifier <see cref="Field"/> objects
            if (Fields != null)
            {
                foreach (var field in Fields)
                {
                    hashCode = System.HashCode.Combine(hashCode, field);
                }
            }

            // Get the qualifier <see cref="Field"/> objects
            if (Qualifiers != null) // Much faster than Qualifiers?.<Methods|Properties>
            {
                foreach (var field in Qualifiers)
                {
                    hashCode = System.HashCode.Combine(hashCode, field);
                }
            }

            HashCode = hashCode;
        }

        return hashCode;
    }

    protected override bool StrictEquals(BaseRequest other)
    {
        // TODO: Implement Equals() and use from here.
        return other is MergeAllRequest;
    }

    #endregion
}
