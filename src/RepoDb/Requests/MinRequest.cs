﻿using System.Data;
using RepoDb.Interfaces;

namespace RepoDb.Requests;

/// <summary>
/// A class that holds the value of the 'Min' operation arguments.
/// </summary>
internal sealed class MinRequest : BaseRequest
{
    /// <summary>
    /// Creates a new instance of <see cref="MinRequest"/> object.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="field">The field object.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public MinRequest(Type type,
        IDbConnection connection,
        IDbTransaction? transaction,
        Field? field = null,
        QueryGroup? where = null,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : this(ClassMappedNameCache.Get(type),
              connection,
              transaction,
              field,
              where,
              hints,
              statementBuilder)
    {
        Type = type;
    }

    /// <summary>
    /// Creates a new instance of <see cref="MinRequest"/> object.
    /// </summary>
    /// <param name="name">The name of the request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="field">The field object.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public MinRequest(string name,
        IDbConnection connection,
        IDbTransaction? transaction,
        Field? field = null,
        QueryGroup? where = null,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : base(name,
              connection,
              transaction,
              statementBuilder)
    {
        Field = field;
        Where = where;
        Hints = hints;
    }

    /// <summary>
    /// Gets the field to be minimized.
    /// </summary>
    public Field Field { get; }

    /// <summary>
    /// Gets the query expression used.
    /// </summary>
    public QueryGroup Where { get; }

    /// <summary>
    /// Gets the hints for the table.
    /// </summary>
    public string Hints { get; }

    #region Equality and comparers

    /// <summary>
    /// Returns the hashcode for this <see cref="MinRequest"/>.
    /// </summary>
    /// <returns>The hashcode value.</returns>
    public override int GetHashCode()
    {
        if (this.HashCode is not { } hashCode)
        {
            // Get first the entity hash code
            HashCode = hashCode = System.HashCode.Combine(
                typeof(MinRequest),
                Name,
                Field,
                Where,
                Hints);
        }

        return hashCode;
    }

    protected override bool StrictEquals(BaseRequest other)
    {
        // TODO: Implement Equals() and use from here.
        return other is MinRequest;
    }

    #endregion
}
