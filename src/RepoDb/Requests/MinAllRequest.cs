﻿using RepoDb.Interfaces;
using System.Data;

namespace RepoDb.Requests;

/// <summary>
/// A class that holds the value of the 'MinAll' operation arguments.
/// </summary>
internal class MinAllRequest : BaseRequest
{
    private int? hashCode = null;

    /// <summary>
    /// Creates a new instance of <see cref="MinAllRequest"/> object.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="field">The field object.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public MinAllRequest(Type type,
        IDbConnection connection,
        IDbTransaction transaction,
        Field field = null,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : this(ClassMappedNameCache.Get(type),
              connection,
              transaction,
              field,
              hints,
              statementBuilder)
    {
        Type = type;
    }

    /// <summary>
    /// Creates a new instance of <see cref="MinAllRequest"/> object.
    /// </summary>
    /// <param name="name">The name of the request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="field">The field object.</param>
    /// <param name="hints">The hints for the table.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public MinAllRequest(string name,
        IDbConnection connection,
        IDbTransaction transaction,
        Field field = null,
        string? hints = null,
        IStatementBuilder? statementBuilder = null)
        : base(name,
              connection,
              transaction,
              statementBuilder)
    {
        Field = field;
        Hints = hints;
    }

    /// <summary>
    /// Gets the field to be minimized.
    /// </summary>
    public Field Field { get; }

    /// <summary>
    /// Gets the hints for the table.
    /// </summary>
    public string Hints { get; }

    #region Equality and comparers

    /// <summary>
    /// Returns the hashcode for this <see cref="MinAllRequest"/>.
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
        var hashCode = HashCode.Combine(base.GetHashCode(), Name, ".MinAll");

        // Add the field
        if (Field != null)
        {
            hashCode = HashCode.Combine(hashCode, Field);
        }

        // Add the hints
        if (!string.IsNullOrWhiteSpace(Hints))
        {
            hashCode = HashCode.Combine(hashCode, Hints);
        }

        // Set and return the hashcode
        return (this.hashCode = hashCode).Value;
    }

    #endregion
}
