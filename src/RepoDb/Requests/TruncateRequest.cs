using System.Data;
using RepoDb.Interfaces;

namespace RepoDb.Requests;

/// <summary>
/// A class that holds the value of the 'Truncate' operation arguments.
/// </summary>
internal sealed class TruncateRequest : BaseRequest
{
    /// <summary>
    /// Creates a new instance of <see cref="TruncateRequest"/> object.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public TruncateRequest(Type type,
        IDbConnection connection,
        IDbTransaction? transaction,
        IStatementBuilder? statementBuilder = null)
        : this(ClassMappedNameCache.Get(type),
            connection,
            transaction,
            statementBuilder)
    {
        Type = type;
    }

    /// <summary>
    /// Creates a new instance of <see cref="TruncateRequest"/> object.
    /// </summary>
    /// <param name="name">The name of the request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public TruncateRequest(string name,
        IDbConnection connection,
        IDbTransaction? transaction,
        IStatementBuilder? statementBuilder = null)
        : base(name,
              connection,
              transaction,
              statementBuilder)
    { }

    #region Equality and comparers

    /// <summary>
    /// Returns the hashcode for this <see cref="TruncateRequest"/>.
    /// </summary>
    /// <returns>The hashcode value.</returns>
    public override int GetHashCode()
    {
        if (this.HashCode is not { } hashCode)
        {
            // Get first the entity hash code
            hashCode = System.HashCode.Combine(typeof(TruncateRequest), Name, ".Truncate");

        }

        // Set and return the hashcode
        return this.HashCode ??= hashCode;
    }

    protected override bool StrictEquals(BaseRequest other)
    {
        // TODO: Implement Equals() and use from here.
        return other is TruncateRequest;
    }

    #endregion
}
