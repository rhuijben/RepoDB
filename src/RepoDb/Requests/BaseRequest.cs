#nullable enable
using System.Data;
using RepoDb.Interfaces;

namespace RepoDb.Requests;


internal sealed class RequestState
{
    public IDbConnection Connection { get; init; }
    public IDbTransaction Transaction { get; init; }
    public DbFieldCollection DbFields { get; init; }
    public IStatementBuilder? StatementBuilder { get; init; }
}

/// <summary>
/// A base class for all operational request.
/// </summary>
internal abstract class BaseRequest : IEquatable<BaseRequest>, IDisposable
{
    RequestState? requestState;

    /// <summary>
    /// Creates a new instance of <see cref="BaseRequest"/> object.
    /// </summary>
    /// <param name="name">The name of request.</param>
    /// <param name="connection">The connection object.</param>
    /// <param name="transaction">The transaction object.</param>
    /// <param name="statementBuilder">The statement builder.</param>
    public BaseRequest(string name, RequestState requestState)
    {
        Name = name;
        this.requestState = requestState;
    }

    /// <summary>
    /// Gets the type.
    /// </summary>
    public Type Type { get; init; }

    /// <summary>
    /// Gets the name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the connection object.
    /// </summary>
    public IDbConnection? Connection => requestState?.Connection ?? throw new InvalidOperationException();

    /// <summary>
    /// Gets the transaction object.
    /// </summary>
    public IDbTransaction Transaction => requestState?.Transaction ?? throw new InvalidOperationException();

    /// <summary>
    /// Gets the statement builder.
    /// </summary>
    public IStatementBuilder StatementBuilder => requestState?.StatementBuilder ?? throw new InvalidOperationException();


    public DbFieldCollection DbFields => requestState?.DbFields ?? throw new InvalidOperationException();

    #region Equality and comparers

    protected int? HashCode { get; set; }

    /// <summary>
    /// Returns the hashcode for this <see cref="BaseRequest"/>.
    /// </summary>
    /// <returns>The hashcode value.</returns>
    public abstract override int GetHashCode();

    /// <summary>
    /// Compares the <see cref="BaseRequest"/> object equality against the given target object.
    /// </summary>
    /// <param name="obj">The object to be compared to the current object.</param>
    /// <returns>True if the instances are equals.</returns>
    public override bool Equals(object obj)
    {
        return Equals(obj as BaseRequest);
    }

    /// <summary>
    /// Compares the <see cref="BaseRequest"/> object equality against the given target object.
    /// </summary>
    /// <param name="other">The object to be compared to the current object.</param>
    /// <returns>True if the instances are equal.</returns>
    public bool Equals(BaseRequest other)
    {
        if ((other is null))
        {
            return false;
        }
        else if (other.GetHashCode() != GetHashCode())
        {
            return false;
        }

        return StrictEquals(other);
    }

    protected abstract bool StrictEquals(BaseRequest other);

    public void Dispose()
    {
        requestState = null;
    }

    /// <summary>
    /// Compares the equality of the two <see cref="BaseRequest"/> objects.
    /// </summary>
    /// <param name="objA">The first <see cref="BaseRequest"/> object.</param>
    /// <param name="objB">The second <see cref="BaseRequest"/> object.</param>
    /// <returns>True if the instances are equal.</returns>
    public static bool operator ==(BaseRequest objA,
        BaseRequest objB) =>
        (objA is null) ? objB is null : objA.Equals(objB);

    /// <summary>
    /// Compares the inequality of the two <see cref="BaseRequest"/> objects.
    /// </summary>
    /// <param name="objA">The first <see cref="BaseRequest"/> object.</param>
    /// <param name="objB">The second <see cref="BaseRequest"/> object.</param>
    /// <returns>True if the instances are not equal.</returns>
    public static bool operator !=(BaseRequest objA,
        BaseRequest objB) =>
        (objA == objB) == false;

    #endregion
}
