using System.Data.Common;

namespace RepoDb;

/// <summary>
/// Keeps a database session: a <see cref="DbConnection"/> + optionally a <see cref="DbTransaction"/>
/// </summary>
/// <remarks>The session optionally has ownership of the transaction *or* the database (never both). This ownership is passed
/// when the struct is passed, so only the creator should dispose the session. Typically via an (await) using pattern</remarks>
public readonly struct DbSession : IAsyncDisposable, IDisposable
{
    private readonly object _value; // Either DbConnection or DbTransaction
    private readonly bool _owns;

    public DbSession(DbConnection connection, bool ownsConnection = false)
    {
#if NET
        ArgumentNullException.ThrowIfNull(connection);
#else
        if (connection is null)
            throw new ArgumentNullException(nameof(connection));
#endif
        _value = connection;
        _owns = ownsConnection;
    }

    public DbSession(DbTransaction transaction, bool ownsTransaction = false)
    {
#if NET
        ArgumentNullException.ThrowIfNull(transaction);
#else
        if (transaction is null)
            throw new ArgumentNullException(nameof(transaction));
#endif
        _value = transaction;
        _owns = ownsTransaction;
    }

    /// <summary>
    /// Creates a copy of the session, without ownership
    /// </summary>
    /// <param name="session"></param>
    public DbSession(in DbSession session)
    {
        _value = session._value;
        _owns = false; // Copying a session does not transfer ownership
    }

    public DbConnection Connection =>
        _value is DbTransaction tx ? tx.Connection! : (DbConnection)_value;

    public DbTransaction? Transaction =>
        _value as DbTransaction;

    public void Dispose()
    {
        if (_owns)
        {
            if (_value is DbTransaction tx)
                tx.Dispose();
            else
                ((DbConnection)_value).Dispose();
        }
    }

#if NET
    public async ValueTask DisposeAsync()
    {
        if (_owns)
        {
            if (_value is DbTransaction tx)
                await tx.DisposeAsync().ConfigureAwait(false);
            else
                await ((DbConnection)_value).DisposeAsync().ConfigureAwait(false);
        }
    }
#else
    public ValueTask DisposeAsync()
    {
        Dispose();
        return new();
    }
#endif

    public void Deconstruct(out DbConnection Connection, out DbTransaction? Transaction)
    {
        Connection = this.Connection;
        Transaction = this.Transaction;
    }
}

public interface IDbSessionFactory
{
    ValueTask<DbSession> CreateOpenSessionAsync(CancellationToken cancellationToken = default);
}
