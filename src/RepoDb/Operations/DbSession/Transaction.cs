using System.Data;
using System.Data.Common;

namespace RepoDb;
public static partial class DbSessionExtension
{
    public static async ValueTask<DbSession> BeginTransactionAsync(this DbSession session, CancellationToken cancellationToken = default)
    {
        // Cast necessary for .Net Framework
        return new((DbTransaction)await session.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false), true);
    }

    public static async ValueTask<DbSession> BeginTransactionAsync(this DbSession session, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        // Cast necessary for .Net Framework
        return new((DbTransaction)await session.Connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false), true);
    }

    public static async ValueTask CommitAsync(this DbSession session, CancellationToken cancellationToken = default)
    {
        if (session.Transaction is DbTransaction dbTransaction)
        {
            await dbTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            throw new InvalidOperationException("No transaction to commit.");
        }
    }

    public static async ValueTask RollbackAsync(this DbSession session, CancellationToken cancellationToken = default)
    {
        if (session.Transaction is DbTransaction dbTransaction)
        {
            await dbTransaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            throw new InvalidOperationException("No transaction to commit.");
        }
    }

    public static async ValueTask<DbSession> EnsureTransactionAsync(this DbSession session, CancellationToken cancellationToken = default)
    {
        if (session.Transaction is null)
            return await session.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        else
            return new(session); // Typically called with using, so make sure owned is false
    }
}
