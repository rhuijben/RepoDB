using RepoDb.Builders;

namespace RepoDb;

public static partial class DbSessionExtension
{
    public static async ValueTask<int> UpdateAsync<TEntity, TResult>(
        this DbSession session,
        TEntity entity,
        FieldSetBuilder<TEntity> fields = default,
        WhereBuilder<TEntity> where = default,
        ConfigureArgumentBuilder config = default,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
#if NET
        ArgumentNullException.ThrowIfNull(entity);
#else
        if (entity is null)
            throw new ArgumentNullException(nameof(entity));
#endif

        var c = config.Build(entity);

        return await session.Connection.UpdateAsyncInternal<TEntity>(
            c.TableName,
            entity,
            await where.BuildAsync(session, c.TableName, cancellationToken).ConfigureAwait(false)
                ?? await WhereFromEntityAsync(session, entity, c.TableName, cancellationToken).ConfigureAwait(false),
            fields.AsEnumerable(session, c),
            c.Hints,
            c.CommandTimeout,
            TraceKeys.Update,
            session.Transaction,
            c.trace,
            c.StatementBuilder,
            cancellationToken)
        .ConfigureAwait(false);
    }

    private static async ValueTask<QueryGroup?> WhereFromEntityAsync<TEntity>(DbSession session, TEntity entity, string tableName, CancellationToken cancellationToken) where TEntity : class
    {
        var key = await DbConnectionExtension.GetAndGuardPrimaryKeyOrIdentityKeyAsync(session.Connection, tableName, session.Transaction, cancellationToken)
        .ConfigureAwait(false);

        return DbConnectionExtension.ToQueryGroup(key, entity);
    }
}
