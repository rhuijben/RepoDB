using RepoDb.Builders;
using RepoDb.Extensions;

namespace RepoDb;

public static partial class DbSessionExtension
{
    public static async ValueTask<TResult> MergeAsync<TEntity, TResult>(
        this DbSession session,
        TEntity entity,
        FieldSetBuilder<TEntity> fields = default,
        QualifierSetBuilder<TEntity> qualifiers = default,
        UpdateFieldSetBuilder<TEntity> updateFields = default,
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

        var flds = fields.AsEnumerable(session, c);

        return await session.Connection.MergeAsyncInternal<TEntity, TResult>(
            c.TableName,
            entity,
            qualifiers.AsEnumerable(flds, session, c),
            flds,
            updateFields.AsEnumerable(flds, session, c),
            c.Hints,
            c.CommandTimeout,
            TraceKeys.Merge,
            session.Transaction,
            c.trace,
            c.StatementBuilder,
            cancellationToken)
        .ConfigureAwait(false);
    }

    public static async ValueTask<TEntity> MergeAsync<TEntity>(
        this DbSession session,
        TEntity entity,
        FieldSetBuilder<TEntity> fields = default,
        QualifierSetBuilder<TEntity> qualifiers = default,
        UpdateFieldSetBuilder<TEntity> updateFields = default,
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

        var flds = fields.AsEnumerable(session, c);

        await session.Connection.MergeAsyncInternal<TEntity, object>(
            c.TableName,
            entity,
            qualifiers.AsEnumerable(flds, session, c),
            flds,
            updateFields.AsEnumerable(flds, session, c),
            c.Hints,
            c.CommandTimeout,
            TraceKeys.Merge,
            session.Transaction,
            c.trace,
            c.StatementBuilder,
            cancellationToken)
        .ConfigureAwait(false);

        return entity;
    }

    public static async ValueTask<IEnumerable<TEntity>> MergeAsync<TEntity>(
        this DbSession session,
        IEnumerable<TEntity> entities,
        FieldSetBuilder<TEntity> fields = default,
        QualifierSetBuilder<TEntity> qualifiers = default,
        UpdateFieldSetBuilder<TEntity> updateFields = default,
        ConfigureArgumentBuilder config = default,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
#if NET
        ArgumentNullException.ThrowIfNull(entities);
#else
        if (entities is null)
            throw new ArgumentNullException(nameof(entities));
#endif
        entities = entities.AsList();

        var c = config.Build(entities.FirstOrDefault());

        var flds = fields.AsEnumerable(session, c);

        await session.Connection.MergeAllAsyncInternal<TEntity>(
            c.TableName,
            entities,
            qualifiers.AsEnumerable(flds, session, c),
            updateFields.AsEnumerable(flds, session, c),
            batchSize: 0,
            flds,
            c.Hints,
            c.CommandTimeout,
            TraceKeys.MergeAll,
            session.Transaction,
            c.trace,
            c.StatementBuilder,
            cancellationToken)
        .ConfigureAwait(false);

        return entities;
    }
}
