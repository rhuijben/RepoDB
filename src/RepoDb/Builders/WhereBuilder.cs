using System.Linq.Expressions;

namespace RepoDb.Builders;
public readonly struct WhereBuilder<TEntity>
    where TEntity : class
{
    readonly Expression<Func<TEntity, bool>>? _expr;
    readonly QueryGroup? _group;

    public WhereBuilder(Expression<Func<TEntity, bool>> whereExpression)
    {
        _expr = whereExpression ?? throw new ArgumentNullException(nameof(whereExpression));
    }

    public WhereBuilder(QueryGroup? group)
    {
        _group = group;
    }

    internal QueryGroup? Build(in DbSession session, string tableName)
    {
        return _group ?? (_expr is { } e ? QueryGroup.Parse(e, session.Connection, session.Transaction, tableName) : null);
    }

    internal ValueTask<QueryGroup?> BuildAsync(in DbSession session, string tableName, CancellationToken cancellationToken)
    {
        return new(_group ?? (_expr is { } e ? QueryGroup.Parse(e, session.Connection, session.Transaction, tableName) : null));
    }
}
