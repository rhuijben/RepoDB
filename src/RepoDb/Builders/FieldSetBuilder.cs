using System.Linq.Expressions;

namespace RepoDb.Builders;

public class FieldSetBuilder<TEntity>
    where TEntity : class
{
    readonly IEnumerable<Field>? _set;
    readonly Func<FieldSetBuilderArg<TEntity>, IEnumerable<Field>?>? _builder;

    public FieldSetBuilder(IEnumerable<Field> set)
    {
        _set = set;
    }

    public FieldSetBuilder(Func<FieldSetBuilderArg<TEntity>, IEnumerable<Field>?> builder)
    {
        _builder = builder;
    }

    //public static implicit operator FieldSetBuilder<TEntity>(FieldSet fs)
    //{
    //    return new(fs);
    //}
    //
    //public static implicit operator FieldSetBuilder<TEntity>(Field[] fields)
    //{
    //    return new(fields);
    //}

    public static implicit operator FieldSetBuilder<TEntity>(Func<CC, IEnumerable<Field>> builder)
    {
        return new([]);
    }

    internal FieldSet? AsEnumerable(in DbSession s, in OperationConfigureArg ca)
    {
        if (_set is { } set)
            return new(set);
        else if (_builder is { } builder)
        {
            var arg = new FieldSetBuilderArg<TEntity>(s, ca.TableName!);

            return builder(arg) is { } set2 ? new(set2) : null;
        }
        else
            return null;
    }
}

public class CC
{ }

// Keep as regular struct for lambda compatibility, but make it readonly for performance
public readonly struct FieldSetBuilderArg<TEntity>
    where TEntity : class
{
    readonly DbSession _session;
    readonly string _tableName;

    internal FieldSetBuilderArg(DbSession session, string tableName)
    {
        _session = session;
        _tableName = tableName;
    }

    public FieldSet All => new(PropertyCache.Get<TEntity>());

    public FieldSet From(object o) => Field.Parse(o);
    public FieldSet From(Expression<Func<TEntity, object?>> expression) => Parse(expression);
    public FieldSet From(params string[] names) => Field.From(names);
    public FieldSet Parse(Expression<Func<TEntity, object?>> expression) => Field.Parse<TEntity>(expression);
}
