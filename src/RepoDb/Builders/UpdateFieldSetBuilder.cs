
namespace RepoDb.Builders;
public readonly struct UpdateFieldSetBuilder<TEntity>
    where TEntity : class
{
    internal IEnumerable<Field>? AsEnumerable(FieldSet? flds, DbSession session, OperationConfigureArg c)
    {
        throw new NotImplementedException();
    }
}
