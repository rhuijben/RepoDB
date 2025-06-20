namespace RepoDb.Builders;
public readonly struct QualifierSetBuilder<TEntity>
    where TEntity : class
{
    internal IEnumerable<Field>? AsEnumerable(FieldSet? flds, DbSession session, OperationConfigureArg c)
    {
        throw new NotImplementedException();
    }
}
