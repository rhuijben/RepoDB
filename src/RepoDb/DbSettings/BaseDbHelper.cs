#nullable enable
using System.Data;
using System.Data.Common;
using RepoDb.Interfaces;

namespace RepoDb.DbSettings;
public abstract class BaseDbHelper : IDbHelper
{
    protected BaseDbHelper(IResolver<string, Type> dbResolver)
    {
        if (dbResolver is null)
            throw new ArgumentNullException(nameof(dbResolver));

        DbTypeResolver = dbResolver;
    }

    public IResolver<string, Type> DbTypeResolver { get; protected init; }

    /// <inheritdoc />
    public virtual void DynamicHandler<TEventInstance>(TEventInstance instance, string key)
    { }

    /// <inheritdoc />
    public abstract IEnumerable<DbField> GetFields(IDbConnection connection, string tableName, IDbTransaction? transaction = null);

    /// <inheritdoc />
    public virtual ValueTask<IEnumerable<DbField>> GetFieldsAsync(IDbConnection connection, string tableName, IDbTransaction? transaction = null, CancellationToken cancellationToken = default) => new(GetFields(connection, tableName, transaction));

    /// <inheritdoc />
    public abstract IEnumerable<DbSchemaObject> GetSchemaObjects(IDbConnection connection, IDbTransaction? transaction = null);

    /// <inheritdoc />
    public virtual ValueTask<IEnumerable<DbSchemaObject>> GetSchemaObjectsAsync(IDbConnection connection, IDbTransaction? transaction = null, CancellationToken cancellationToken = default) => new(GetSchemaObjects(connection, transaction));

    /// <inheritdoc />
    public virtual T GetScopeIdentity<T>(IDbConnection connection, IDbTransaction? transaction = null) => throw new NotImplementedException();

    /// <inheritdoc />
    public virtual ValueTask<T> GetScopeIdentityAsync<T>(IDbConnection connection, IDbTransaction? transaction = null, CancellationToken cancellationToken = default) => new(GetScopeIdentity<T>(connection, transaction));

    /// <inheritdoc />
    public virtual object? ParameterValueToDb(object? value) => value;

    /// <inheritdoc />
    public virtual Func<object?>? PrepareForIdentityOutput(DbCommand command) => null;
}
