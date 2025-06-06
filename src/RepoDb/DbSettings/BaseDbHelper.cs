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

    public virtual DbParameter? CreateTableParameter(DbConnection connection, IDbTransaction? transaction, DbType? dbType, IEnumerable<object> values, string parameterName)
    {
        return null;
    }

    public ValueTask<DbParameter?> CreateTableParameterAsync(DbConnection connection, IDbTransaction? transaction, DbType? dbType, IEnumerable<object> values, string parameterName, CancellationToken cancellationToken = default)
    {
        return new(CreateTableParameter(connection, transaction, dbType, values, parameterName));
    }

    public virtual string? CreateTableParameterText(DbConnection connection, IDbTransaction? transaction, string parameterName, IEnumerable<object> values)
    {
        return null;
    }

    /// <inheritdoc />
    public virtual void DynamicHandler<TEventInstance>(TEventInstance instance, string key)
    { }

    public virtual DbConnectionRuntimeInformation GetDbConnectionRuntimeInformation(IDbConnection connection, IDbTransaction transaction)
    {
        return new();
    }

    public virtual ValueTask<DbConnectionRuntimeInformation> GetDbConnectionRuntimeInformationAsync(IDbConnection connection, IDbTransaction transaction, CancellationToken cancellationToken)
    {
        return new(GetDbConnectionRuntimeInformation(connection, transaction));
    }

    /// <inheritdoc />
    public abstract IEnumerable<DbField> GetFields(IDbConnection connection, string tableName, IDbTransaction? transaction = null);

    /// <inheritdoc />
    public virtual ValueTask<IEnumerable<DbField>> GetFieldsAsync(IDbConnection connection, string tableName, IDbTransaction? transaction = null, CancellationToken cancellationToken = default) => new(GetFields(connection, tableName, transaction));

    /// <inheritdoc />
    public abstract IEnumerable<DbSchemaObject> GetSchemaObjects(IDbConnection connection, IDbTransaction? transaction = null);

    /// <inheritdoc />
    public virtual ValueTask<IEnumerable<DbSchemaObject>> GetSchemaObjectsAsync(IDbConnection connection, IDbTransaction? transaction = null, CancellationToken cancellationToken = default) => new(GetSchemaObjects(connection, transaction));

    /// <inheritdoc />
    public virtual object? ParameterValueToDb(object? value) => value;

    /// <inheritdoc />
    public virtual Func<object?>? PrepareForIdentityOutput(DbCommand command) => null;
}
