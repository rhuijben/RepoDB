using RepoDb.Interfaces;

namespace RepoDb.Builders;

public record struct OperationConfigureArg
{
    public string TableName;
    public int CommandTimeout;
    public string Hints;
    public ITrace trace;
    public IStatementBuilder? StatementBuilder => null;
}

public readonly struct ConfigureArgumentBuilder
{
    readonly object? _settings;

    internal OperationConfigureArg Build()
    {
        return new();
    }

    public ConfigureArgumentBuilder(Func<OperationConfigureArg> build)
    {
        _settings = build();
    }

    public ConfigureArgumentBuilder(OperationConfigureArg arg)
    {
        _settings = arg;
    }

    public static implicit operator ConfigureArgumentBuilder(Func<OperationConfigureArg> build)
    {
        return new(build);
    }

    public static implicit operator ConfigureArgumentBuilder(OperationConfigureArg arg)
    {
        return new(arg);
    }

    internal OperationConfigureArg Build<TEntity>(TEntity? entity)
        where TEntity : class
    {
        var stored = _settings is OperationConfigureArg ca ? ca : new();

        return stored with
        {
            TableName = stored.TableName ?? DbConnectionExtension.GetMappedName(entity)
        };
    }
}
