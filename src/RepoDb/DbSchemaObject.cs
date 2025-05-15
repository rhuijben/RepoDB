using RepoDb.Enumerations;

namespace RepoDb;

public sealed record class DbSchemaObject
{
    public DbSchemaType Type { get; init; }
    public string Name { get; init; }
    public string? Schema { get; init; }
}


