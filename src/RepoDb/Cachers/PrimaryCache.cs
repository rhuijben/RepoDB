#nullable enable
using RepoDb.Extensions;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the primary property of the data entity.
/// </summary>
[Obsolete("Use .PrimaryKeyCache that supports keys of multiple properties/columns")]
public static class PrimaryCache
{
    /// <summary>
    /// Gets the cached primary property of the data entity.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <returns>The cached primary property.</returns>
    public static ClassProperty? Get<TEntity>()
        where TEntity : class =>
        Get(typeof(TEntity));

    /// <summary>
    /// Gets the cached primary property of the data entity.
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <returns>The cached primary property.</returns>
    public static ClassProperty? Get(Type entityType)
    {
        return PrimaryKeyCache.Get(entityType)?.OneOrDefault();
    }


    /// <summary>
    /// Flushes all the existing cached primary <see cref="ClassProperty"/> objects.
    /// </summary>
    public static void Flush() =>
        PrimaryKeyCache.Flush();
}
