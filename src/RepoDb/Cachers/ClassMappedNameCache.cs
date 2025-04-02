using System.Collections.Concurrent;
using RepoDb.Resolvers;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the database object name mappings of the data entity type.
/// </summary>
public static class ClassMappedNameCache
{
    private static readonly ConcurrentDictionary<Type, string> cache = new();
    private static readonly ClassMappedNameResolver resolver = new();

    #region Methods

    /// <summary>
    /// Gets the cached database object name of the data entity type.
    /// </summary>
    /// <typeparam name="T">The type of the target type.</typeparam>
    /// <returns>The cached mapped name of the data entity.</returns>
    public static string Get<T>() =>
        Get(typeof(T));

    /// <summary>
    /// Gets the cached database object name of the data entity type.
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <returns>The cached mapped name of the data entity.</returns>
    public static string Get(Type entityType)
    {
        // Validate
        ThrowArgumentNullException(entityType, "EntityType");

        // Try get the value
        return cache.GetOrAdd(entityType, resolver.Resolve);
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Flushes all the existing cached class mapped names.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    /// <summary>
    /// Validates the target object presence.
    /// </summary>
    /// <typeparam name="T">The type of the object.</typeparam>
    /// <param name="obj">The object to be checked.</param>
    /// <param name="argument">The name of the argument.</param>
    private static void ThrowArgumentNullException<T>(T obj,
        string argument)
    {
        if (obj is null)
        {
            throw new ArgumentNullException(argument);
        }
    }

    #endregion
}
