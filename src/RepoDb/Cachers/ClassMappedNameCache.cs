﻿#nullable enable
using System.Collections.Concurrent;
using RepoDb.Extensions;
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
        ObjectExtension.ThrowIfNull(entityType, nameof(entityType));

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

    #endregion
}
