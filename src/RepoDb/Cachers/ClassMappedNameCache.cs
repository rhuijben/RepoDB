﻿using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the database object name mappings of the data entity type.
/// </summary>
public static class ClassMappedNameCache
{
    private static readonly ConcurrentDictionary<int, string> cache = new();
    private static readonly IResolver<Type, string> resolver = new ClassMappedNameResolver();

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

        // Variables
        var key = GenerateHashCode(entityType);

        // Try get the value
        return cache.GetOrAdd(key, resolver.Resolve(entityType));
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Flushes all the existing cached class mapped names.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    /// <summary>
    /// Generates a hashcode for caching.
    /// </summary>
    /// <param name="type">The type of the data entity.</param>
    /// <returns>The generated hashcode.</returns>
    private static int GenerateHashCode(Type type) =>
        TypeExtension.GenerateHashCode(type);

    /// <summary>
    /// Validates the target object presence.
    /// </summary>
    /// <param name="obj">The object to be checked.</param>
    /// <param name="argument">The name of the argument.</param>
    private static void ThrowArgumentNullException(
#if NET
        [NotNull]
#endif
    object? obj,
        string argument)
    {
#if NET
        ArgumentNullException.ThrowIfNull(obj, argument);
#else
        if (obj == null)
        {
            throw new ArgumentNullException($"The argument '{argument}' cannot be null.");
        }
#endif
    }

    #endregion
}
