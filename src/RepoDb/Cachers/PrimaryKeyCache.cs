﻿#nullable enable
using System.Collections.Concurrent;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the primary property of the data entity.
/// </summary>
public static class PrimaryKeyCache
{
    private static readonly ConcurrentDictionary<Type, IEnumerable<ClassProperty>> cache = new();
    private static readonly IResolver<Type, IEnumerable<ClassProperty>> resolver = new PrimaryResolver();

    #region Methods

    /// <summary>
    /// Gets the cached primary property of the data entity.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <returns>The cached primary property.</returns>
    public static IEnumerable<ClassProperty>? Get<TEntity>()
        where TEntity : class =>
        Get(typeof(TEntity));

    /// <summary>
    /// Gets the cached primary property of the data entity.
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <returns>The cached primary property.</returns>
    public static IEnumerable<ClassProperty>? Get(Type entityType)
    {
#if NET
        ArgumentNullException.ThrowIfNull(entityType);
#else
        if (entityType is null)
            throw new ArgumentNullException(nameof(entityType));
#endif
        return cache.GetOrAdd(entityType, (_) => resolver.Resolve(entityType) ?? []);
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Flushes all the existing cached primary <see cref="ClassProperty"/> objects.
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

    #endregion
}
