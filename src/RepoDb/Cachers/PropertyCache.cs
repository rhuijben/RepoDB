﻿#nullable enable
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using RepoDb.Extensions;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the properties of the data entity.
/// </summary>
public static class PropertyCache
{
    private static readonly ConcurrentDictionary<Type, IEnumerable<ClassProperty>> cache = new();

    #region Methods

    /// <summary>
    /// Gets the cached <see cref="ClassProperty"/> object of the data entity (via expression).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="expression">The expression to be parsed.</param>
    /// <returns>The instance of cached <see cref="ClassProperty"/> object.</returns>
    public static ClassProperty? Get<TEntity>(Expression<Func<TEntity, object?>> expression)
        where TEntity : class =>
        Get(typeof(TEntity), ExpressionExtension.GetProperty<TEntity>(expression), false);

    /// <summary>
    /// Gets the cached <see cref="ClassProperty"/> object of the data entity (via property name).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="propertyName">The name of the property.</param>
    /// <param name="includeMappings">True to evaluate the existing mappings.</param>
    /// <returns>The instance of cached <see cref="ClassProperty"/> object.</returns>
    public static ClassProperty? Get<TEntity>(string propertyName,
        bool includeMappings = false)
        where TEntity : class =>
        Get(typeof(TEntity), propertyName, includeMappings);

    /// <summary>
    /// Gets the cached <see cref="ClassProperty"/> object of the data entity (via property name).
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <param name="propertyName">The name of the property.</param>
    /// <param name="includeMappings">True to evaluate the existing mappings.</param>
    /// <returns>The instance of cached <see cref="ClassProperty"/> object.</returns>
    public static ClassProperty? Get(Type? entityType,
        string propertyName,
        bool includeMappings = false)
    {
        ObjectExtension.ThrowIfNull(propertyName, nameof(propertyName));

        // Return the value
        return Get(entityType)?
            .FirstOrDefault(p =>
                string.Equals(p.PropertyInfo.Name, propertyName, StringComparison.OrdinalIgnoreCase) ||
                (includeMappings && string.Equals(p.GetMappedName(), propertyName, StringComparison.OrdinalIgnoreCase)));
    }

    /// <summary>
    /// Gets the cached <see cref="ClassProperty"/> object of the data entity (via <see cref="Field"/> object).
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="field">The instance of the <see cref="Field"/> object.</param>
    /// <param name="includeMappings">True to evaluate the existing mappings.</param>
    /// <returns>The instance of cached <see cref="ClassProperty"/> object.</returns>
    public static ClassProperty? Get<TEntity>(Field field,
        bool includeMappings = false)
        where TEntity : class =>
        Get(typeof(TEntity), field, includeMappings);

    /// <summary>
    /// Gets the cached <see cref="ClassProperty"/> object of the data entity (via <see cref="Field"/> object).
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <param name="field">The instance of the <see cref="Field"/> object.</param>
    /// <param name="includeMappings">True to evaluate the existing mappings.</param>
    /// <returns>The instance of cached <see cref="ClassProperty"/> object.</returns>
    public static ClassProperty? Get(Type? entityType,
        Field field,
        bool includeMappings = false)
    {
        // Validate the presence
        ObjectExtension.ThrowIfNull(field, nameof(field));

        // Return the value
        return Get(entityType, field.Name, includeMappings);
    }

    /// <summary>
    /// Gets the cached <see cref="ClassProperty"/> object of the data entity (via <see cref="PropertyInfo"/> object).
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <param name="propertyInfo">The instance of the <see cref="PropertyInfo"/> object.</param>
    /// <param name="includeMappings">True to evaluate the existing mappings.</param>
    /// <returns>The instance of cached <see cref="ClassProperty"/> object.</returns>
    internal static ClassProperty? Get(Type? entityType,
        PropertyInfo propertyInfo,
        bool includeMappings = false)
    {
        // Validate the presence
        ObjectExtension.ThrowIfNull(propertyInfo, nameof(propertyInfo));

        // Return the value
        return Get(entityType, propertyInfo.Name, includeMappings);
    }

    /// <summary>
    /// Gets the cached list of <see cref="ClassProperty"/> objects of the data entity.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <returns>The cached list <see cref="ClassProperty"/> objects.</returns>
    public static IEnumerable<ClassProperty> Get<TEntity>()
        where TEntity : class =>
        Get(typeof(TEntity));

    /// <summary>
    /// Gets the cached list of <see cref="ClassProperty"/> objects of the data entity.
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <returns>The cached list <see cref="ClassProperty"/> objects.</returns>
    public static IEnumerable<ClassProperty> Get(Type? entityType)
    {
        if (entityType is null || TypeCache.Get(entityType).IsClassType() != true)
        {
            return [];
        }

        return cache.GetOrAdd(entityType, (_) => entityType.GetClassProperties().AsList());
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Flushes all the existing cached enumerable of <see cref="ClassProperty"/> objects.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    #endregion
}
