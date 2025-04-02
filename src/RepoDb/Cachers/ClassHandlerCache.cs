﻿using System.Collections.Concurrent;
using System.Reflection;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the mappings between a class property and a <see cref="IClassHandler{TEntity}"/> object.
/// </summary>
public static class ClassHandlerCache
{
    #region Privates

    private static readonly ConcurrentDictionary<int, object> cache = new();
    private static readonly IResolver<Type, object> resolver = new ClassHandlerResolver();

    #endregion

    #region Methods

    /// <summary>
    /// Gets the cached <see cref="IClassHandler{TEntity}"/> object that is being mapped to a specific .NET CLR type.
    /// </summary>
    /// <typeparam name="TType">The .NET CLR type.</typeparam>
    /// <typeparam name="TClassHandler">The type of the handler.</typeparam>
    /// <returns>The mapped <see cref="IClassHandler{TEntity}"/> object of the .NET CLR type.</returns>
    public static TClassHandler Get<TType, TClassHandler>() =>
        Get<TClassHandler>(typeof(TType));

    /// <summary>
    /// Gets the cached <see cref="IClassHandler{TEntity}"/> object that is being mapped to a specific .NET CLR type.
    /// </summary>
    /// <typeparam name="TClassHandler">The type of the handler.</typeparam>
    /// <param name="type">The target .NET CLR type.</param>
    /// <returns>The mapped <see cref="IClassHandler{TEntity}"/> object of the .NET CLR type.</returns>
    public static TClassHandler Get<TClassHandler>(Type type)
    {
        // Validate
        ThrowArgumentNullException(type, "Type");

        // Variables
        var key = GenerateHashCode(type);

        // Try get the value
        var value = cache.GetOrAdd(key, (_) => resolver.Resolve(type));

        return Converter.ToType<TClassHandler>(value);
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Flushes all the existing cached <see cref="IClassHandler{TEntity}"/> objects.
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
    /// Generates a hashcode for caching.
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <param name="propertyInfo">The instance of <see cref="PropertyInfo"/>.</param>
    /// <returns>The generated hashcode.</returns>
    private static int GenerateHashCode(Type entityType,
        PropertyInfo propertyInfo) =>
        TypeExtension.GenerateHashCode(entityType, propertyInfo);

    /// <summary>
    /// Validates the target object presence.
    /// </summary>
    /// <typeparam name="T">The type of the object.</typeparam>
    /// <param name="obj">The object to be checked.</param>
    /// <param name="argument">The name of the argument.</param>
    private static void ThrowArgumentNullException<T>(T obj,
        string argument)
    {
        if (obj == null)
        {
            throw new ArgumentNullException($"The argument '{argument}' cannot be null.");
        }
    }

    #endregion
}
