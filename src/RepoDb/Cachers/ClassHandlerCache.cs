using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the mappings between a class property and a <see cref="IClassHandler{TEntity}"/> object.
/// </summary>
public static class ClassHandlerCache
{
    #region Privates

    private static readonly ConcurrentDictionary<Type, object> cache = new();
    private static readonly IResolver<Type, PropertyInfo, object> propertyLevelResolver = new PropertyHandlerPropertyLevelResolver();
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
        ThrowArgumentNullException(type, nameof(type));

        // Try get the value
        var value = cache.GetOrAdd(type, resolver.Resolve);

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
