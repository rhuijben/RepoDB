using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace RepoDb.Extensions;

/// <summary>
/// Contains the extension methods for <see cref="Object"/>.
/// </summary>
internal static class ObjectExtension
{
    /// <summary>
    /// Converts an object to a <see cref="long"/>.
    /// </summary>
    /// <param name="value">The value to be converted.</param>
    /// <returns>A <see cref="long"/> value of the object.</returns>
    internal static long ToNumber(this object value) =>
        Convert.ToInt64(value, CultureInfo.InvariantCulture);

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="obj"></param>
    internal static void ThrowIfNull<T>(T obj) =>
        ThrowIfNull(obj, null);

    /// <summary>
    /// 
    /// </summary>
    /// <param name="argument"></param>
    /// <param name="parameterName"></param>
    internal static void ThrowIfNull(
#if NET
        [NotNull]
#endif
    object? argument,
#if NET
        [CallerArgumentExpression(nameof(argument))]
#endif
        string? paramName = null)
    {
#if NET
        ArgumentNullException.ThrowIfNull(argument, paramName);
#else
        if (argument != null)
        {
            return;
        }
        throw new ArgumentNullException(paramName: paramName);
#endif
    }
}
