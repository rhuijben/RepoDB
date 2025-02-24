using System.Globalization;

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
    /// <typeparam name="T"></typeparam>
    /// <param name="obj"></param>
    /// <param name="argument"></param>
    internal static void ThrowIfNull<T>(T obj,
        string argument)
    {
        if (obj != null)
        {
            return;
        }
        if (string.IsNullOrEmpty(argument))
        {
            throw new ArgumentNullException();
        }
        else
        {
            throw new ArgumentNullException($"The argument '{argument}' cannot be null.");
        }
    }
}
