#nullable enable
using System.Diagnostics.CodeAnalysis;

namespace RepoDb.Extensions;

/// <summary>
/// Contains the extension methods for <see cref="ClassProperty"/>.
/// </summary>
public static class ClassPropertyExtension
{
    /// <summary>
    /// Converts the list of <see cref="ClassProperty"/> into a a list of <see cref="Field"/> objects.
    /// </summary>
    /// <param name="properties">The current instance of <see cref="ClassProperty"/>.</param>
    /// <returns>A list of <see cref="string"/> objects.</returns>
#if NET
    [return: NotNullIfNotNull(nameof(properties))]
#endif
    public static IEnumerable<Field>? AsFields(this IEnumerable<ClassProperty>? properties)
    {
        return properties?.Select(x => x.AsField());
    }

    /// <summary>
    /// Converts the list of <see cref="ClassProperty"/> into a a list of <see cref="Field"/> objects.
    /// </summary>
    /// <param name="properties">The current instance of <see cref="ClassProperty"/>.</param>
    /// <returns>A list of <see cref="string"/> objects.</returns>
    public static IEnumerable<Field> AsFields(this IList<ClassProperty> properties) =>
        AsFields(properties.AsEnumerable());


    /// <summary>
    /// Retrieves the first ClassProperty from a collection that matches a specified mapped name, considering case
    /// sensitivity options.
    /// </summary>
    /// <param name="source">The collection of ClassProperty instances to search through.</param>
    /// <param name="mappedName">The name to match against the mapped names of ClassProperty instances.</param>
    /// <param name="comparison">Specifies how to compare the mapped names, allowing for case-sensitive or case-insensitive matching.</param>
    /// <returns>Returns the first matching ClassProperty or null if no match is found.</returns>
    public static ClassProperty? GetByMappedName(this IEnumerable<ClassProperty> source, string mappedName, StringComparison comparison = StringComparison.OrdinalIgnoreCase)
        => source.FirstOrDefault(x => string.Equals(x.GetMappedName(), mappedName, comparison));
}
