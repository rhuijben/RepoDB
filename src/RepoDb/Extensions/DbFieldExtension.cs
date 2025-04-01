﻿namespace RepoDb.Extensions;

/// <summary>
/// Contains the extension methods for <see cref="Field"/> object.
/// </summary>
public static class DbFieldExtension
{
    /// <summary>
    /// Converts an instance of a <see cref="DbField"/> into an <see cref="IEnumerable{T}"/> of <see cref="DbField"/> object.
    /// </summary>
    /// <param name="dbField">The <see cref="DbField"/> to be converted.</param>
    /// <returns>An <see cref="IEnumerable{T}"/> list of <see cref="DbField"/> object.</returns>
    public static IEnumerable<DbField> AsEnumerable(this DbField dbField)
    {
        yield return dbField;
    }

    /// <summary>
    /// Converts an instance of a <see cref="DbField"/> into <see cref="Field"/> object.
    /// </summary>
    /// <param name="dbField">The <see cref="DbField"/> to be converted.</param>
    /// <returns>An instance of <see cref="Field"/> object.</returns>
    public static Field AsField(this DbField dbField) =>
        new(dbField.Name, dbField.Type);

    /// <summary>
    /// Converts the list of <see cref="DbField"/> objects into an <see cref="IEnumerable{T}"/> of <see cref="Field"/> objects.
    /// </summary>
    /// <param name="dbFields">The <see cref="DbField"/> to be converted.</param>
    /// <returns>An <see cref="IEnumerable{T}"/> list of <see cref="Field"/> object.</returns>
    public static IEnumerable<Field> AsFields(this IEnumerable<DbField> dbFields)
    {
        foreach (var dbField in dbFields)
        {
            yield return dbField.AsField();
        }
    }

    /// <summary>
    /// Converts the list of <see cref="DbField"/> objects into an <see cref="IReadOnlyList{T}"/> of <see cref="Field"/> objects.
    /// </summary>
    /// <param name="dbFields">The <see cref="DbField"/> to be converted.</param>
    /// <returns>An <see cref="IEnumerable{T}"/> list of <see cref="Field"/> object.</returns>
    public static IEnumerable<Field> AsFields(this IReadOnlyList<DbField> dbFields)
    {
        var result = new List<Field>(dbFields.Count);

        foreach (var dbField in dbFields)
        {
            result.Add(dbField.AsField());
        }

        return result;
    }

    internal static TItem? OneOrDefault<TItem>(this IEnumerable<TItem> source)
    {
#if NET
        ArgumentNullException.ThrowIfNull(source);
#endif
        TItem? first = default;
        bool isFirst = true;

        foreach (var v in source)
        {
            if (isFirst)
            {
                first = v;
                isFirst = false;
            }
            else
                return default;
        }

        return isFirst ? default : first;
    }
}

