using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace RepoDb.Extensions;

/// <summary>
/// An extension class for <see cref="IEnumerable{T}"/>.
/// </summary>
public static class EnumerableExtension
{
    /// <summary>
    /// Split the enumerable into multiple enumerables.
    /// </summary>
    /// <typeparam name="T">The target dynamic type of the enumerable.</typeparam>
    /// <param name="value">The actual enumerable instance.</param>
    /// <param name="sizePerSplit">The sizes of the items per split.</param>
    /// <returns>An enumerable of enumerables.</returns>
    public static IEnumerable<T[]> Split<T>(this IEnumerable<T> value,
        int sizePerSplit)
    {
        var count = value?.Count() ?? 0;
        if (sizePerSplit <= 0 || count <= sizePerSplit)
        {
            return new[] { value.ToArray() };
        }
#if !NET
        else
        {
            var batchCount = (count / sizePerSplit) + ((count % sizePerSplit) != 0 ? 1 : 0);
            var array = new T[batchCount][];
            for (var i = 0; i < batchCount; i++)
            {
                array[i] = value.Where((_, index) =>
                    {
                        return index >= (sizePerSplit * i) &&
                            index < (sizePerSplit * i) + sizePerSplit;
                    })
                    .ToArray();
            }
            return array;
        }
#else
        return value.Chunk(sizePerSplit);
#endif
    }

    /// <summary>
    /// Splits a collection into optimally-sized chunks to minimize cached database plan overhead.
    /// Uses a small set of standard chunk sizes to maximize cache reuse while maintaining efficiency.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source collection.</typeparam>
    /// <param name="source">The source collection to chunk.</param>
    /// <param name="maxChunkSize">The maximum size for any single chunk. Defaults to 2000.</param>
    /// <returns>
    /// An enumerable of arrays, where each array represents a chunk of the original collection.
    /// Small collections (≤10 items) are returned as-is without chunking.
    /// </returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="source"/> is null.</exception>
    /// <remarks>
    /// <para>
    /// This method optimizes for database operations that cache execution plans based on parameter count.
    /// By using a limited set of standard chunk sizes (20, 50, 100, 200, 500, etc.), it minimizes
    /// the number of cached plans while maintaining good performance characteristics.
    /// </para>
    /// <para>
    /// Chunking strategy:
    /// <list type="bullet">
    /// <item><description>Collections with ≤10 items: No chunking (returned as single chunk)</description></item>
    /// <item><description>Larger collections: Uses standard sizes progressing through 20, 50, 100, 200, 500, 1000, 2000, 5000...</description></item>
    /// <item><description>Always includes the specified <paramref name="maxChunkSize"/> for optimal large dataset handling</description></item>
    /// <item><description>Automatically drops standard sizes that are too close to the maximum (within 20%) to avoid redundancy</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Performance characteristics:
    /// <list type="bullet">
    /// <item><description>Memory efficient: Copies source once, then uses array slicing</description></item>
    /// <item><description>Cache friendly: Typically generates only 5-8 different chunk sizes total</description></item>
    /// <item><description>Large dataset optimized: Always uses maximum chunk size when beneficial</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// var items = Enumerable.Range(1, 2500);
    /// 
    /// // Creates chunks of sizes: 2000, 500 (2 different cached plans)
    /// var chunks = items.ChunkOptimally(maxChunkSize: 2000);
    /// 
    /// // Small collection optimization
    /// var smallItems = Enumerable.Range(1, 8);
    /// var smallChunks = smallItems.ChunkOptimally(); // Returns single chunk of 8 items
    /// </code>
    /// </example>
    public static IEnumerable<T[]> ChunkOptimally<T>(
        this IEnumerable<T> source,
        int maxChunkSize = 2000)
    {
        if (source is null)
            throw new ArgumentNullException(nameof(source));
        else if (maxChunkSize < 1)
            throw new ArgumentOutOfRangeException(nameof(maxChunkSize));

        var array = source as T[] ?? source.ToArray();

        if (array.Length == 0)
            yield break;

        if (array.Length <= 10)
        {
            yield return array;
            yield break;
        }

        var standardSizes = GetStandardSizes(maxChunkSize);

        for (int i = 0; i < array.Length;)
        {
            var remaining = array.Length - i;
            var chunkSize = SelectOptimalSize(remaining, standardSizes);

            yield return array.AsSpan(i, chunkSize).ToArray();
            i += chunkSize;
        }

        static int[] GetStandardSizes(int maxChunkSize)
        {
            var baseSizes = new[] { 20, 50, 100, 200, 500, 1000, 2000, 5000 };
            var validSizes = new List<int>();

            foreach (var size in baseSizes)
            {
                if (size >= maxChunkSize) break;

                // Skip if too close to max (within 20% difference)
                if (size > maxChunkSize * 0.8) continue;

                validSizes.Add(size);
            }

            // Always include the provided maximum
            validSizes.Add(maxChunkSize);

            return validSizes.ToArray();
        }

        static int SelectOptimalSize(int remaining, int[] standardSizes)
        {
            // Find the largest standard size that fits
            for (int i = standardSizes.Length - 1; i >= 0; i--)
            {
                if (remaining >= standardSizes[i])
                    return standardSizes[i];
            }

            // Fallback to remaining (shouldn't happen with our logic)
            return remaining;
        }
    }

    /// <summary>
    /// Checks whether the instance of <see cref="IEnumerable"/> is of type <see cref="IEnumerable{T}"/>, then casts it, otherwise, 
    /// returns the instance of <see cref="IEnumerable{T}"/> with the specified items. The items that are not of type <typeparamref name="T"/> will be
    /// eliminated from the result. This method is using the underlying method <see cref="Enumerable.OfType{TResult}(IEnumerable)"/>.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="value">The actual enumerable instance.</param>
    /// <returns>The <see cref="IEnumerable{T}"/> object in which the items are of type <typeparamref name="T"/>.</returns>
    public static IEnumerable<T> WithType<T>(this IEnumerable value) =>
        value as IEnumerable<T> ?? value.OfType<T>();

    /// <summary>
    /// Checks whether the instance of <see cref="IEnumerable{T}"/> is of type <see cref="List{T}"/>, then casts it, otherwise, converts it.
    /// This method is using the underlying method <see cref="Enumerable.ToList{TSource}(IEnumerable{TSource})"/> method.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="value">The actual enumerable instance.</param>
    /// <returns>The converted <see cref="IList{T}"/> object.</returns>
    public static List<T> AsList<T>(this IEnumerable<T> value) =>
        value as List<T> ?? value?.ToList();

    /// <summary>
    /// Checks whether the instance of <see cref="IEnumerable{T}"/> is an array of <typeparamref name="T"/>, then casts it, otherwise, converts it.
    /// This method is using the underlying method <see cref="Enumerable.ToArray{TSource}(IEnumerable{TSource})"/> method.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="value">The actual enumerable instance.</param>
    /// <returns>The converted <see cref="Array"/> object.</returns>
    public static T[] AsArray<T>(this IEnumerable<T> value) =>
        value as T[] ?? value?.ToArray();

    /// <summary>
    /// Gets a value indicating whether the current collection is null or empty.
    /// </summary>
    /// <param name="value">The target type.</param>
    /// <typeparam name="T">The actual enumerable instance.</typeparam>
    /// <returns>A value indicating whether the collection is null or empty.</returns>
    public static bool IsNullOrEmpty<T>(
        [NotNullWhen(true)] this IEnumerable<T>? value) => value?.Any() != true;

#if NETSTANDARD
    /// <summary>
    /// CCreates a new <see cref="HashSet{T}"/> from an <see cref="IEnumerable{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the elements.</typeparam>
    /// <param name="source">The actual enumerable instance.</param>
    /// <param name="comparer">An <see cref="IEqualityComparer{T}"/> to compare keys.</param>
    /// <returns>The created <see cref="HashSet{T}"/> object.</returns>
    internal static HashSet<T> ToHashSet<T>(this IEnumerable<T> source, IEqualityComparer<T> comparer) =>
        new(source, comparer);

    /// <summary>
    /// Creates a new <see cref="HashSet{T}"/> from an <see cref="IEnumerable{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the elements=.</typeparam>
    /// <param name="source">The actual enumerable instance.</param>
    /// <returns>The created <see cref="HashSet{T}"/> object.</returns>
    internal static HashSet<T> ToHashSet<T>(this IEnumerable<T> source) => [.. source];
#endif
}
