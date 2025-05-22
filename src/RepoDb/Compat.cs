using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

#if !NET
namespace System.Runtime.CompilerServices
{

// Required to allow init properties in netstandard
internal sealed class IsExternalInit : Attribute
{
}
}

namespace System
{

internal static class CompatExtensions
{
    public static bool StartsWith(this string v, char value)
    {
        return v.Length > 0 && v[0] == value;
    }

    public static bool EndsWith(this string v, char value)
    {
        return v.Length > 0 && v[v.Length-1] == value;
    }
}

}

namespace System.Diagnostics.CodeAnalysis
{
    //
    // Summary:
    //     Specifies that when a method returns System.Diagnostics.CodeAnalysis.NotNullWhenAttribute.ReturnValue,
    //     the parameter will not be null even if the corresponding type allows it.
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    internal sealed class NotNullWhenAttribute : Attribute
    {
        //
        // Summary:
        //     Initializes the attribute with the specified return value condition.
        //
        // Parameters:
        //   returnValue:
        //     The return value condition. If the method returns this value, the associated
        //     parameter will not be null.
        public NotNullWhenAttribute(bool returnValue)
        {
            ReturnValue = returnValue; 
        }

        //
        // Summary:
        //     Gets the return value condition.
        //
        // Returns:
        //     The return value condition. If the method returns this value, the associated
        //     parameter will not be null.
        public bool ReturnValue { get; }
    }
}
#endif

namespace RepoDb
{
    internal static class NetCompatExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
#pragma warning disable CS1998 // Async function should await
        public static async ValueTask<IDbTransaction> BeginTransactionAsync(this IDbConnection dbConnection, CancellationToken cancellationToken = default)
#pragma warning restore CS1998 // Async function should await
        {
#if NET
            if (dbConnection is DbConnection dbc)
                return await dbc.BeginTransactionAsync(cancellationToken);
#endif
            return dbConnection.BeginTransaction();
        }

        public static async ValueTask CommitAsync(this IDbTransaction dbTransaction, CancellationToken cancellationToken = default)
        {
            if (dbTransaction is DbTransaction dbTransaction1)
                await dbTransaction1.CommitAsync(cancellationToken);
            else
                dbTransaction.Commit();
        }
    }


    internal static partial class AsyncEnumerable
    {
        /// <summary>Creates a list from an <see cref="IAsyncEnumerable{T}"/>.</summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">An <see cref="IEnumerable{T}"/> to create a list from.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> to monitor for cancellation requests. The default is <see cref="CancellationToken.None"/>.</param>
        /// <returns>A list that contains the elements from the input sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source" /> is <see langword="null" />.</exception>
        public static ValueTask<List<TSource>> ToListAsync<TSource>(
            this IAsyncEnumerable<TSource> source,
            CancellationToken cancellationToken = default)
        {
#if NET
            ArgumentNullException.ThrowIfNull(source);
#else
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }
#endif

            return Impl(source.WithCancellation(cancellationToken));

            static async ValueTask<List<TSource>> Impl(
                ConfiguredCancelableAsyncEnumerable<TSource> source)
            {
                List<TSource> list = [];
                await foreach (TSource element in source)
                {
                    list.Add(element);
                }

                return list;
            }
        }
    }
}
