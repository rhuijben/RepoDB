using System.Data;
using System.Data.Common;

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

public static class CompatExtensions
{
    public static bool StartsWith(this string v, char value)
    {
        return v.StartsWith(value.ToString());
    }
}

}
#endif

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
