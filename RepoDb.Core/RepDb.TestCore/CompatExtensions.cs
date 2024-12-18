using System.Data.Common;
using System.Threading.Tasks;

namespace RepoDb.TestCore
{
    public static class CompatExtensions
    {
#if !NET
        public static ValueTask<DbTransaction> BeginTransactionAsync(this DbConnection connection)
        {
            return new(connection.BeginTransaction());
        }

        public static Task RollbackAsync(this DbTransaction transaction)
        {
            transaction.Rollback();

            return Task.CompletedTask;
        }
#endif
    }
}
