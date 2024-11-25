using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace RepoDb.TestCore
{
    public abstract class DbInstance : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {

        }

        internal async Task ClassInitializeAsync(TestContext context)
        {

        }
    }

}
