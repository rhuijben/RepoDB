using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RepoDb.Interfaces;

namespace RepoDb.TestCore;

public sealed class StoreQueryTracer : ITrace
{
    public void AfterExecution<TResult>(ResultTraceLog<TResult> log)
    {
    }

    public Task AfterExecutionAsync<TResult>(ResultTraceLog<TResult> log, CancellationToken cancellationToken = default) => Task.CompletedTask;

    public void BeforeExecution(CancellableTraceLog log)
    {
        Traces.Add(log.Statement);
    }

    public Task BeforeExecutionAsync(CancellableTraceLog log, CancellationToken cancellationToken = default)
    {
        Traces.Add(log.Statement);

        return Task.CompletedTask;
    }

    public List<string> Traces = new();
}
