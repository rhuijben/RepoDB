using RepoDb.Interfaces;

namespace RepoDb.TestCore;

public sealed class StoreQueryTracer : ITrace
{
    public void AfterExecution<TResult>(ResultTraceLog<TResult> log)
    {
    }

    public ValueTask AfterExecutionAsync<TResult>(ResultTraceLog<TResult> log, CancellationToken cancellationToken = default) => new();

    public void BeforeExecution(CancellableTraceLog log)
    {
        Traces.Add(log.Statement);
    }

    public ValueTask BeforeExecutionAsync(CancellableTraceLog log, CancellationToken cancellationToken = default)
    {
        Traces.Add(log.Statement);

        return new();
    }

    public List<string> Traces = [];
}
