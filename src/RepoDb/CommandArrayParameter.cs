using System.Data.Common;

namespace RepoDb;

/// <summary>
/// A class that is being used to handle the array value of the parameter.
/// </summary>
internal sealed class CommandArrayParameter
{
    public required DbCommand Command { get; init; }
    public required string ParameterName { get; init; }
    public required System.Collections.IEnumerable Values { get; init; }
}
