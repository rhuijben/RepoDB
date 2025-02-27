using System.Data.Common;

namespace RepoDb.Contexts.Execution;

/// <summary>
/// 
/// </summary>
internal class UpdateAllExecutionContext
{
    /// <summary>
    /// 
    /// </summary>
    public string CommandText { get; init; }

    /// <summary>
    /// 
    /// </summary>
    public IEnumerable<DbField> InputFields { get; init; }

    /// <summary>
    /// 
    /// </summary>
    public Action<DbCommand, object> SingleDataEntityParametersSetterFunc { get; init; }

    /// <summary>
    /// 
    /// </summary>
    public Action<DbCommand, IList<object>> MultipleDataEntitiesParametersSetterFunc { get; init; }
}
