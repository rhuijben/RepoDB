﻿using System.Data.Common;

namespace RepoDb.Contexts.Execution;

/// <summary>
/// 
/// </summary>
internal class UpdateAllExecutionContext
{
    /// <summary>
    /// 
    /// </summary>
    public string CommandText { get; set; }

    /// <summary>
    /// 
    /// </summary>
    public IEnumerable<DbField> InputFields { get; set; }

    /// <summary>
    /// 
    /// </summary>
    public Action<DbCommand, object> SingleDataEntityParametersSetterFunc { get; set; }

    /// <summary>
    /// 
    /// </summary>
    public Action<DbCommand, IList<object>> MultipleDataEntitiesParametersSetterFunc { get; set; }
}
