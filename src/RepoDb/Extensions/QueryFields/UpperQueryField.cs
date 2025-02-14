﻿using RepoDb.Enumerations;
using System.Data;

namespace RepoDb.Extensions.QueryFields;

/// <summary>
/// A functional-based <see cref="QueryField"/> object that is using the UPPER function.
/// </summary>
public sealed class UpperQueryField : FunctionalQueryField
{
    #region Constructors

    /// <summary>
    /// Creates a new instance of <see cref="UpperQueryField"/> object.
    /// </summary>
    /// <param name="fieldName">The name of the field for the query expression.</param>
    /// <param name="value">The value to be used for the query expression.</param>
    public UpperQueryField(string fieldName,
        object value)
        : this(fieldName, Operation.Equal, value, null)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="UpperQueryField"/> object.
    /// </summary>
    /// <param name="fieldName">The name of the field for the query expression.</param>
    /// <param name="value">The value to be used for the query expression.</param>
    /// <param name="dbType">The database type to be used for the query expression.</param>
    public UpperQueryField(string fieldName,
        object value,
        DbType? dbType)
        : this(fieldName, Operation.Equal, value, dbType)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="UpperQueryField"/> object.
    /// </summary>
    /// <param name="fieldName">The name of the field for the query expression.</param>
    /// <param name="operation">The operation to be used for the query expression.</param>
    /// <param name="value">The value to be used for the query expression.</param>
    public UpperQueryField(string fieldName,
        Operation operation,
        object value)
        : this(fieldName, operation, value, null)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="UpperQueryField"/> object.
    /// </summary>
    /// <param name="fieldName">The name of the field for the query expression.</param>
    /// <param name="operation">The operation to be used for the query expression.</param>
    /// <param name="value">The value to be used for the query expression.</param>
    /// <param name="dbType">The database type to be used for the query expression.</param>
    public UpperQueryField(string fieldName,
        Operation operation,
        object value,
        DbType? dbType)
        : base(fieldName, operation, value, dbType, "UPPER({0})")
    { }

    #endregion
}
