﻿using System;
using System.Data.SqlTypes;
using Microsoft.Data.SqlClient;

namespace RepoDb.Attributes.Parameter.SqlServer
{
    /// <summary>
    /// An attribute used to define a value to the <see cref="SqlParameter.CompareInfo"/>
    /// property via an entity property before the actual execution.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class CompareInfoAttribute : PropertyValueAttribute
    {
        /// <summary>
        /// Creates a new instance of <see cref="CompareInfoAttribute"/> class.
        /// </summary>
        /// <param name="compareInfo">The value that determines how the string comparission is being defined.</param>
        public CompareInfoAttribute(SqlCompareOptions compareInfo)
            : base(typeof(SqlParameter), nameof(SqlParameter.CompareInfo), compareInfo)
        { }

        /// <summary>
        /// Gets the mapped value that determines how the string comparission is being defined on the parameter.
        /// </summary>
        public SqlCompareOptions CompareInfo => (SqlCompareOptions)Value;
    }
}
