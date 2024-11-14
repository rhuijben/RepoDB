﻿using System;
using Npgsql;
using NpgsqlTypes;

namespace RepoDb.Attributes.Parameter.Npgsql
{
    /// <summary>
    /// An attribute used to define a value to the <see cref="NpgsqlParameter.NpgsqlDbType"/>
    /// property via an entity property before the actual execution.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class NpgsqlDbTypeAttribute : PropertyValueAttribute
    {
        /// <summary>
        /// Creates a new instance of <see cref="NpgsqlDbTypeAttribute"/> class.
        /// </summary>
        /// <param name="npgsqlDbType">The target <see cref="NpgsqlTypes.NpgsqlDbType"/> value.</param>
        public NpgsqlDbTypeAttribute(NpgsqlDbType npgsqlDbType)
            : base(typeof(NpgsqlParameter), nameof(NpgsqlParameter.NpgsqlDbType), npgsqlDbType)
        { }

        /// <summary>
        /// Gets the mapped <see cref="NpgsqlTypes.NpgsqlDbType"/> value of the parameter.
        /// </summary>
        public NpgsqlDbType NpgsqlDbType => (NpgsqlDbType)Value;
    }
}
