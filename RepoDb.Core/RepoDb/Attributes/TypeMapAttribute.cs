using System;
using System.Data;
using RepoDb.Attributes.Parameter;

namespace RepoDb.Attributes
{
    /// <summary>
    /// An attribute that is used to define a mapping between the .NET CLR <see cref="Type"/> and the <see cref="System.Data.DbType"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class TypeMapAttribute : DbTypeAttribute
    {
        /// <summary>
        /// Creates a new instance of <see cref="TypeMapAttribute"/> class.
        /// </summary>
        /// <param name="dbType">The equivalent <see cref="System.Data.DbType"/> value of the parameter.</param>
        public TypeMapAttribute(DbType dbType)
            : base(dbType)
        { }
    }
}
