using RepoDb.Interfaces;

namespace RepoDb.Resolvers;

public class OracleDbTypeToClientTypeResolver : IResolver<string, Type>
{
    /// <inheritdoc/>
    public Type Resolve(string dbTypeName)
    {
        if (dbTypeName == null)
            throw new ArgumentNullException(nameof(dbTypeName), "The DB Type name must not be null.");

        return dbTypeName.ToLowerInvariant() switch
        {
            // Numeric types
            "number" => typeof(decimal),
            "binary_float" => typeof(float),
            "binary_double" => typeof(double),
            "float" => typeof(double),
            "real" => typeof(float),
            "int" or "integer" or "smallint" => typeof(int),
            "decimal" => typeof(decimal),
            "numeric" => typeof(decimal),

            // Character types
            "char" => typeof(string),
            "nchar" => typeof(string),
            "varchar" => typeof(string),
            "varchar2" => typeof(string),
            "nvarchar2" => typeof(string),
            "clob" or "nclob" => typeof(string),
            "long" => typeof(string),

            // Binary types
            "raw" => typeof(byte[]),
            "long raw" => typeof(byte[]),
            "blob" => typeof(byte[]),
            "bfile" => typeof(byte[]), // often accessed as a stream/file

            // Date/time types
            "date" => typeof(DateTime),
            "timestamp" => typeof(DateTime),
            "timestamp with time zone" => typeof(DateTimeOffset),
            "timestamp with local time zone" => typeof(DateTime),
            "interval year to month" => typeof(string), // Could also be custom struct
            "interval day to second" => typeof(TimeSpan),

            // Boolean (simulated in older Oracle versions)
            "boolean" => typeof(bool), // Only supported in 12c+ PL/SQL or 23c SQL

            // Row IDs
            "rowid" => typeof(string),
            "urowid" => typeof(string),

            // XML
            "xmltype" => typeof(string),

            // JSON (Oracle 21c+ supports JSON natively as datatype)
            "json" => typeof(string),

            // Defaults
            _ => typeof(object),
        };
    }
}
