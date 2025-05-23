namespace RepoDb;

/// <summary>
/// A class that is being used to initialize the necessary settings for the <see cref="NpgsqlConnection"/> object.
/// </summary>
public static partial class OracleGlobalConfiguration
{
    /// <summary>
    /// Initializes all the necessary settings for PostgreSql.
    /// </summary>
    /// <param name="globalConfiguration">The instance of the global configuration in used.</param>
    /// <returns>The used global configuration instance itself.</returns>
    public static GlobalConfiguration UseOracle(this GlobalConfiguration globalConfiguration)
    {
        OracleBootstrap.InitializeInternal();
        return globalConfiguration;
    }
}
