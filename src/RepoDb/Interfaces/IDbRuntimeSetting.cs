namespace RepoDb.Interfaces;

internal interface IDbRuntimeSetting : IDbSetting
{
    DbConnectionRuntimeInformation RuntimeInfo { get; }
}
