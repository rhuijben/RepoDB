﻿using Oracle.ManagedDataAccess.Client;
using RepoDb.DbHelpers;
using RepoDb.DbSettings;
using RepoDb.StatementBuilders;

namespace RepoDb;

/// <summary>
/// A class used to initialize necessary objects that is connected to <see cref="NpgsqlConnection"/> object.
/// </summary>
internal static class OracleBootstrap
{
    #region Properties

    /// <summary>
    /// Gets the value indicating whether the initialization is completed.
    /// </summary>
    public static bool IsInitialized { get; private set; }

    #endregion

    #region Methods

    /// <summary>
    /// Initializes all necessary settings for PostgreSql.
    /// </summary>
    [Obsolete("This class will soon to be hidden as internal class. Use the 'GlobalConfiguration.Setup().UsePostgreSql()' method instead.")]
    public static void Initialize() => InitializeInternal();

    /// <summary>
    /// 
    /// </summary>
    internal static void InitializeInternal()
    {
        // Skip if already initialized
        if (IsInitialized == true)
        {
            return;
        }

        var setting = new OracleDbSetting();

        // Map the DbSetting
        DbSettingMapper.Add<OracleConnection>(setting, true);

        // Map the DbHelper
        DbHelperMapper.Add<OracleConnection>(new OracleDbHelper(setting), true);

        // Map the Statement Builder
        StatementBuilderMapper.Add<OracleConnection>(new OracleStatementBuilder(), true);

        // Set the flag
        IsInitialized = true;
    }

    #endregion
}
