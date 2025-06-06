﻿using MySql.Data.MySqlClient;
using RepoDb.DbHelpers;
using RepoDb.DbSettings;
using RepoDb.StatementBuilders;

namespace RepoDb;

/// <summary>
/// A class used to initialize necessary objects that is connected to <see cref="MySqlConnection"/> object.
/// </summary>
public static class MySqlBootstrap
{
    #region Properties

    /// <summary>
    /// Gets the value indicating whether the initialization is completed.
    /// </summary>
    public static bool IsInitialized { get; private set; }

    #endregion

    #region Methods

    /// <summary>
    /// Initializes all necessary settings for MySql.
    /// </summary>
    [Obsolete("This class will soon to be hidden as internal class. Use the 'GlobalConfiguration.Setup().UseMySql()' method instead.")]
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

        // Map the DbSetting
        DbSettingMapper.Add<MySqlConnection>(new MySqlDbSetting(), true);

        // Map the DbHelper
        DbHelperMapper.Add<MySqlConnection>(new MySqlDbHelper(), true);

        // Map the Statement Builder
        StatementBuilderMapper.Add<MySqlConnection>(new MySqlStatementBuilder(), true);

        // Set the flag
        IsInitialized = true;
    }

    #endregion
}
