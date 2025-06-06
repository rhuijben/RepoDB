﻿using MySql.Data.MySqlClient;

namespace RepoDb.DbSettings;

/// <summary>
/// A setting class used for <see cref="MySqlConnection"/> data provider.
/// </summary>
public sealed record MySqlDbSetting : BaseDbSetting
{
    /// <summary>
    /// Creates a new instance of <see cref="MySqlDbSetting"/> class.
    /// </summary>
    public MySqlDbSetting()
    {
        AreTableHintsSupported = false;
        AverageableType = typeof(double);
        ClosingQuote = "`";
        DefaultSchema = null;
        IsDirectionSupported = false;
        IsExecuteReaderDisposable = false;
        IsMultiStatementExecutable = true;
        IsPreparable = false;
        IsUseUpsert = false;
        OpeningQuote = "`";
        ParameterPrefix = "@";
    }
}
