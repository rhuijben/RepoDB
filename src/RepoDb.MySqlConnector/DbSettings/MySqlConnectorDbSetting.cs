﻿using MySqlConnector;

namespace RepoDb.DbSettings;

/// <summary>
/// A setting class used for <see cref="MySqlConnection"/> data provider.
/// </summary>
public sealed record MySqlConnectorDbSetting : BaseDbSetting
{
    /// <summary>
    /// Creates a new instance of <see cref="MySqlConnectorDbSetting"/> class.
    /// </summary>
    public MySqlConnectorDbSetting()
    {
        AreTableHintsSupported = false;
        AverageableType = typeof(double);
        ClosingQuote = "`";
        DefaultSchema = null;
        IsDirectionSupported = false;
        IsExecuteReaderDisposable = true;
        IsMultiStatementExecutable = true;
        IsPreparable = true;
        IsUseUpsert = false;
        OpeningQuote = "`";
        ParameterPrefix = "@";
        // MySql doesn't have a max size, but needs the query including parameters needs to fit 4MB
        MaxParameterCount = 10000;
        // MaxQueriesInBatchCount = base.MaxQueriesInBatchCount;
    }
}
