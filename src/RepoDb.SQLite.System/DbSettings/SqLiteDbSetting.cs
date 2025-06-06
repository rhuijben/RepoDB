﻿namespace RepoDb.DbSettings;

/// <summary>
/// A setting class used for SQLite data provider.
/// </summary>
public sealed record SqLiteDbSetting : BaseDbSetting
{
    /// <summary>
    /// Creates a new instance of <see cref="SqLiteDbSetting"/> class.
    /// </summary>
    public SqLiteDbSetting()
        : this(true)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="SqLiteDbSetting"/> class.
    /// </summary>
    public SqLiteDbSetting(bool isExecuteReaderDisposable)
    {
        AreTableHintsSupported = false;
        AverageableType = typeof(double);
        ClosingQuote = "]";
        DefaultSchema = null;
        IsDirectionSupported = false;
        IsExecuteReaderDisposable = isExecuteReaderDisposable;
        IsMultiStatementExecutable = true;
        IsPreparable = true;
        IsUseUpsert = true; // Different than Microsoft.Data.SQLite
        OpeningQuote = "[";
        ParameterPrefix = "@";
        ForceAutomaticConversions = true;
    }
}
