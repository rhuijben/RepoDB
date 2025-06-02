﻿namespace RepoDb.DbSettings;

public sealed record OracleDbSetting : BaseDbSetting
{
    public OracleDbSetting() : base()
    {
        AreTableHintsSupported = false;
        OpeningQuote = "\"";
        ClosingQuote = "\"";
        AverageableType = typeof(decimal);
        DefaultSchema = null;
        IsDirectionSupported = true;
        IsExecuteReaderDisposable = false;
        IsMultiStatementExecutable = true;
        IsPreparable = true;
        ParameterPrefix = ":";
        ForceAutomaticConversions = false; // Yes — Oracle returns untyped `NUMBER`, `DATE`, etc., requiring conversion
        MaxParameterCount = 32766;
        MaxQueriesInBatchCount = 1000;
        GenerateFinalSemiColon = false;
        QuoteParameterNames = true;
    }
}
