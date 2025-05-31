namespace RepoDb.DbSettings;

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
        IsUseUpsert = false;
        ParameterPrefix = ":";
        ForceAutomaticConversions = false; // Yes — Oracle returns untyped `NUMBER`, `DATE`, etc., requiring conversion
        ParameterBatchCount = 32766;
        GenerateFinalSemiColon = false;
        QuoteParameterNames = true;
    }
}
