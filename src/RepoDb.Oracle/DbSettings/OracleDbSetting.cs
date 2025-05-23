namespace RepoDb.DbSettings;

public sealed class OracleDbSetting : BaseDbSetting
{
    public OracleDbSetting() : base()
    {
        AreTableHintsSupported = false;
        OpeningQuote = "\"";
        ClosingQuote = "\"";
        AverageableType = typeof(decimal);
        DefaultSchema = "USER";
        IsDirectionSupported = true;
        IsExecuteReaderDisposable = false;
        IsMultiStatementExecutable = false;
        IsPreparable = true;
        IsUseUpsert = false;
        ParameterPrefix = ":";
        ForceAutomaticConversions = false; // Yes — Oracle returns untyped `NUMBER`, `DATE`, etc., requiring conversion
        ParameterBatchCount = 32766;
        GenerateFinalSemiColon = false;
        QuoteParameterNames = true;
    }
}
