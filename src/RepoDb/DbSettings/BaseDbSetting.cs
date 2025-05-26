#nullable enable
using RepoDb.Interfaces;

namespace RepoDb.DbSettings;

/// <summary>
/// A base class to be used when implementing an <see cref="IDbSetting"/>-based object to support a specific RDBMS data provider.
/// </summary>
public abstract class BaseDbSetting : IDbSetting, IEquatable<BaseDbSetting>
{
    #region Privates

    private int? hashCode = null;

    #endregion

    #region Constructor

    /// <summary>
    /// Creates a new instance of <see cref="BaseDbSetting"/> class.
    /// </summary>
    public BaseDbSetting()
    {
        AreTableHintsSupported = true;
        AverageableType = StaticType.Double;
        ClosingQuote = "]";
        DefaultSchema = "dbo";
        IsDirectionSupported = true;
        IsExecuteReaderDisposable = true;
        IsMultiStatementExecutable = true;
        IsPreparable = true;
        IsUseUpsert = false;
        OpeningQuote = "[";
        ParameterPrefix = "@";
        GenerateFinalSemiColon = true;
    }

    #endregion

    #region Properties

    /// <inheritdoc />
    public bool AreTableHintsSupported { get; protected init; }

    /// <inheritdoc />
    public string ClosingQuote { get; protected init; }

    /// <inheritdoc />
    public Type AverageableType { get; protected init; }

    /// <inheritdoc />
    public string? DefaultSchema { get; protected init; }

    /// <inheritdoc />
    public bool IsDirectionSupported { get; protected init; }

    /// <inheritdoc />
    public bool IsExecuteReaderDisposable { get; protected init; }

    /// <inheritdoc />
    public bool IsMultiStatementExecutable { get; protected init; }

    /// <inheritdoc />
    public bool IsPreparable { get; protected init; }

    /// <inheritdoc />
    public bool IsUseUpsert { get; protected init; }

    /// <inheritdoc />
    public string OpeningQuote { get; protected init; }

    /// <inheritdoc />
    public string ParameterPrefix { get; protected init; }

    /// <inheritdoc />
    public bool ForceAutomaticConversions { get; protected init; }

    /// <inheritdoc />
    public int ParameterBatchCount { get; protected init; } = 2100 - 2;

    /// <inheritdoc />
    public bool GenerateFinalSemiColon { get; protected init; }

    /// <inheritdoc />
    public bool QuoteParameterNames { get; protected init; }

    /// <inheritdoc />
    public bool IdentityViaOutputParameter { get; protected init; }

    #endregion

    #region Equality and comparers

    /// <summary>
    /// Returns the hashcode for this <see cref="BaseDbSetting"/>.
    /// </summary>
    /// <returns>The hashcode value.</returns>
    public override int GetHashCode()
    {
        if (this.hashCode != null)
        {
            return this.hashCode.Value;
        }

        // Use the non nullable for perf purposes
        var hashCode = 0;

        // AreTableHintsSupported
        hashCode = HashCode.Combine(hashCode, AreTableHintsSupported);

        // ClosingQuote
        if (!string.IsNullOrWhiteSpace(ClosingQuote))
        {
            hashCode = HashCode.Combine(hashCode, ClosingQuote);
        }

        // DefaultAverageableType
        if (AverageableType != null)
        {
            hashCode = HashCode.Combine(hashCode, AverageableType);
        }

        // DefaultSchema
        if (!string.IsNullOrWhiteSpace(DefaultSchema))
        {
            hashCode = HashCode.Combine(hashCode, DefaultSchema);
        }

        // IsDirectionSupported
        hashCode = HashCode.Combine(hashCode, IsDirectionSupported, IsExecuteReaderDisposable, IsMultiStatementExecutable, IsPreparable);

        // IsUseUpsert
        hashCode = HashCode.Combine(hashCode, IsUseUpsert);

        // OpeningQuote
        if (!string.IsNullOrWhiteSpace(OpeningQuote))
        {
            hashCode = HashCode.Combine(hashCode, OpeningQuote);
        }

        // ParameterPrefix
        if (!string.IsNullOrWhiteSpace(ParameterPrefix))
        {
            hashCode = HashCode.Combine(hashCode, ParameterPrefix);
        }

        hashCode = HashCode.Combine(hashCode, ParameterBatchCount, GenerateFinalSemiColon, QuoteParameterNames, IdentityViaOutputParameter);

        // Set and return the hashcode
        return this.hashCode ??= hashCode;
    }

    /// <summary>
    /// Compares the <see cref="BaseDbSetting"/> object equality against the given target object.
    /// </summary>
    /// <param name="obj">The object to be compared to the current object.</param>
    /// <returns>True if the instances are equals.</returns>
    public override bool Equals(object? obj)
    {
        return Equals(obj as BaseDbSetting);
    }

    /// <summary>
    /// Compares the <see cref="BaseDbSetting"/> object equality against the given target object.
    /// </summary>
    /// <param name="other">The object to be compared to the current object.</param>
    /// <returns>True if the instances are equal.</returns>
    public bool Equals(BaseDbSetting? other)
    {
        return other is not null
            && other.AreTableHintsSupported == AreTableHintsSupported
            && other.ClosingQuote == ClosingQuote
            && other.AverageableType == AverageableType
            && other.DefaultSchema == DefaultSchema
            && other.IsDirectionSupported == IsDirectionSupported
            && other.IsExecuteReaderDisposable == IsExecuteReaderDisposable
            && other.IsMultiStatementExecutable == IsMultiStatementExecutable
            && other.IsPreparable == IsPreparable
            && other.IsUseUpsert == IsUseUpsert
            && other.OpeningQuote == OpeningQuote
            && other.ParameterPrefix == ParameterPrefix
            && other.ParameterBatchCount == ParameterBatchCount
            && other.GenerateFinalSemiColon == GenerateFinalSemiColon
            && other.QuoteParameterNames == QuoteParameterNames
            && other.IdentityViaOutputParameter == IdentityViaOutputParameter;
    }

    /// <summary>
    /// Compares the equality of the two <see cref="BaseDbSetting"/> objects.
    /// </summary>
    /// <param name="objA">The first <see cref="BaseDbSetting"/> object.</param>
    /// <param name="objB">The second <see cref="BaseDbSetting"/> object.</param>
    /// <returns>True if the instances are equal.</returns>
    public static bool operator ==(BaseDbSetting objA,
        BaseDbSetting objB)
    {
        if (objA is null)
        {
            return objB is null;
        }
        return objA.Equals(objB);
    }

    /// <summary>
    /// Compares the inequality of the two <see cref="BaseDbSetting"/> objects.
    /// </summary>
    /// <param name="objA">The first <see cref="BaseDbSetting"/> object.</param>
    /// <param name="objB">The second <see cref="BaseDbSetting"/> object.</param>
    /// <returns>True if the instances are not equal.</returns>
    public static bool operator !=(BaseDbSetting objA,
        BaseDbSetting objB) =>
        (objA == objB) == false;

    #endregion
}
