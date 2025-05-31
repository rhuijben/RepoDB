#nullable enable
using System.Collections.ObjectModel;
using System.ComponentModel;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb;

/// <summary>
/// A class the holds the collection of column definitions of the table.
/// </summary>
public sealed class DbFieldCollection : ReadOnlyCollection<DbField>
{
    private readonly IDbSetting dbSetting;
    private readonly Lazy<IEnumerable<Field>> lazyFields;
    private readonly Lazy<DbField?> lazyIdentity;
    private readonly Lazy<IReadOnlyList<DbField>?> lazyPrimaryFields;
    private readonly Lazy<Dictionary<string, DbField>> lazyMapByName;
    private readonly Lazy<Dictionary<string, DbField>> lazyMapByUnquotedName;
    private readonly Lazy<DbField?> lazyPrimary;

    /// <summary>
    /// Creates a new instance of <see cref="DbFieldCollection" /> object.
    /// </summary>
    /// <param name="dbFields">A collection of column definitions of the table.</param>
    /// <param name="dbSetting">The currently in used <see cref="IDbSetting"/> object.</param>
    public DbFieldCollection(IEnumerable<DbField> dbFields, IDbSetting dbSetting)
        : base(dbFields.AsList())
    {
        this.dbSetting = dbSetting;

        lazyPrimaryFields = new(GetPrimaryDbFields);
        lazyPrimary = new(GetPrimaryDbField);
        lazyIdentity = new(GetIdentityDbField);
        lazyMapByName = new(GetDbFieldsMappedByName);
        lazyMapByUnquotedName = new(GetDbFieldsMappedByUnquotedName);
        lazyFields = new(GetDbFieldsAsFields);
    }

    /// <summary>
    /// Gets the column of the primary key if there is a single column primary key
    /// </summary>
    /// <returns>A primary column definition.</returns>
    public DbField? GetPrimary() => lazyPrimary.Value;

    public IReadOnlyList<DbField>? GetPrimaryFields() => lazyPrimaryFields.Value;

    /// <summary>
    /// Gets the identity column of this table if there is ine
    /// </summary>
    /// <returns>A identity column definition.</returns>
    public DbField? GetIdentity() => lazyIdentity.Value;

    /// <summary>
    /// Gets a column definitions of the table.
    /// </summary>
    /// <returns>A column definitions of the table.</returns>
    [Obsolete("Use DbFieldCollection directly")]
    public IEnumerable<DbField> GetItems() => this;

    /// <summary>
    /// Get the list of <see cref="DbField" /> objects converted into an <see cref="IReadOnlyList{T}" /> of <see cref="Field" /> objects.
    /// </summary>
    /// <returns></returns>
    public IEnumerable<Field> AsFields() => lazyFields.Value;

    /// <summary>
    /// Gets a value indicating whether the current column definitions of the table is empty.
    /// </summary>
    /// <returns>A value indicating whether the column definitions of the table is empty.</returns>
    public bool IsEmpty() => Count == 0;

    /// <summary>
    /// Gets column definition of the table based on the name of the database field.
    /// </summary>
    /// <param name="name">The name of the mapping that is equivalent to the column definition of the table.</param>
    /// <returns>A column definition of table.</returns>
    public DbField? GetByName(string name)
    {
        lazyMapByName.Value.TryGetValue(name, out var dbField);

        return dbField;
    }

    /// <summary>
    /// Gets column definition of the table based on the unquotes name of the database field.
    /// </summary>
    /// <param name="name">The name of the mapping that is equivalent to the column definition of the table.</param>
    /// <returns>A column definition of table.</returns>
    public DbField? GetByUnquotedName(string name)
    {
        lazyMapByUnquotedName.Value.TryGetValue(name, out var dbField);

        return dbField;
    }

    private Dictionary<string, DbField> GetDbFieldsMappedByName() =>
        this.ToDictionary(df => df.Name, df => df, StringComparer.OrdinalIgnoreCase);

    private Dictionary<string, DbField> GetDbFieldsMappedByUnquotedName() =>
        this.ToDictionary(df => df.Name.AsUnquoted(true, dbSetting), df => df, StringComparer.OrdinalIgnoreCase);

    private DbField? GetPrimaryDbField() => this.OneOrDefault(df => df.IsPrimary);


    private IReadOnlyList<DbField>? GetPrimaryDbFields() => this.Where(x => x.IsPrimary) is { } p && p.Any() ? p.ToArray() : null;

    private DbField? GetIdentityDbField() => this.FirstOrDefault(df => df.IsIdentity);

    private IEnumerable<Field> GetDbFieldsAsFields() => this.AsFields();

    [EditorBrowsable(EditorBrowsableState.Never)]
    public IEnumerable<Field> GetAsFields() => AsFields();
}
