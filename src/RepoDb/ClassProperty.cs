﻿#nullable enable
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Reflection;
using RepoDb.Attributes;
using RepoDb.Attributes.Parameter;
using RepoDb.Extensions;

namespace RepoDb;

/// <summary>
/// A class that wraps the <see cref="PropertyInfo"/> object. This class is used to extract the information from the <see cref="System.Reflection.PropertyInfo"/> object in a fast and efficient manner.
/// </summary>
public sealed class ClassProperty : IEquatable<ClassProperty>
{
    /// <summary>
    /// Creates a new instance of <see cref="ClassProperty"/> object.
    /// </summary>
    /// <param name="property">The wrapped property.</param>
    public ClassProperty(PropertyInfo property) :
        this(property.DeclaringType!, property)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="ClassProperty"/> object.
    /// </summary>
    /// <param name="parentType">The declaring type (avoiding the interface collision).</param>
    /// <param name="property">The wrapped property.</param>
    public ClassProperty(Type parentType,
        PropertyInfo property)
    {
        DeclaringType = parentType;
        PropertyInfo = property;

        typeMapAttribute = new(() => PropertyInfo.GetCustomAttribute<TypeMapAttribute>(), true);
        propertyHandlerAttribute = new(() => PropertyInfo.GetCustomAttribute<PropertyHandlerAttribute>(), true);
        dbType = new Lazy<DbType?>(() => PropertyInfo.GetDbType(), true);
        propertyValueAttributes = new Lazy<IEnumerable<PropertyValueAttribute>>(() => PropertyInfo.GetPropertyValueAttributes(DeclaringType), true);
        propertyValueAttribute = new(() =>
        {
            return (PropertyInfo.GetCustomAttribute<DbTypeAttribute>() ?? PropertyInfo.GetCustomAttribute<TypeMapAttribute>()) ??
                   (GetPropertyValueAttributes()
                       .LastOrDefault(e => string.Equals(nameof(IDbDataParameter.ParameterName), e.PropertyName, StringComparison.OrdinalIgnoreCase)));
        }, true);
    }

    #region Properties

    /// <summary>
    /// Gets the wrapped property of this object.
    /// </summary>
    public PropertyInfo PropertyInfo { get; }


    /// <summary>
    /// Gets the propertyname via <see cref="PropertyInfo"/>
    /// </summary>
    public string Name => PropertyInfo.Name;

    #endregion

    #region Methods

    /// <summary>
    /// Returns the string that represent the current <see cref="ClassProperty"/> object.
    /// </summary>
    /// <returns>The unquoted name.</returns>
    public override string ToString() =>
        string.Concat("ClassProperty :: Name = ", GetMappedName(), " (", PropertyInfo.PropertyType.FullName, "), ",
            "DeclaringType = ", DeclaringType.FullName);

    /// <summary>
    /// Gets the declaring parent type of the current property info. If the class inherits an interface, then this will return
    /// the derived class type instead (if there is), otherwise the <see cref="PropertyInfo.DeclaringType"/> property.
    /// </summary>
    /// <returns>The declaring type.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Type GetDeclaringType() => DeclaringType;

    /// <summary>
    /// Gets the declaring parent type of the current property info. If the class inherits an interface, then this will return
    /// the derived class type instead (if there is), otherwise the <see cref="PropertyInfo.DeclaringType"/> property.
    /// </summary>
    /// <returns>The declaring type.</returns>
    public Type DeclaringType { get; }

    /*
     * AsField
     */

    private Field? field;

    /// <summary>
    /// Convert the <see cref="ClassProperty"/> into a <see cref="Field"/> objects.
    /// </summary>
    /// <returns>An instance of <see cref="string"/> object.</returns>
    public Field AsField()
    {
        return field ??= new Field(GetMappedName(), PropertyInfo.PropertyType);
    }

    /*
     * GetPrimaryAttribute
     */

    private bool isPrimaryAttributeWasSet;
    private PrimaryAttribute? primaryAttribute;

    /// <summary>
    /// Gets the <see cref="PrimaryAttribute"/> if present.
    /// </summary>
    /// <returns>The instance of <see cref="PrimaryAttribute"/>.</returns>
    public PrimaryAttribute? GetPrimaryAttribute()
    {
        if (isPrimaryAttributeWasSet)
        {
            return primaryAttribute;
        }
        isPrimaryAttributeWasSet = true;
        return primaryAttribute = PropertyInfo.GetCustomAttribute<PrimaryAttribute>() ??
            (PropertyInfo.GetCustomAttribute<KeyAttribute>() != null ? new PrimaryAttribute() : null);
    }

    /*
     * GetIdentityAttribute
     */
    private bool isIdentityAttributeWasSet;
    private IdentityAttribute? identityAttribute;

    /// <summary>
    /// Gets the <see cref="IdentityAttribute"/> if present.
    /// </summary>
    /// <returns>The instance of <see cref="IdentityAttribute"/>.</returns>
    public IdentityAttribute? GetIdentityAttribute()
    {
        if (isIdentityAttributeWasSet)
        {
            return identityAttribute;
        }
        isIdentityAttributeWasSet = true;
        return identityAttribute = PropertyInfo.GetCustomAttribute<IdentityAttribute>();
    }

    /*
     * GetTypeMapAttribute
     */
    private readonly Lazy<TypeMapAttribute?> typeMapAttribute;

    /// <summary>
    /// Gets the <see cref="TypeMapAttribute"/> if present.
    /// </summary>
    /// <returns>The instance of <see cref="TypeMapAttribute"/>.</returns>
    public TypeMapAttribute? GetTypeMapAttribute()
    {
        return typeMapAttribute.Value;
    }

    /*
     * GetDbTypeAttribute
     */
    private readonly Lazy<PropertyValueAttribute?> propertyValueAttribute;

    /// <summary>
    /// Gets the <see cref="PropertyValueAttribute"/> if present.
    /// </summary>
    /// <returns>The instance of <see cref="PropertyValueAttribute"/>.</returns>
    public PropertyValueAttribute? GetDbTypeAttribute()
    {
        return propertyValueAttribute.Value;
    }

    /*
     * GetPropertyHandlerAttribute
     */
    private readonly Lazy<PropertyHandlerAttribute?> propertyHandlerAttribute;

    /// <summary>
    /// Gets the <see cref="PropertyHandlerAttribute"/> if present.
    /// </summary>
    /// <returns>The instance of <see cref="PropertyHandlerAttribute"/>.</returns>
    public PropertyHandlerAttribute? GetPropertyHandlerAttribute()
    {
        return propertyHandlerAttribute.Value;
    }

    [Obsolete("Always returns true"), EditorBrowsable(EditorBrowsableState.Never)]
    public bool? IsPrimary() => true;
    [Obsolete("Always returns true"), EditorBrowsable(EditorBrowsableState.Never)]
    public bool? IsIdentity() => true;

    /*
     * GetDbType
     */
    private readonly Lazy<DbType?> dbType;

    /// <summary>
    /// Gets the mapped <see cref="DbType"/> for the current property.
    /// </summary>
    /// <returns>The mapped <see cref="DbType"/> value.</returns>
    public DbType? GetDbType()
    {
        return dbType.Value;
    }

    /*
     * GetPropertyHandler
     */

    /// <summary>
    /// Gets the mapped property handler object for the current property.
    /// </summary>
    /// <returns>The mapped property handler object.</returns>
    public object? GetPropertyHandler() =>
        GetPropertyHandler<object>();

    /// <summary>
    /// Gets the mapped property handler object for the current property.
    /// </summary>
    /// <typeparam name="TPropertyHandler">The type of the handler.</typeparam>
    /// <returns>The mapped property handler object.</returns>
    public TPropertyHandler? GetPropertyHandler<TPropertyHandler>()
    {
        return Converter.ToType<TPropertyHandler>(PropertyHandlerCache.Get<TPropertyHandler>(DeclaringType, PropertyInfo));
    }

    /*
     * GetMappedName
     */

    private string? mappedName;

    /// <summary>
    /// Gets the mapped-name for the current property.
    /// </summary>
    /// <returns>The mapped-name value.</returns>
    public string GetMappedName() =>
        mappedName ??= PropertyMappedNameCache.Get(DeclaringType, PropertyInfo);

    /*
     * PropertyHandlerAttributes
     */
    private readonly Lazy<IEnumerable<PropertyValueAttribute>> propertyValueAttributes;

    /// <summary>
    /// Gets the list of mapped <see cref="PropertyValueAttribute"/> object for the current property.
    /// </summary>
    /// <returns>The list of mapped <see cref="PropertyValueAttribute"/> object.</returns>
    public IEnumerable<PropertyValueAttribute> GetPropertyValueAttributes()
    {
        return propertyValueAttributes.Value;
    }

    #endregion

    #region Comparers

    /// <summary>
    /// Returns the hashcode of the <see cref="PropertyInfo"/> object of this instance.
    /// </summary>
    /// <returns>The hash code value.</returns>
    public override int GetHashCode() =>
        HashCode.Combine(DeclaringType, PropertyInfo.GenerateCustomizedHashCode(DeclaringType));

    /// <summary>
    /// Compare the current instance to the other object instance.
    /// </summary>
    /// <param name="obj">The object to be compared.</param>
    /// <returns>True if the two instance is the same.</returns>
    public override bool Equals(object? obj)
    {
        return Equals(obj as ClassProperty);
    }

    /// <summary>
    /// Compare the current instance to the other object instance.
    /// </summary>
    /// <param name="other">The object to be compared.</param>
    /// <returns>True if the two instance is the same.</returns>
    public bool Equals(ClassProperty? other) =>
        PropertyInfo.Equals(other?.PropertyInfo);


    /// <summary>
    /// Compares the equality of the two <see cref="ClassProperty"/> objects.
    /// </summary>
    /// <param name="objA">The first <see cref="ClassProperty"/> object.</param>
    /// <param name="objB">The second <see cref="ClassProperty"/> object.</param>
    /// <returns>True if the instances are equal.</returns>
    public static bool operator ==(ClassProperty? objA,
        ClassProperty? objB)
    {
        if (objA is null)
        {
            return objB is null;
        }
        return objB?.GetHashCode() == objA.GetHashCode();
    }

    /// <summary>
    /// Compares the inequality of the two <see cref="ClassProperty"/> objects.
    /// </summary>
    /// <param name="objA">The first <see cref="ClassProperty"/> object.</param>
    /// <param name="objB">The second <see cref="ClassProperty"/> object.</param>
    /// <returns>True if the instances are not equal.</returns>
    public static bool operator !=(ClassProperty? objA,
        ClassProperty? objB) =>
        (objA == objB) == false;

    #endregion
}
