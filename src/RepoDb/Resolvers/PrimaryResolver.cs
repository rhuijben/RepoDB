using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb.Resolvers;

/// <summary>
/// A class that is being used to resolve the primary property of the data entity type.
/// </summary>
public class PrimaryResolver : IResolver<Type, ClassProperty>, IResolver<Type, IEnumerable<ClassProperty>>
{
    /// <summary>
    /// Resolves the primary <see cref="ClassProperty"/> of the data entity type.
    /// </summary>
    /// <param name="entityType">The type of the data entity.</param>
    /// <returns>The instance of the primary <see cref="ClassProperty"/> object.</returns>
    public IEnumerable<ClassProperty>? Resolve(Type entityType)
    {
        var allProperties = PropertyCache.Get(entityType);

        // Check for the properties
        if (allProperties == null)
        {
            return null;
        }

        // Get the first entry with Primary attribute
        var properties = allProperties
            .Where(p => p.GetPrimaryAttribute() != null);

        if (properties.Any())
            return properties.ToArray();

        // Get from the implicit mapping
        properties = PrimaryMapper.GetPrimaryKeys(entityType);
        if (properties?.Any() == true)
            return properties;

        // Id Property
        ClassProperty? property;

        property = allProperties
            .FirstOrDefault(p =>
                string.Equals(p.PropertyInfo.Name, "id", StringComparison.OrdinalIgnoreCase));

        // Type.Name + Id
        if (property == null)
        {
            property = allProperties
                .FirstOrDefault(p =>
                    string.Equals(p.PropertyInfo.Name, string.Concat(p.GetDeclaringType().Name, "id"), StringComparison.OrdinalIgnoreCase));
        }

        // Mapping.Name + Id
        if (property == null)
        {
            property = allProperties
                .FirstOrDefault(p =>
                    string.Equals(p.PropertyInfo.Name, string.Concat(ClassMappedNameCache.Get(p.GetDeclaringType()), "id"), StringComparison.OrdinalIgnoreCase));
        }

        // Return the instance
        return property is { } ? [property] : null;
    }

    ClassProperty IResolver<Type, ClassProperty>.Resolve(Type input)
    {
        return Resolve(input)?.OneOrDefault();
    }
}
