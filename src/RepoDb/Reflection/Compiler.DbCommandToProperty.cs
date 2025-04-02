﻿using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb.Reflection;

partial class Compiler
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="field"></param>
    /// <param name="parameterName"></param>
    /// <param name="index"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    public static Action<TEntity, DbCommand> CompileDbCommandToProperty<TEntity>(Field field,
        string parameterName,
        int index,
        IDbSetting dbSetting)
        where TEntity : class
    {
        // Variables needed
        var typeOfEntity = typeof(TEntity);
        var entityParameterExpression = Expression.Parameter(typeOfEntity, "entity");
        var dbCommandParameterExpression = Expression.Parameter(StaticType.DbCommand, "command");

        // Variables for DbCommand
        var dbCommandParametersProperty = StaticType.DbCommand.GetProperty(nameof(DbCommand.Parameters));

        // Variables for DbParameterCollection
        var dbParameterCollectionIndexerMethod = StaticType.DbParameterCollection.GetMethod("get_Item", new[] { StaticType.String });

        // Variables for DbParameter
        var dbParameterValueProperty = StaticType.DbParameter.GetProperty(nameof(DbParameter.Value));

        // Get the entity property
        var propertyName = field.Name.AsUnquoted(true, dbSetting).AsAlphaNumeric();
        var property = (typeOfEntity.GetProperty(propertyName) ?? PropertyCache.Get(typeOfEntity)?.GetByMappedName(propertyName)?.PropertyInfo)?.SetMethod;

        // Get the command parameter
        var name = parameterName ?? propertyName;
        var parameters = Expression.Property(dbCommandParameterExpression, dbCommandParametersProperty);
        var parameter = Expression.Call(parameters, dbParameterCollectionIndexerMethod,
            Expression.Constant(index > 0 ? string.Concat(name, "_", index.ToString(CultureInfo.InvariantCulture)) : name));

        // Assign the Parameter.Value into DataEntity.Property
        var value = Expression.Property(parameter, dbParameterValueProperty);
        var propertyAssignment = Expression.Call(entityParameterExpression, property,
            Expression.Convert(value, TypeCache.Get(field.Type)?.GetUnderlyingType()));

        // Return function
        return Expression.Lambda<Action<TEntity, DbCommand>>(
            propertyAssignment, entityParameterExpression, dbCommandParameterExpression).Compile();
    }
}
