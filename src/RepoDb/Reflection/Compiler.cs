#nullable enable
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using RepoDb.Enumerations;
using RepoDb.Exceptions;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb.Reflection;

/// <summary>
/// The compiler class of the library.
/// </summary>
internal sealed partial class Compiler
{
    #region SubClasses/SubStructs


    /// <summary>
    /// A class that contains both the instance of <see cref="RepoDb.ClassProperty"/> and <see cref="System.Reflection.ParameterInfo"/> objects.
    /// </summary>
    private sealed class ClassPropertyParameterInfo
    {
        private string? descriptiveContextString;

        /// <summary>
        /// Gets the instance of <see cref="RepoDb.ClassProperty"/> object in used.
        /// </summary>
        public ClassProperty? ClassProperty { get; init; }

        /// <summary>
        /// Gets the instance of <see cref="System.Reflection.ParameterInfo"/> object in used.
        /// </summary>
        public ParameterInfo? ParameterInfo { get; init; }

        /// <summary>
        /// Gets the instance of <see cref="RepoDb.ClassProperty"/> object that is mapped to the current <see cref="ParameterInfo"/>.
        /// </summary>
        public ClassProperty? ParameterInfoMappedClassProperty { get; init; }

        /// <summary>
        /// Gets the target type.
        /// </summary>
        public Type? TargetType { get; init; }

        /// <summary>
        /// Gets the target type based on the combinations.
        /// </summary>
        /// <returns></returns>
        public Type GetTargetType() =>
            TargetType ?? ParameterInfo?.ParameterType ?? ClassProperty?.PropertyInfo?.PropertyType!;

        /// <summary>
        /// Gets the descriptive context string for error messaging.
        /// </summary>
        /// <returns></returns>
        public string GetDescriptiveContextString()
        {
            if (descriptiveContextString != null)
            {
                return descriptiveContextString;
            }

            // Variable
            var message = $"Context :: TargetType: {GetTargetType()} ";

            // ParameterInfo
            if (ParameterInfo != null)
            {
                message = string.Concat(descriptiveContextString, $"Parameter: {ParameterInfo.Name} ({ParameterInfo.ParameterType}) ");
            }

            // ClassProperty
            if (ClassProperty?.PropertyInfo != null)
            {
                message = string.Concat(descriptiveContextString, $"PropertyInfo: {ClassProperty.PropertyInfo.Name} ({ClassProperty.PropertyInfo.PropertyType}), DeclaringType: {ClassProperty.DeclaringType} ");
            }

            // Return
            return (descriptiveContextString = message.Trim());
        }

        /// <summary>
        /// Returns the string that represents this object.
        /// </summary>
        /// <returns>The presented string.</returns>
        public override string ToString() =>
            string.Concat("TargetType = ", GetTargetType()?.FullName, ", ClassProperty = ", ClassProperty?.ToString(), ", ",
                "ParameterInfo = ", ParameterInfo?.ToString(), ")", ", TargetType = ", TargetType?.ToString(), ", ");
    }

    /// <summary>
    ///
    /// </summary>
    private struct FieldDirection
    {
        public int Index { get; set; }
        public DbField DbField { get; set; }
        public ParameterDirection Direction { get; set; }
    }

    /// <summary>
    /// A class that contains both the property <see cref="MemberAssignment"/> object and the constructor argument <see cref="Expression"/> value.
    /// </summary>
    private class MemberBinding
    {
        /// <summary>
        /// Gets the instance of <see cref="ClassProperty"/> object in used.
        /// </summary>
        public ClassProperty? ClassProperty { get; init; }

        /// <summary>
        /// Gets the instance of <see cref="ParameterInfo"/> object in used.
        /// </summary>
        public ParameterInfo? ParameterInfo { get; init; }

        /// <summary>
        /// Gets the current member assignment of the defined property.
        /// </summary>
        public MemberAssignment? MemberAssignment { get; init; }

        /// <summary>
        /// Gets the corresponding constructor argument of the defined property.
        /// </summary>
        public Expression? Argument { get; init; }

        /// <summary>
        /// Returns the string that represents this object.
        /// </summary>
        /// <returns>The presented string.</returns>
        public override string ToString() =>
            ClassProperty?.ToString() ?? ParameterInfo?.ToString() ?? "";
    }

    #endregion

    #region Methods

    /// <summary>
    ///
    /// </summary>
    /// <param name="fields"></param>
    /// <returns></returns>
    private static IEnumerable<FieldDirection> GetInputFieldDirections(IEnumerable<DbField> fields)
    {
        if (fields?.Any() != true)
        {
            return Enumerable.Empty<FieldDirection>();
        }
        return fields.Select((value, index) => new FieldDirection
        {
            Index = index,
            DbField = value,
            Direction = ParameterDirection.Input
        });
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="fields"></param>
    /// <returns></returns>
    private static IEnumerable<FieldDirection> GetOutputFieldDirections(IEnumerable<DbField> fields)
    {
        if (fields?.Any() != true)
        {
            return Enumerable.Empty<FieldDirection>();
        }
        return fields.Select((value, index) => new FieldDirection
        {
            Index = index,
            DbField = value,
            Direction = ParameterDirection.Output
        });
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="fromType"></param>
    /// <param name="toType"></param>
    /// <returns></returns>
    private static MethodInfo? GetSystemConvertToTypeMethod(Type fromType,
        Type toType) =>
        StaticType.Convert.GetMethod(string.Concat("To", TypeCache.Get(toType).GetUnderlyingType().Name),
            new[] { TypeCache.Get(fromType).GetUnderlyingType() })!;

    /// <summary>
    ///
    /// </summary>
    /// <param name="conversionType"></param>
    /// <returns></returns>
    private static MethodInfo? GetSystemConvertChangeTypeMethod(Type conversionType) =>
        StaticType.Convert.GetMethod(nameof(Convert.ChangeType),
            new[] { StaticType.Object, TypeCache.Get(conversionType).GetUnderlyingType() });

    /// <summary>
    ///
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    private static object? GetClassHandler(Type type) =>
        ClassHandlerCache.Get<object>(type);

    /// <summary>
    ///
    /// </summary>
    /// <param name="handlerInstance"></param>
    /// <returns></returns>
    private static MethodInfo? GetClassHandlerGetMethod(object handlerInstance) =>
        handlerInstance.GetType().GetMethod(nameof(IPropertyHandler<object, object>.Get));

    /// <summary>
    ///
    /// </summary>
    /// <param name="handlerInstance"></param>
    /// <returns></returns>
    private static MethodInfo? GetClassHandlerSetMethod(object handlerInstance) =>
        handlerInstance.GetType().GetMethod(nameof(IPropertyHandler<object, object>.Set));

    /// <summary>
    ///
    /// </summary>
    /// <param name="handlerInstance"></param>
    /// <returns></returns>
    public static Type? GetPropertyHandlerInterfaceOrHandlerType(object? handlerInstance)
    {
        if (handlerInstance is null)
            return null;

        Type handlerType = handlerInstance.GetType();
        var propertyHandlerInterface = handlerType
            .GetInterfaces()
            .FirstOrDefault(interfaceType =>
                interfaceType.IsInterfacedTo(StaticType.IPropertyHandler));
        return propertyHandlerInterface ?? handlerType;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="handlerInstance"></param>
    /// <returns></returns>
    private static MethodInfo? GetPropertyHandlerGetMethod(object? handlerInstance) =>
        // In F#, the instance is not a concrete class, therefore, we need to extract it by interface
        GetPropertyHandlerInterfaceOrHandlerType(handlerInstance)?.GetMethod(nameof(IPropertyHandler<object, object>.Get));

    /// <summary>
    ///
    /// </summary>
    /// <param name="handlerInstance"></param>
    /// <returns></returns>
    private static MethodInfo? GetPropertyHandlerGetMethodFromInterface(object handlerInstance)
    {
        var propertyHandlerInterface = handlerInstance?
            .GetType()?
            .GetInterfaces()
            .FirstOrDefault(interfaceType =>
                interfaceType.IsInterfacedTo(StaticType.IPropertyHandler));
        return propertyHandlerInterface?.GetMethod(nameof(IPropertyHandler<object, object>.Get));
    }

    /// <summary>
    ///
    /// </summary>
    /// <returns></returns>
    private static MethodInfo GetDbCommandCreateParameterMethod() =>
        GetMethodInfo(() => DbCommandExtension.CreateParameter(null!, null!, null, null));

    /// <summary>
    ///
    /// </summary>
    /// <returns></returns>
    private static MethodInfo GetDbParameterCollectionAddMethod() =>
        StaticType.DbParameterCollection.GetMethod("Add")!;

    /// <summary>
    ///
    /// </summary>
    /// <param name="handlerInstance"></param>
    /// <returns></returns>
    private static MethodInfo? GetPropertyHandlerSetMethod(object? handlerInstance) =>
        GetPropertyHandlerInterfaceOrHandlerType(handlerInstance)?.GetMethod(nameof(IPropertyHandler<object, object>.Set));

    /// <summary>
    ///
    /// </summary>
    /// <param name="classPropertyParameterInfo"></param>
    /// <returns></returns>
    private static ParameterInfo? GetPropertyHandlerGetParameter(ClassPropertyParameterInfo classPropertyParameterInfo) =>
        GetPropertyHandlerGetParameter(classPropertyParameterInfo?.ClassProperty);

    /// <summary>
    ///
    /// </summary>
    /// <param name="property"></param>
    /// <param name="targetType"></param>
    /// <returns></returns>
    private static Type? GetPropertyHandlerSetMethodReturnType(ClassProperty property,
        Type targetType) =>
        GetPropertyHandlerSetMethod(property?.GetPropertyHandler() ??
            PropertyHandlerCache.Get<object>(targetType))?.ReturnType;

    /// <summary>
    ///
    /// </summary>
    /// <param name="classProperty"></param>
    /// <returns></returns>
    private static ParameterInfo? GetPropertyHandlerGetParameter(ClassProperty? classProperty) =>
        GetPropertyHandlerGetParameter(classProperty?.GetPropertyHandler());

    /// <summary>
    ///
    /// </summary>
    /// <param name="handlerInstance"></param>
    /// <returns></returns>
    private static ParameterInfo? GetPropertyHandlerGetParameter(object? handlerInstance) =>
        GetPropertyHandlerGetParameter(GetPropertyHandlerGetMethod(handlerInstance));

    /// <summary>
    ///
    /// </summary>
    /// <param name="getMethod"></param>
    /// <returns></returns>
    private static ParameterInfo? GetPropertyHandlerGetParameter(MethodInfo? getMethod) =>
        getMethod?.GetParameters().First();

    /// <summary>
    ///
    /// </summary>
    /// <param name="handlerInstance"></param>
    /// <returns></returns>
    private static ParameterInfo? GetPropertyHandlerSetParameter(object? handlerInstance) =>
        GetPropertyHandlerSetParameter(GetPropertyHandlerSetMethod(handlerInstance));

    /// <summary>
    ///
    /// </summary>
    /// <param name="setMethod"></param>
    /// <returns></returns>
    private static ParameterInfo? GetPropertyHandlerSetParameter(MethodInfo? setMethod) =>
        setMethod?.GetParameters().First();

    /// <summary>
    ///
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static IEnumerable<DataReaderField> GetDataReaderFields(DbDataReader reader,
        IDbSetting dbSetting) =>
        GetDataReaderFields(reader, null, dbSetting);

    /// <summary>
    ///
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="dbFields"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static IEnumerable<DataReaderField> GetDataReaderFields(DbDataReader reader,
        DbFieldCollection? dbFields,
        IDbSetting dbSetting)
    {
        return Enumerable.Range(0, reader.FieldCount)
            .Select(reader.GetName)
            .Select((name, ordinal) => new DataReaderField
            {
                Name = name,
                Ordinal = ordinal,
                Type = reader.GetFieldType(ordinal) ?? StaticType.Object,
                DbField = dbFields?.GetByUnquotedName(name.AsUnquoted(true, dbSetting))
            });
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="classPropertyParameterInfo"></param>
    /// <param name="readerField"></param>
    /// <returns></returns>
    private static object? GetHandlerInstance(ClassPropertyParameterInfo classPropertyParameterInfo,
        DataReaderField readerField) =>
        GetHandlerInstance(classPropertyParameterInfo.ClassProperty, readerField);

    /// <summary>
    ///
    /// </summary>
    /// <param name="classProperty"></param>
    /// <param name="readerField"></param>
    /// <returns></returns>
    private static object? GetHandlerInstance(ClassProperty? classProperty,
        DataReaderField readerField)
    {
        if (classProperty == null)
        {
            return null;
        }
        var value = classProperty.GetPropertyHandler();
        if (value == null && readerField?.Type != null)
        {
            value = PropertyHandlerCache
                .Get<object>(TypeCache.Get(readerField.Type).GetUnderlyingType());
        }
        return value;
    }

    static PropertyInfo GetPropertyInfo<TFrom>(Expression<Func<TFrom, object?>> expression) => (PropertyInfo)((MemberExpression)UnwrapUnary(expression.Body)).Member;

    /// <summary>
    ///
    /// </summary>
    /// <param name="targetType"></param>
    /// <returns></returns>
    private static MethodInfo? GetDbReaderGetValueMethod(Type targetType, Type? readerType) =>
        readerType?.GetMethod(string.Concat("Get", targetType?.Name), new[] { StaticType.Int32 })
        ?? StaticType.DbDataReader.GetMethod(string.Concat("Get", targetType?.Name));

    /// <summary>
    ///
    /// </summary>
    /// <param name="readerField"></param>
    /// <returns></returns>
    private static MethodInfo GetDbReaderGetValueOrDefaultMethod(DataReaderField readerField, Type readerType) =>
        GetDbReaderGetValueOrDefaultMethod(readerField.Type, readerType);

    /// <summary>
    ///
    /// </summary>
    /// <param name="targetType"></param>
    /// <returns></returns>
    private static MethodInfo GetDbReaderGetValueOrDefaultMethod(Type targetType, Type readerType) =>
        GetDbReaderGetValueMethod(targetType, readerType) ?? GetMethodInfo<DbDataReader>((x) => x.GetValue(default(int)));

    private static TEnum? EnumParseNull<TEnum>(string value) where TEnum : struct, System.Enum
    {
        if (Enum.TryParse<TEnum>(value, true, out var r))
            return r;
        else
            return null;
    }

    private static TEnum? EnumParseNullDefined<TEnum>(string value) where TEnum : struct, System.Enum
    {
        if (Enum.TryParse<TEnum>(value, true, out var r) && Enum.IsDefined(typeof(TEnum), r))
            return r;
        else
            return null;
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static T ThrowNullableValueIsNull<T>(string expressionDescription)
    {
        throw new InvalidOperationException($"Required Nullable<{typeof(T).Name}> property {expressionDescription} evaluated to null.");
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    ///
    /// <returns></returns>
    private static Expression ConvertExpressionToNullableValue(Expression expression)
    {
        var underlyingType = Nullable.GetUnderlyingType(expression.Type);
        if (underlyingType is null)
        {
            return expression; // Already non-nullable
        }

        var temp = Expression.Variable(expression.Type, "temp");
        var assign = Expression.Assign(temp, expression);

        var hasValue = Expression.Property(temp, nameof(Nullable<int>.HasValue));
        var value = Expression.Property(temp, nameof(Nullable<int>.Value));

        var description = Expression.Constant(
            (expression as MemberExpression)?.Member.Name ?? expression.ToString());

        var throwCall = Expression.Call(
            GetMethodInfo(() => ThrowNullableValueIsNull<int?>(""))
                .GetGenericMethodDefinition()
                .MakeGenericMethod(underlyingType),
            description);

        var conditional = Expression.Condition(hasValue, value, throwCall);

        return Expression.Block(new[] { temp }, assign, conditional);
    }

    private static Expression ConvertExpressionToNullableGetValueOrDefaultExpression(Func<Expression, Expression> converter, Expression expression)
    {
        if (Nullable.GetUnderlyingType(expression.Type) != null)
        {
            var converted = converter(ConvertExpressionToNullableValue(expression));
            var nullableType = typeof(Nullable<>).MakeGenericType(converted.Type);
            return Expression.Condition(
                Expression.Property(expression, nameof(Nullable<int>.HasValue)),
                Expression.Convert(converted, nullableType),
                Expression.Constant(null, nullableType)
            );
        }

        return converter(expression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToStringToGuid(Expression expression) =>
        Expression.New(StaticType.Guid.GetConstructor(new[] { StaticType.String })!, ConvertExpressionToNullableValue(expression));

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToTimeSpanToDateTime(Expression expression) =>
        Expression.New(StaticType.DateTime.GetConstructor(new[] { StaticType.Int64 })!,
            ConvertExpressionToNullableValue(ConvertExpressionToTimeSpanTicksExpression(expression)));

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToTimeSpanTicksExpression(Expression expression) =>
        Expression.Property(expression, GetPropertyInfo<TimeSpan>(x => x.Ticks));

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToDateTimeToTimeSpan(Expression expression) =>
        Expression.Property(expression, GetPropertyInfo<DateTime>(d => d.TimeOfDay));
#if NET
    private static Expression ConvertExpressionToDateOnlyToDateTime(Expression expression) =>
        Expression.Call(expression, GetMethodInfo<DateOnly>((x) => x.ToDateTime(default(TimeOnly))), Expression.Constant(default(TimeOnly)));

    private static Expression ConvertExpressionToDateTimeToDateOnly(Expression expression) =>
        Expression.Call(null, GetMethodInfo(() => DateOnly.FromDateTime(default(DateTime))), expression);

    private static Expression ConvertExpressionToTimeSpanToTimeOnly(Expression expression) =>
        Expression.Call(null, GetMethodInfo(() => TimeOnly.FromTimeSpan(default)), expression);

    private static Expression ConvertExpressionToTimeOnlyToTimeSpan(Expression expression) =>
        Expression.Call(expression, GetMethodInfo<TimeOnly>((x) => x.ToTimeSpan()));
#endif
    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="toType"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToSystemConvertExpression(Expression expression,
        Type toType)
    {
        var fromType = expression.Type;
        var underlyingFromType = TypeCache.Get(fromType).GetUnderlyingType();
        var underlyingToType = TypeCache.Get(toType).GetUnderlyingType();

        if (fromType == toType)
        {
            return expression;
        }

        // Identify
        if (underlyingToType.IsAssignableFrom(fromType))
        {
            return ConvertExpressionToTypeExpression(expression, underlyingToType);
        }

        var result = ConvertExpressionToNullableValue(expression);

        // Convert.To<Type>()
        if (underlyingFromType.IsEnum)
        {
            if (underlyingToType == StaticType.String)
            {
                result = Expression.Call(result, nameof(ToString), Array.Empty<Type>());
            }
            else if (underlyingToType.IsPrimitive &&
                (underlyingToType) == StaticType.Int16
                || underlyingToType == StaticType.Int32
                || underlyingToType == StaticType.Int64
                || underlyingToType == StaticType.Byte
                || underlyingToType == StaticType.UInt16
                || underlyingToType == StaticType.UInt32
                || underlyingToType == StaticType.UInt64
                || underlyingToType == StaticType.SByte)
            {
                result = Expression.Convert(result, Enum.GetUnderlyingType(underlyingFromType));

                if (result.Type != underlyingToType)
                    result = Expression.Convert(result, underlyingToType);
            }
            else if (underlyingToType == StaticType.Decimal)
            {
                // Oracle loves decimal
                result = Expression.Convert(result, Enum.GetUnderlyingType(underlyingFromType));

                if (result.Type != underlyingToType)
                    result = Expression.Convert(result, underlyingToType);
            }
            else
                return result; // Will fail
        }
        else if (toType == StaticType.String && fromType == StaticType.DateTime)
            result = Expression.Call(GetMethodInfo(() => StrictToString(DateTime.MinValue)), result);
        else if (toType == StaticType.String && fromType == StaticType.DateTimeOffset)
            result = Expression.Call(GetMethodInfo(() => StrictToString(DateTimeOffset.MinValue)), result);
#if NET
        else if (toType == StaticType.String && fromType == StaticType.DateOnly)
            result = Expression.Call(GetMethodInfo(() => StrictToString(DateOnly.MinValue)), result);
        else if (toType == StaticType.String && fromType == StaticType.TimeOnly)
            result = Expression.Call(GetMethodInfo(() => StrictToString(TimeOnly.MinValue)), result);
#endif
        else if (underlyingToType == StaticType.DateTime && underlyingFromType == StaticType.DateTimeOffset)
        {
            result = Expression.Property(result, nameof(DateTimeOffset.DateTime));
        }
        else if (underlyingToType == StaticType.DateTimeOffset && underlyingFromType == StaticType.DateTime)
        {
            result = Expression.New(typeof(DateTimeOffset).GetConstructor([typeof(DateTime)])!, [result]);
        }
        else if (underlyingToType == StaticType.Boolean && underlyingFromType == StaticType.String)
        {
            result = Expression.Call(GetMethodInfo(() => StrictParseBoolean(null)), expression);
        }
        else if (GetSystemConvertToTypeMethod(underlyingFromType, underlyingToType) is { } methodInfo)
        {
            result = Expression.Call(methodInfo, Expression.Convert(result, methodInfo.GetParameters()[0].ParameterType));
        }
        else if (GetSystemConvertChangeTypeMethod(underlyingToType) is { } systemChangeType)
        {
            result = Expression.Call(systemChangeType, new Expression[]
            {
                ConvertExpressionToTypeExpression(result, StaticType.Object),
                Expression.Constant(TypeCache.Get(underlyingToType).GetUnderlyingType())
            });
        }
        else if (underlyingFromType == StaticType.String)
        {
            if (underlyingToType == StaticType.Decimal)
            {
                result = Expression.Call(GetMethodInfo(() => StrictParseDecimal(null!)), expression);
            }
            else if (underlyingToType == StaticType.DateTime)
            {
                result = Expression.Call(GetMethodInfo(() => StrictParseDateTime(null!)), expression);
            }
            else if (underlyingToType == StaticType.DateTimeOffset)
            {
                result = Expression.Call(GetMethodInfo(() => StrictParseDateTimeOffset(null!)), expression);
            }
#if NET
            else if (underlyingToType == typeof(DateOnly))
            {
                result = Expression.Call(GetMethodInfo(() => StrictParseDateOnly(null!)), expression);
            }
            else if (underlyingToType == typeof(TimeOnly))
            {
                result = Expression.Call(GetMethodInfo(() => StrictParseTimeOnly(null!)), expression);
            }
#endif
            else
                return result; // Will fail
        }
        else
        {
            return result; // Will fail!
        }

        // Do we need manual NULL handling?
        if ((!underlyingToType.IsValueType || underlyingToType != toType)
            && (!underlyingFromType.IsValueType || underlyingFromType != fromType))
        {
            Expression condition;
            if (underlyingFromType != fromType)
            {
                // E.g. Nullable<System.Int32> -> string
                condition = Expression.Property(expression, nameof(Nullable<int>.HasValue));
            }
            else
            {
                // E.g. String -> Nullable<System.Int32>
                condition = Expression.NotEqual(expression, Expression.Constant(null, expression.Type));
            }

            return Expression.Condition(
                condition,
                (result.Type != toType) ? Expression.Convert(result, toType) : result,
                Expression.Constant(null, toType));
        }

        // Return
        return result;
    }

    static Expression UnwrapUnary(Expression e) => e is UnaryExpression ue ? UnwrapUnary(ue.Operand) : e;

    static MethodInfo GetMethodInfo<TFrom>(Expression<Action<TFrom>> call) => ((MethodCallExpression)call.Body).Method;

    static MethodInfo GetMethodInfo(Expression<Action> call)
    {
        return ((MethodCallExpression)call.Body).Method;
    }

    static bool StrictParseBoolean(string? value)
    {
        if (value is null)
            return false;
        else if (bool.TryParse(value, out var v))
            return v;
        else if (value.Length == 1)
        {
            if (value[0] == '1')
                return true;
            else if (value[0] == '0')
                return false;
        }

        throw new FormatException($"String '{value}' was not recognized as a valid Boolean");
    }

    static decimal StrictParseDecimal(string value)
    {
        return decimal.Parse(value, CultureInfo.InvariantCulture);
    }

    static DateTime StrictParseDateTime(string value)
    {
        return DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
    }

    static string StrictToString(DateTime value)
    {
        return value.ToString("o", CultureInfo.InvariantCulture);
    }

    static string StrictToString(DateTimeOffset value)
    {
        return value.ToString("o", CultureInfo.InvariantCulture);
    }

    static DateTimeOffset StrictParseDateTimeOffset(string value)
    {
        return DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.RoundtripKind); // If we have no offset assume no offset
    }

#if NET
    static DateOnly StrictParseDateOnly(string value)
    {
        try
        {
            return DateOnly.Parse(value, CultureInfo.InvariantCulture);
        }
        catch (FormatException) when (DateTime.TryParse(value, CultureInfo.InvariantCulture, out var vv) && DateOnly.FromDateTime(vv) is { } dateOnly && dateOnly.ToDateTime(TimeOnly.MinValue) == vv)
        {
            return dateOnly; // We can't guarantee that DateOnly is only serialized in text form as dateonly :(
        }
    }

    static string StrictToString(DateOnly value)
    {
        return value.ToString("d", CultureInfo.InvariantCulture);
    }

    static TimeOnly StrictParseTimeOnly(string value)
    {
        try
        {
            return TimeOnly.Parse(value, CultureInfo.InvariantCulture);
        }
        catch (FormatException) when (DateTime.TryParse(value, CultureInfo.InvariantCulture, out var vv) && TimeOnly.FromDateTime(vv) is { } timeOnly)
        {
            return timeOnly;
        }
    }

    static string StrictToString(TimeOnly value)
    {
        return value.ToString("o", CultureInfo.InvariantCulture);
    }
#endif

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="toType"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToTypeExpression(Expression expression,
        Type toType) =>
        (expression.Type != toType) ? Expression.Convert(expression, toType) : expression;

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="fromType"></param>
    /// <param name="toEnumType"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToEnumExpression(Expression expression,
        Type fromType,
        Type toEnumType,
        IDbSetting dbSetting) =>
        (fromType == StaticType.String) ?
            ConvertExpressionToEnumExpressionForString(expression, toEnumType) :
                ConvertExpressionToEnumExpressionForNonString(expression, toEnumType);

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="toEnumType"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToEnumExpressionForString(Expression expression, Type toEnumType)
    {
        var options = GlobalConfiguration.Options.EnumHandling;
        var parseMethod = (
            options is InvalidEnumValueHandling.Cast || toEnumType.GetCustomAttribute<FlagsAttribute>() is not null
                ? GetMethodInfo<Compiler>((x) => Compiler.EnumParseNull<DbType>(default!)).GetGenericMethodDefinition()
                : GetMethodInfo<Compiler>((x) => Compiler.EnumParseNullDefined<DbType>(default!)).GetGenericMethodDefinition()
        ).MakeGenericMethod(toEnumType);

        var parseCall = Expression.Call(parseMethod, expression);

        Expression fallbackExpression;
        if (options == InvalidEnumValueHandling.UseDefault)
        {
            fallbackExpression = Expression.Default(toEnumType);
        }
        else
        {
            // Create static helper call for throwing, but typed as `toEnumType`
            fallbackExpression = Expression.Call(
                GetMethodInfo(() => ThrowInvalidEnumValue<DbType>(default!)).GetGenericMethodDefinition().MakeGenericMethod(toEnumType),
                expression);
        }

        return Expression.Coalesce(parseCall, fallbackExpression);
    }

#if NET
    [DoesNotReturn]
#endif
    private static TEnum ThrowInvalidEnumValue<TEnum>(object value)
        where TEnum : struct, Enum
    {
        throw new ArgumentOutOfRangeException("value", value, $"Invalid value for {typeof(TEnum).Name}");
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="toEnumType"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToEnumExpressionForNonString(Expression expression,
        Type toEnumType)
    {
        if (GlobalConfiguration.Options.EnumHandling == InvalidEnumValueHandling.Cast)
        {
            return Expression.Convert(expression, toEnumType);
        }
        else
        {
            // Handle long/short to enum and/or non integer based enums
            if (expression.Type != Enum.GetUnderlyingType(toEnumType))
                expression = Expression.Convert(expression, Enum.GetUnderlyingType(toEnumType));

            return Expression.Condition(
                GetEnumIsDefinedExpression(expression, toEnumType), // Check if the value is defined
                Expression.Convert(expression, toEnumType), // Cast to enum
                GlobalConfiguration.Options.EnumHandling switch
                {
                    InvalidEnumValueHandling.UseDefault => Expression.Default(toEnumType),
                    InvalidEnumValueHandling.ThrowError => Expression.Throw(Expression.New(typeof(InvalidEnumArgumentException).GetConstructor(new[] { StaticType.String, StaticType.Int32, StaticType.Type })!,
                                                                new Expression[] { Expression.Constant("value"), Expression.Convert(expression, StaticType.Int32), Expression.Constant(toEnumType) }),
                        toEnumType
                    ),
                    _ => throw new InvalidEnumArgumentException("EnumHandling set to invalid value")
                }); // Default value for undefined
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="toType"></param>
    /// <returns></returns>
    private static Expression ConvertEnumExpressionToTypeExpression(Expression expression,
        Type toType)
    {
        var underlyingType = TypeCache.Get(toType).GetUnderlyingType();
        if (underlyingType == StaticType.String || underlyingType == StaticType.Boolean)
        {
            return ConvertEnumExpressionToTypeExpressionForString(expression);
        }
        else
        {
            return ConvertEnumExpressionToTypeExpressionForNonString(expression, toType);
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    private static Expression ConvertEnumExpressionToTypeExpressionForString(Expression expression)
    {
        var method = GetMethodInfo(() => Convert.ToString((object?)null));

        // Variables
        Expression? isNullExpression = null;
        Expression? trueExpression = null;
        Expression falseExpression;

        // Ensure (Ref/Nullable)
        if (TypeCache.Get(expression.Type).IsNullable())
        {
            // Check
            isNullExpression = Expression.Equal(Expression.Constant(null), expression);

            // True
            trueExpression = Expression.Convert(Expression.Constant(null), StaticType.String);
        }

        // False
        var methodCallExpression = Expression.Call(method, ConvertExpressionToTypeExpression(expression, StaticType.Object));
        falseExpression = ConvertExpressionToTypeExpression(methodCallExpression, StaticType.String);

        // Call and return
        return isNullExpression == null ? falseExpression :
            Expression.Condition(isNullExpression, trueExpression!, falseExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="toType"></param>
    /// <returns></returns>
    private static Expression ConvertEnumExpressionToTypeExpressionForNonString(Expression expression,
        Type toType)
    {
        Expression? isNullExpression = null;
        Expression? trueExpression = null;
        var falseExpression = expression;

        // Ensure (Ref/Nullable)
        var cachedType = TypeCache.Get(expression.Type);
        if (cachedType.IsNullable())
        {
            isNullExpression = Expression.Equal(Expression.Constant(null), expression);
            trueExpression = GetNullableTypeExpression(toType);
        }

        // Casting
        if (TypeCache.Get(toType).GetUnderlyingType() is { } tt && cachedType.GetUnderlyingType() != tt)
        {
            if (tt != StaticType.Decimal)
            {
                falseExpression = ConvertExpressionToTypeExpression(expression, tt);
            }
            else
            {
                // Oracle loves decimal
                falseExpression =
                    ConvertExpressionToTypeExpression(
                        ConvertExpressionToTypeExpression(expression, Enum.GetUnderlyingType(cachedType.GetUnderlyingType())),
                        tt);
            }
        }

        // Nullable
        if (cachedType.IsNullable())
        {
            falseExpression = ConvertExpressionToNullableExpression(falseExpression, toType);
        }

        // Return
        return isNullExpression == null ? falseExpression :
            Expression.Condition(isNullExpression, trueExpression, falseExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToDbNullExpression(Expression expression)
    {
        var valueIsNullExpression = Expression.Equal(expression, Expression.Constant(null));
        var dbNullValueExpresion = ConvertExpressionToTypeExpression(Expression.Constant(DBNull.Value), StaticType.Object);
        return Expression.Condition(valueIsNullExpression, dbNullValueExpresion, expression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="targetNullableType"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToNullableExpression(Expression expression,
        Type targetNullableType)
    {
        if (expression.Type.IsValueType == false)
        {
            return expression;
        }

        var underlyingType = Nullable.GetUnderlyingType(expression.Type);
        targetNullableType = TypeCache.Get(targetNullableType).GetUnderlyingType();

        if (targetNullableType.IsValueType && (underlyingType == null || underlyingType != targetNullableType))
        {
            var nullableType = StaticType.Nullable.MakeGenericType(targetNullableType);
            var constructor = nullableType.GetConstructor(new[] { targetNullableType })!;
            expression = TypeCache.Get(expression.Type).IsNullable() ? expression :
                Expression.New(constructor, ConvertExpressionToTypeExpression(expression, targetNullableType));
        }

        return expression;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="trueToType"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionWithAutomaticConversion(Expression expression,
        Type trueToType)
    {
        var fromType = TypeCache.Get(expression.Type).GetUnderlyingType();
        var toType = TypeCache.Get(trueToType)?.GetUnderlyingType();

        // Guid to String
        if (fromType == StaticType.Guid && toType == StaticType.String)
        {
            var result = Expression.Call(ConvertExpressionToNullableValue(expression), GetMethodInfo<Guid>(x => x.ToString()));

            if (fromType != expression.Type)
            {
                // Handle nullability
                expression = Expression.Condition(
                    Expression.Property(expression, nameof(Nullable<Guid>.HasValue)),
                    result,
                    Expression.Constant(null, StaticType.String)
                    );
            }
            else
                expression = result;
        }
        else if (fromType == StaticType.Guid && toType == StaticType.ByteArray)
        {
            var result = Expression.Call(ConvertExpressionToNullableValue(expression), GetMethodInfo<Guid>(x => x.ToByteArray()));

            if (fromType != expression.Type)
            {
                // Handle nullability
                expression = Expression.Condition(
                    Expression.Property(expression, nameof(Nullable<Guid>.HasValue)),
                    result,
                    Expression.Constant(null, StaticType.ByteArray)
                    );
            }
            else
                expression = result;
        }
        // String to Guid
        else if (fromType == StaticType.String && toType == StaticType.Guid)
            expression = ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToStringToGuid, expression);
        else if (fromType == StaticType.TimeSpan && toType == StaticType.DateTime)
            expression = ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToTimeSpanToDateTime, expression);
        else if (fromType == StaticType.DateTime && toType == StaticType.TimeSpan)
            expression = ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToDateTimeToTimeSpan, expression);
#if NET
        else if (fromType == StaticType.DateTime && toType == StaticType.DateOnly)
            expression = ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToDateTimeToDateOnly, expression);
        else if (fromType == StaticType.DateOnly && toType == StaticType.DateTime)
            expression = ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToDateOnlyToDateTime, expression);
        else if (fromType == StaticType.TimeSpan && toType == StaticType.TimeOnly)
            expression = ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToTimeSpanToTimeOnly, expression);
        else if (fromType == StaticType.TimeOnly && toType == StaticType.TimeSpan)
            expression = ConvertExpressionToNullableGetValueOrDefaultExpression(ConvertExpressionToTimeOnlyToTimeSpan, expression);
#endif
        // Others
        else if (toType != StaticType.SqlVariant)
        {
            expression = ConvertExpressionToSystemConvertExpression(expression, trueToType);
        }

        // Return
        return expression;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="readerExpression"></param>
    /// <param name="handlerInstance"></param>
    /// <param name="classPropertyParameterInfo"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToPropertyHandlerGetExpression(Expression expression,
        Expression readerExpression,
        object handlerInstance,
        ClassPropertyParameterInfo classPropertyParameterInfo)
    {
        // Return if null
        if (handlerInstance == null)
        {
            return expression;
        }

        // Variables Needed
        var getMethod = GetPropertyHandlerGetMethod(handlerInstance)!;
        var getParameter = GetPropertyHandlerGetParameter(getMethod)!;

        // Call the PropertyHandler.Get
        expression = Expression.Call(Expression.Constant(handlerInstance), getMethod, new[]
        {
            ConvertExpressionToTypeExpression(expression, getParameter.ParameterType),
            CreatePropertyHandlerGetOptionsExpression(readerExpression, classPropertyParameterInfo?.ClassProperty)
        });

        // Convert to the return type
        return ConvertExpressionToTypeExpression(expression, getMethod.ReturnType);
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="entityExpression"></param>
    /// <param name="readerParameterExpression"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToClassHandlerGetExpression<TResult>(Expression entityExpression,
        ParameterExpression readerParameterExpression)
    {
        var typeOfResult = typeof(TResult);

        // Check the handler
        var handlerInstance = GetClassHandler(typeOfResult);
        if (handlerInstance == null)
        {
            return entityExpression;
        }

        // Validate
        var handlerType = handlerInstance.GetType();
        if (handlerType.IsClassHandlerValidForModel(typeOfResult) == false)
        {
            throw new InvalidTypeException($"The class handler '{handlerType.FullName}' cannot be used for the type '{typeOfResult.FullName}'.");
        }

        // Call the ClassHandler.Get method
        var getMethod = GetClassHandlerGetMethod(handlerInstance);
        return Expression.Call(Expression.Constant(handlerInstance),
            getMethod,
            entityExpression,
            CreateClassHandlerGetOptionsExpression(readerParameterExpression));
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="parameterExpression"></param>
    /// <param name="classProperty"></param>
    /// <param name="targetType"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToPropertyHandlerSetExpression(Expression expression,
        Expression? parameterExpression,
        ClassProperty? classProperty,
        Type targetType) =>
        ConvertExpressionToPropertyHandlerSetExpressionTuple(expression, parameterExpression, classProperty, targetType).convertedExpression;

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="parameterExpression"></param>
    /// <param name="classProperty"></param>
    /// <param name="targetType"></param>
    /// <returns></returns>
    private static (Expression convertedExpression, Type? handlerSetReturnType) ConvertExpressionToPropertyHandlerSetExpressionTuple(Expression expression,
        Expression? parameterExpression,
        ClassProperty? classProperty,
        Type targetType)
    {
        var handlerInstance = classProperty?.GetPropertyHandler() ??
            PropertyHandlerCache.Get<object>(targetType);

        // Check
        if (handlerInstance == null)
        {
            return (expression, null);
        }

        // Variables
        var setMethod = GetPropertyHandlerSetMethod(handlerInstance)!;
        var setParameter = GetPropertyHandlerSetParameter(setMethod)!;

        // Nullable
        expression = ConvertExpressionToNullableExpression(expression,
            TypeCache.Get(setParameter.ParameterType).GetUnderlyingType() ?? targetType);

        // Call
        var valueExpression = ConvertExpressionToTypeExpression(expression, setParameter.ParameterType);
        expression = Expression.Call(Expression.Constant(handlerInstance),
            setMethod,
            new[]
            {
                valueExpression,
                CreatePropertyHandlerSetOptionsExpression(parameterExpression,classProperty)
            });

        // Align
        return (ConvertExpressionToTypeExpression(expression, setMethod.ReturnType), setMethod.ReturnType);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="commandExpression"></param>
    /// <param name="resultType"></param>
    /// <param name="entityOrEntitiesExpression"></param>
    /// <returns></returns>
    private static Expression ConvertExpressionToClassHandlerSetExpression(Expression commandExpression,
        Type resultType,
        Expression entityOrEntitiesExpression)
    {
        // Check the handler
        var handlerInstance = GetClassHandler(resultType);
        if (handlerInstance == null)

        {
            return entityOrEntitiesExpression;
        }

        // Validate
        var handlerType = handlerInstance.GetType();
        if (handlerType.IsClassHandlerValidForModel(resultType) == false)
        {
            throw new InvalidTypeException($"The class handler '{handlerType.FullName}' cannot be used for type '{resultType.FullName}'.");
        }

        // Call the IClassHandler.Set method
        //var typeOfListEntity = typeof(IList<>).MakeGenericType(StaticType.Object);
        //var type = typeOfListEntity.IsAssignableFrom(entityOrEntitiesExpression.Type) ?
        //    typeOfListEntity : resultType;
        var setMethod = GetClassHandlerSetMethod(handlerInstance);
        entityOrEntitiesExpression = Expression.Call(Expression.Constant(handlerInstance),
            setMethod,
            ConvertExpressionToTypeExpression(entityOrEntitiesExpression, resultType),
            CreateClassHandlerSetOptionsExpression(commandExpression));

        // Return the block
        return entityOrEntitiesExpression;
    }

    #endregion

    #region Common

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="enumType"></param>
    /// <returns></returns>
    private static Expression GetEnumIsDefinedExpression(Expression expression,
        Type enumType)
    {
        var parameters = new Expression[]
        {
            Expression.Constant(enumType),
            ConvertExpressionToTypeExpression(expression, StaticType.Object)
        };
        return Expression.Call(GetMethodInfo<Enum>((x) => Enum.IsDefined(default!, default!)), parameters);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="enumType"></param>
    /// <returns></returns>
    private static Expression GetEnumGetNameExpression(Expression expression,
        Type enumType)
    {
        var parameters = new Expression[]
        {
            Expression.Constant(enumType),
            ConvertExpressionToTypeExpression(expression, StaticType.Object)
        };
        return Expression.Call(GetMethodInfo<Enum>((x) => Enum.GetName(default!, default!)), parameters);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="readerParameterExpression"></param>
    /// <param name="classPropertyParameterInfo"></param>
    /// <param name="readerField"></param>
    /// <returns></returns>
    private static Expression GetClassPropertyParameterInfoValueExpression(ParameterExpression readerParameterExpression,
        ClassPropertyParameterInfo classPropertyParameterInfo,
        DataReaderField readerField, IDbSetting dbSetting, Type readerType)
    {
        // False expression
        var falseExpression = GetClassPropertyParameterInfoIsDbNullFalseValueExpression(readerParameterExpression,
            classPropertyParameterInfo, readerField, dbSetting, readerType);

        // Skip if possible
        if (readerField.DbField?.IsNullable == false)
        {
            return falseExpression;
        }

        // IsDbNull Check
        var isDbNullExpression = Expression.Call(readerParameterExpression,
            StaticType.DbDataReader.GetMethod(nameof(DbDataReader.IsDBNull))!, Expression.Constant(readerField.Ordinal));

        // True Expression
        var trueExpression = GetClassPropertyParameterInfoIsDbNullTrueValueExpression(readerParameterExpression,
            classPropertyParameterInfo, readerField);

        // Set the value
        return Expression.Condition(isDbNullExpression, trueExpression, falseExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="readerExpression"></param>
    /// <param name="classPropertyParameterInfo"></param>
    /// <param name="readerField"></param>
    ///
    /// <returns></returns>
    private static Expression GetClassPropertyParameterInfoIsDbNullTrueValueExpression(Expression readerExpression,
        ClassPropertyParameterInfo classPropertyParameterInfo,
        DataReaderField readerField)
    {
        var parameterType = GetPropertyHandlerGetParameter(classPropertyParameterInfo)?.ParameterType;
        var classPropertyParameterInfoType = classPropertyParameterInfo.GetTargetType();

        // get handler on class property or type level. for detect default value type and convert
        var handlerInstance = GetHandlerInstance(classPropertyParameterInfo, readerField) ?? PropertyHandlerCache.Get<object>(classPropertyParameterInfo.GetTargetType()!);

        // default value expression
        var valueType = handlerInstance == null ?
            parameterType ?? classPropertyParameterInfoType :
            GetPropertyHandlerGetParameter(GetPropertyHandlerGetMethod(handlerInstance)!)!.ParameterType;
        Expression valueExpression = Expression.Default(valueType);

        // Property Handler
        try
        {
            valueExpression = ConvertExpressionToPropertyHandlerGetExpression(valueExpression, readerExpression, handlerInstance!, classPropertyParameterInfo);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.TrueExpression: Failed to convert the value expression for property handler '{handlerInstance?.GetType()}'. " +
                $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
        }

        // Align the type
        try
        {
            valueExpression = ConvertExpressionToTypeExpression(valueExpression, classPropertyParameterInfoType);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.TrueExpression: Failed to convert the value expression into its destination .NET CLR Type '{classPropertyParameterInfoType.FullName}'. " +
                $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
        }

        // Return
        return valueExpression;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="readerParameterExpression"></param>
    /// <param name="classPropertyParameterInfo"></param>
    /// <param name="readerField"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static Expression GetClassPropertyParameterInfoIsDbNullFalseValueExpression(ParameterExpression readerParameterExpression,
        ClassPropertyParameterInfo classPropertyParameterInfo,
        DataReaderField readerField,
        IDbSetting dbSetting,
        Type readerType)
    {
        var parameterType = GetPropertyHandlerGetParameter(classPropertyParameterInfo)?.ParameterType;
        var classPropertyParameterInfoType = classPropertyParameterInfo.GetTargetType();
        var targetType = parameterType ?? classPropertyParameterInfoType;
        var readerGetValueMethod = GetDbReaderGetValueOrDefaultMethod(readerField, readerType);
        var valueExpression = (Expression)GetDbReaderGetValueExpression(readerParameterExpression,
            readerGetValueMethod, readerField.Ordinal);
        var targetTypeUnderlyingType = TypeCache.Get(targetType).GetUnderlyingType();

        // get handler on class property or type level
        var handlerInstance = GetHandlerInstance(classPropertyParameterInfo, readerField) ?? PropertyHandlerCache.Get<object>(classPropertyParameterInfo.GetTargetType());

        // Enumerations
        if (targetTypeUnderlyingType.IsEnum)
        {
            // If it has a PropertyHandler and the parameter type is matching, then, skip the auto conversion.
            var autoConvertEnum = true;
            if (handlerInstance != null)
            {
                var getParameter = GetPropertyHandlerGetParameter(GetPropertyHandlerGetMethod(handlerInstance))!;
                autoConvertEnum = !(TypeCache.Get(getParameter.ParameterType).GetUnderlyingType() == readerField.Type);
            }
            if (autoConvertEnum)
            {
                try
                {
                    valueExpression = ConvertExpressionToEnumExpression(valueExpression, readerField.Type, targetTypeUnderlyingType, dbSetting);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to convert the value expression into enum type '{targetType.GetUnderlyingType()}'. " +
                        $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
                }

            }
        }
        else
        {
            // Auto-conversion
            try
            {
                valueExpression = ConvertExpressionWithAutomaticConversion(valueExpression, targetType);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.FalseExpression: Failed to automatically convert the value expression. " +
                    $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
            }
        }

        // Property Handler
        try
        {
            valueExpression = ConvertExpressionToPropertyHandlerGetExpression(
                valueExpression, readerParameterExpression, handlerInstance, classPropertyParameterInfo);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.FalseExpression: Failed to convert the value expression for property handler '{handlerInstance?.GetType()}'. " +
                $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
        }

        // Align the type
        try
        {
            valueExpression = ConvertExpressionToTypeExpression(valueExpression, classPropertyParameterInfoType);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Compiler.DataReader.IsDbNull.FalseExpression: Failed to convert the value expression into its destination .NET CLR Type '{classPropertyParameterInfoType.FullName}'. " +
                $"{classPropertyParameterInfo.GetDescriptiveContextString()}", ex);
        }

        // Return
        return valueExpression;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="targetType"></param>
    /// <returns></returns>
    private static Expression GetNullableTypeExpression(Type targetType) =>
        targetType.IsValueType
        ? Expression.New(StaticType.Nullable.MakeGenericType(TypeCache.Get(targetType).GetUnderlyingType()))
        : Expression.Constant(null, targetType);

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="readerFieldsName"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static IEnumerable<ClassPropertyParameterInfo> GetClassPropertyParameterInfos<TResult>(IEnumerable<string> readerFieldsName,
        IDbSetting dbSetting)
    {
        var typeOfResult = typeof(TResult);
        var list = new List<ClassPropertyParameterInfo>();

        // Parameter information
        var constructorInfo = typeOfResult.GetConstructorWithMostArguments();
        var parameterInfos = constructorInfo?.GetParameters().AsList();

        // Class properties
        var classProperties = PropertyCache
            .Get(typeOfResult)
            //.Where(property => property.PropertyInfo.CanWrite)
            .Where(property =>
                readerFieldsName?.FirstOrDefault(field =>
                    string.Equals(field.AsUnquoted(true, dbSetting), property.GetMappedName().AsUnquoted(true, dbSetting), StringComparison.OrdinalIgnoreCase)) != null)
            .AsList();

        // ParameterInfos
        parameterInfos?
            .ForEach(parameterInfo =>
            {
                var classProperty = classProperties.
                    FirstOrDefault(property =>
                        string.Equals(property.PropertyInfo.Name, parameterInfo.Name, StringComparison.OrdinalIgnoreCase));
                if (classProperty != null)
                {
                    list.Add(new ClassPropertyParameterInfo
                    {
                        ClassProperty = classProperty, //classProperty.PropertyInfo.CanWrite ? classProperty : null,
                        ParameterInfo = parameterInfo,
                        ParameterInfoMappedClassProperty = classProperty
                    });
                }
            });

        // ClassProperties
        classProperties
            .Where(property => property.PropertyInfo.CanWrite)
            .AsList()
            .ForEach(property =>
            {
                var listItem = list.FirstOrDefault(item => item.ClassProperty == property);
                if (listItem != null)
                {
                    return;
                }
                list.Add(new ClassPropertyParameterInfo { ClassProperty = property });
            });

        // Return the list
        return list;
    }

    /// <summary>
    /// Returns the list of the bindings for the entity.
    /// </summary>
    /// <typeparam name="TResult">The target entity type.</typeparam>
    /// <param name="readerParameterExpression">The data reader parameter.</param>
    /// <param name="readerFields">The list of fields to be bound from the data reader.</param>
    /// <param name="dbSetting">The database setting that is being used.</param>
    /// <returns>The enumerable list of <see cref="MemberBinding"/> objects.</returns>
    private static IEnumerable<MemberBinding>? GetMemberBindingsForDataEntity<TResult>(ParameterExpression readerParameterExpression,
        IEnumerable<DataReaderField> readerFields,
        IDbSetting dbSetting,
        Type readerType)
    {
        // Variables needed
        var readerFieldsName = readerFields.Select(f => f.Name.ToLowerInvariant()).AsList();
        var classProperties = GetClassPropertyParameterInfos<TResult>(readerFieldsName, dbSetting);

        // Check the presence
        if (classProperties?.Any() != true)
        {
            return null;
        }

        // Variables needed
        var memberBindings = new List<MemberBinding>();

        // Iterate each properties
        foreach (var p in classProperties)
        {
            var mappedName = p.ParameterInfoMappedClassProperty?.GetMappedName().AsUnquoted(true, dbSetting) ??
                p.ParameterInfo?.Name?.AsUnquoted(true, dbSetting) ??
                p.ClassProperty?.GetMappedName().AsUnquoted(true, dbSetting);

            // Skip if not found
            var ordinal = readerFieldsName.IndexOf(mappedName?.ToLowerInvariant());
            if (ordinal < 0)
            {
                continue;
            }

            // Get the value expression
            var readerField = readerFields.First(f => string.Equals(f.Name.AsUnquoted(true, dbSetting), mappedName.AsUnquoted(true, dbSetting), StringComparison.OrdinalIgnoreCase));
            var expression = GetClassPropertyParameterInfoValueExpression(readerParameterExpression,
                p, readerField, dbSetting, readerType);

            try
            {
                // Member values
                var memberAssignment = p.ClassProperty?.PropertyInfo?.CanWrite == true ?
                    Expression.Bind(p.ClassProperty.PropertyInfo, expression) : null;
                var argument = p.ParameterInfo != null ? expression : null;

                // Add the bindings
                memberBindings.Add(new MemberBinding
                {
                    ClassProperty = p.ClassProperty,
                    ParameterInfo = p?.ParameterInfo,
                    MemberAssignment = memberAssignment,
                    Argument = argument
                });
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Compiler.MemberBinding: Failed to bind the value expression into a property/ctor-argument. " +
                    $"{p.GetDescriptiveContextString()}", ex);
            }
        }

        // Return the value
        return memberBindings;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="readerParameterExpression"></param>
    /// <param name="ordinal"></param>
    /// <returns></returns>
    private static Expression GetDbNullExpression(ParameterExpression readerParameterExpression,
        int ordinal) =>
        GetDbNullExpression(readerParameterExpression, Expression.Constant(ordinal));

    /// <summary>
    ///
    /// </summary>
    /// <param name="readerParameterExpression"></param>
    /// <param name="ordinalExpression"></param>
    /// <returns></returns>
    private static Expression GetDbNullExpression(ParameterExpression readerParameterExpression,
        ConstantExpression ordinalExpression) =>
        Expression.Call(readerParameterExpression, StaticType.DbDataReader.GetMethod(nameof(DbDataReader.IsDBNull)), ordinalExpression);

    /// <summary>
    ///
    /// </summary>
    /// <param name="readerParameterExpression"></param>
    /// <param name="readerGetValueMethod"></param>
    /// <param name="ordinal"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbReaderGetValueExpression(ParameterExpression readerParameterExpression,
        MethodInfo readerGetValueMethod,
        int ordinal) =>
        GetDbReaderGetValueExpression(readerParameterExpression, readerGetValueMethod, Expression.Constant(ordinal));

    /// <summary>
    ///
    /// </summary>
    /// <param name="readerParameterExpression"></param>
    /// <param name="readerGetValueMethod"></param>
    /// <param name="ordinalExpression"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbReaderGetValueExpression(ParameterExpression readerParameterExpression,
        MethodInfo readerGetValueMethod,
        ConstantExpression ordinalExpression) =>
        Expression.Call(
            readerGetValueMethod.DeclaringType == StaticType.DbDataReader
            ? readerParameterExpression
            : Expression.Convert(readerParameterExpression, readerGetValueMethod.DeclaringType!), readerGetValueMethod, ordinalExpression);

    /// <summary>
    /// Returns the list of the bindings for the object.
    /// </summary>
    /// <param name="readerParameterExpression">The data reader parameter.</param>
    /// <param name="readerFields">The list of fields to be bound from the data reader.</param>
    /// <returns>The enumerable list of child elements initializations.</returns>
    private static IEnumerable<ElementInit> GetMemberBindingsForDictionary(ParameterExpression readerParameterExpression,
        IList<DataReaderField> readerFields,
        Type readerType)
    {
        // Initialize variables
        var elementInits = new List<ElementInit>();
        var addMethod = StaticType.IDictionaryStringObject.GetMethod("Add", new[] { StaticType.String, StaticType.Object })!;

        // Iterate each properties
        for (var ordinal = 0; ordinal < readerFields?.Count; ordinal++)
        {
            var readerField = readerFields[ordinal];
            var readerGetValueMethod = GetDbReaderGetValueOrDefaultMethod(readerField, readerType);
            var expression = (Expression)GetDbReaderGetValueExpression(readerParameterExpression, readerGetValueMethod, ordinal);

            // Check for nullables
            if (readerField.DbField == null || readerField.DbField?.IsNullable == true)
            {
                var isDbNullExpression = GetDbNullExpression(readerParameterExpression, ordinal);
                var toType = (readerField.Type?.IsValueType != true) ? (readerField.Type ?? StaticType.Object) : StaticType.Object;
                expression = Expression.Condition(isDbNullExpression, Expression.Default(toType),
                    ConvertExpressionToTypeExpression(expression, toType));
            }

            // Add to the bindings
            var values = new Expression[]
            {
                Expression.Constant(readerField.Name),
                ConvertExpressionToTypeExpression(expression, StaticType.Object)
            };
            elementInits.Add(Expression.ElementInit(addMethod, values));
        }

        // Return the result
        return elementInits;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="entityInstanceExpression"></param>
    /// <param name="classProperty"></param>
    /// <param name="dbField"></param>
    /// <returns></returns>
    private static Expression GetEntityInstancePropertyValueExpression(Expression entityInstanceExpression,
        ClassProperty classProperty,
        DbField dbField)
    {
        var expression = (Expression)Expression.Property(entityInstanceExpression, classProperty.PropertyInfo);

        // Target type
        var handlerInstance = classProperty.GetPropertyHandler() ?? PropertyHandlerCache.Get<object>(TypeCache.Get(dbField.Type).GetUnderlyingType());
        var targetType = GetPropertyHandlerSetParameter(handlerInstance)?.ParameterType
            ?? (classProperty.GetDbType() is { } dbt ? new DbTypeToClientTypeResolver().Resolve(dbt) : null)
            ?? dbField.TypeNullable();

        if (targetType.IsValueType && dbField.IsNullable)
        {
            if (Nullable.GetUnderlyingType(targetType) == null)
                targetType = typeof(Nullable<>).MakeGenericType(targetType);
        }

        /*
         * Note: The other data provider can coerce the Enum into its destination data type in the DB by default,
         *       except for PostgreSQL. The code written below is only to address the issue for this specific provider.
         */

        // Enum Handling
        if (TypeCache.Get(classProperty.PropertyInfo.PropertyType).GetUnderlyingType() is { } underlyingType
            && underlyingType.IsEnum == true)
        {
            try
            {
                if (!IsPostgreSqlUserDefined(dbField))
                {
                    var dbType = classProperty.GetDbType() ?? underlyingType.GetDbType();
                    var toType = dbType.HasValue ? new DbTypeToClientTypeResolver().Resolve(dbType.Value) : targetType;

                    expression = ConvertEnumExpressionToTypeExpression(expression, toType);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Compiler.Entity/Object.Property: Failed to convert the value expression from " +
                    $"enumeration '{classProperty.PropertyInfo.PropertyType.FullName}' to type '{targetType?.GetUnderlyingType()}'. {classProperty}", ex);
            }
        }

        // Auto-conversion Handling
        try
        {
            var origExpression = expression;
            expression = ConvertExpressionWithAutomaticConversion(expression, targetType);

            if (dbField?.IsIdentity == true
                && targetType.IsValueType && !TypeCache.Get(targetType).IsNullable()
                && TypeCache.Get(origExpression.Type).IsNullable())
            {
                var nullableType = typeof(Nullable<>).MakeGenericType(expression.Type);

                // Don't set '0' in the identity output property
                expression = Expression.Condition(
                    Expression.Property(origExpression, nameof(Nullable<int>.HasValue)),
                    Expression.Convert(expression, nullableType),
                    Expression.Constant(null, nullableType));
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Compiler.Entity/Object.Property: Failed to automatically convert the value expression for " +
                $"property '{classProperty.GetMappedName()} ({classProperty.PropertyInfo.PropertyType.FullName})'. {classProperty}", ex);
        }

        // Property Handler
        try
        {
            expression = ConvertExpressionToPropertyHandlerSetExpression(
                expression, null, classProperty, TypeCache.Get(dbField?.Type).GetUnderlyingType());
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Compiler.Entity/Object.Property: Failed to convert the value expression for property handler '{handlerInstance?.GetType()}'. " +
                $"{classProperty}", ex);
        }

        // Return the Value
        return ConvertExpressionToTypeExpression(expression, StaticType.Object);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbField"></param>
    /// <returns></returns>
    private static bool IsPostgreSqlUserDefined(DbField dbField) =>
        string.Equals(dbField?.DatabaseType, "USER-DEFINED", StringComparison.OrdinalIgnoreCase) &&
        string.Equals(dbField?.Provider, "PGSQL", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    ///
    /// </summary>
    /// <param name="propertyExpression"></param>
    /// <param name="objectInstanceExpression"></param>
    /// <param name="dbField"></param>
    /// <returns></returns>
    private static Expression GetObjectInstancePropertyValueExpression(ParameterExpression propertyExpression,
        Expression objectInstanceExpression,
        DbField dbField)
    {
        var methodInfo = StaticType.PropertyInfo.GetMethod("GetValue", new[] { StaticType.Object });
        var expression = (Expression)Expression.Call(propertyExpression, methodInfo, objectInstanceExpression);

        // Property Handler
        expression = ConvertExpressionToPropertyHandlerSetExpression(expression,
            null, null, TypeCache.Get(dbField?.Type).GetUnderlyingType());

        // Convert to object
        return ConvertExpressionToTypeExpression(expression, StaticType.Object);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dictionaryInstanceExpression"></param>
    /// <param name="dbField"></param>
    /// <returns></returns>
    private static Expression GetDictionaryStringObjectPropertyValueExpression(Expression dictionaryInstanceExpression,
        DbField dbField)
    {
        var methodInfo = StaticType.IDictionaryStringObject.GetMethod("get_Item", new[] { StaticType.String });
        var expression = (Expression)Expression.Call(dictionaryInstanceExpression, methodInfo, Expression.Constant(dbField.Name));

        // Property Handler
        expression = ConvertExpressionToPropertyHandlerSetExpression(expression,
            null, null, TypeCache.Get(dbField.Type).GetUnderlyingType());

        // Convert to object
        return ConvertExpressionToTypeExpression(expression, StaticType.Object);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="entityExpression"></param>
    /// <param name="propertyExpression"></param>
    /// <param name="classProperty"></param>
    /// <param name="dbField"></param>
    /// <returns></returns>
    private static Expression GetDataEntityDbParameterValueAssignmentExpression(ParameterExpression dbParameterExpression,
        Expression entityExpression,
        ParameterExpression propertyExpression,
        ClassProperty? classProperty,
        DbField dbField)
    {
        Expression expression;

        // Get the property value
        if (propertyExpression.Type == StaticType.PropertyInfo)
        {
            expression = GetObjectInstancePropertyValueExpression(propertyExpression, entityExpression, dbField);
        }
        else
        {
            expression = GetEntityInstancePropertyValueExpression(entityExpression, classProperty!, dbField);
        }

        // Nullable? -> Convert to DBNull when necessary
        if (TypeCache.Get(expression.Type) is { } returnType && returnType.HasNullValue())
        {
            expression = ConvertExpressionToDbNullExpression(expression);
        }

        // Create the method call to set the parameter
        var setValueCall = Expression.Assign(Expression.Property(dbParameterExpression, GetPropertyInfo<DbParameter>(x => x.Value)), expression);

        // Use a static helper to throw the exception (to avoid closure allocation)
        var exceptionHelperMethod = GetMethodInfo(() => ThrowParameterAssignmentException("", default, default!));

        var ex = Expression.Parameter(typeof(ArgumentException), "ex");
        return Expression.TryCatch(
            Expression.Block(setValueCall, Expression.Empty()), Expression.Catch(ex,
            Expression.Call(exceptionHelperMethod,
                Expression.Constant(classProperty?.Name ?? dbField?.Name),
                expression,
                ex)));
    }

#if NET
    [DoesNotReturn]
#endif
    private static void ThrowParameterAssignmentException(string fieldName, object? value, ArgumentException ex)
    {
        if (ex is ArgumentOutOfRangeException)
            throw new ArgumentOutOfRangeException($"While setting {fieldName} to {value} ({value?.GetType()})", ex);
        else
            throw new ArgumentException($"While setting {fieldName} to {value} ({value?.GetType()})", ex);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="dictionaryInstanceExpression"></param>
    /// <param name="dbField"></param>
    /// 
    /// <returns></returns>
    private static Expression GetDictionaryStringObjectDbParameterValueAssignmentExpression(ParameterExpression dbParameterExpression,
        Expression dictionaryInstanceExpression,
        DbField dbField)
    {
        var expression = GetDictionaryStringObjectPropertyValueExpression(dictionaryInstanceExpression, dbField);

        // Nullable
        if (dbField?.IsNullable == true)
        {
            expression = ConvertExpressionToDbNullExpression(expression);
        }

        // Set the value
        return Expression.Assign(Expression.Property(dbParameterExpression, GetPropertyInfo<DbParameter>(x => x.Value)), expression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="classProperty"></param>
    /// <param name="dbField"></param>
    /// <returns></returns>
    private static DbType? GetDbType(ClassProperty? classProperty,
        DbField dbField)
    {
        var dbType = IsPostgreSqlUserDefined(dbField) ? DbType.Object : classProperty?.GetDbType();
        if (dbType == null)
        {
            var underlyingType = TypeCache.Get(dbField?.Type).GetUnderlyingType();
            dbType = TypeMapper.Get(underlyingType) ?? new ClientTypeToDbTypeResolver().Resolve(underlyingType);
        }
        return dbType;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    private static MethodCallExpression? GetDbParameterDbTypeAssignmentExpression(ParameterExpression dbParameterExpression,
        DbType? dbType)
    {
        MethodCallExpression? expression = null;

        // Set the DB Type
        if (dbType != null)
        {
            var dbParameterDbTypeSetMethod = StaticType.DbParameter.GetProperty(nameof(DbParameter.DbType))!.SetMethod!;
            expression = Expression.Call(dbParameterExpression, dbParameterDbTypeSetMethod, Expression.Constant(dbType));
        }

        // Return the expression
        return expression;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbCommandExpression"></param>
    /// 
    /// <returns></returns>
    private static MethodCallExpression GetDbCommandCreateParameterExpression(ParameterExpression dbCommandExpression)
    {
        var dbCommandCreateParameterMethod = StaticType.DbCommand.GetMethod(nameof(DbCommand.CreateParameter));
        return Expression.Call(dbCommandExpression, dbCommandCreateParameterMethod);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="dbField"></param>
    /// <param name="entityIndex"></param>
    /// <param name="dbSetting"></param>
    private static MethodCallExpression GetDbParameterNameAssignmentExpression(Expression dbParameterExpression,
        DbField dbField,
        int entityIndex,
        IDbSetting dbSetting)
    {
        var parameterName = dbField.Name.AsAlphaNumeric();
        parameterName = entityIndex > 0 ? string.Concat(dbSetting.ParameterPrefix, parameterName, "_", entityIndex.ToString(CultureInfo.InvariantCulture)) :
            string.Concat(dbSetting.ParameterPrefix, parameterName);
        return GetDbParameterNameAssignmentExpression(dbParameterExpression, parameterName);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="parameterName"></param>
    private static MethodCallExpression GetDbParameterNameAssignmentExpression(Expression dbParameterExpression,
        string parameterName) =>
        GetDbParameterNameAssignmentExpression(dbParameterExpression, Expression.Constant(parameterName));

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="paramaterNameExpression"></param>
    private static MethodCallExpression GetDbParameterNameAssignmentExpression(Expression dbParameterExpression,
        Expression paramaterNameExpression)
    {
        var dbParameterValueNameMethod = StaticType.DbParameter.GetProperty(nameof(DbParameter.ParameterName)).SetMethod;
        return Expression.Call(dbParameterExpression, dbParameterValueNameMethod, paramaterNameExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterValueAssignmentExpression(Expression dbParameterExpression,
        object value) =>
        GetDbParameterValueAssignmentExpression(dbParameterExpression, Expression.Constant(value));

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="valueExpression"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterValueAssignmentExpression(Expression dbParameterExpression,
        Expression valueExpression)
    {
        var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
        var dbParameterValueSetMethod = StaticType.DbParameter.GetProperty(nameof(DbParameter.Value)).SetMethod;
        var convertToDbNullMethod = StaticType.Converter.GetMethod(nameof(Converter.NullToDbNull));
        return Expression.Call(parameterExpression, dbParameterValueSetMethod,
            Expression.Call(convertToDbNullMethod, ConvertExpressionToTypeExpression(valueExpression, StaticType.Object)));
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="direction"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterDirectionAssignmentExpression(Expression dbParameterExpression,
        ParameterDirection direction) =>
        GetDbParameterDirectionAssignmentExpression(dbParameterExpression, Expression.Constant(direction));

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="directionExpression"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterDirectionAssignmentExpression(Expression dbParameterExpression,
        Expression directionExpression)
    {
        var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
        var dbParameterDirectionSetMethod = StaticType.DbParameter.GetProperty(nameof(DbParameter.Direction)).SetMethod;
        return Expression.Call(parameterExpression, dbParameterDirectionSetMethod, directionExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="size"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterSizeAssignmentExpression(Expression dbParameterExpression,
        int size) =>
        GetDbParameterSizeAssignmentExpression(dbParameterExpression, Expression.Constant(size));

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="sizeExpression"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterSizeAssignmentExpression(Expression dbParameterExpression,
        Expression sizeExpression)
    {
        var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
        var dbParameterSizeSetMethod = StaticType.DbParameter.GetProperty(nameof(DbParameter.Size)).SetMethod;
        return Expression.Call(parameterExpression, dbParameterSizeSetMethod, sizeExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="precision"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterPrecisionAssignmentExpression(Expression dbParameterExpression,
        byte precision) =>
        GetDbParameterPrecisionAssignmentExpression(dbParameterExpression, Expression.Constant(precision));

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="precisionExpression"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterPrecisionAssignmentExpression(Expression dbParameterExpression,
        Expression precisionExpression)
    {
        var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
        var dbParameterPrecisionSetMethod = StaticType.DbParameter.GetProperty(nameof(DbParameter.Precision)).SetMethod;
        return Expression.Call(parameterExpression, dbParameterPrecisionSetMethod, precisionExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="scale"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterScaleAssignmentExpression(Expression dbParameterExpression,
        byte scale) =>
        GetDbParameterScaleAssignmentExpression(dbParameterExpression, Expression.Constant(scale));

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <param name="scaleExpression"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbParameterScaleAssignmentExpression(Expression dbParameterExpression,
        Expression scaleExpression)
    {
        var parameterExpression = ConvertExpressionToTypeExpression(dbParameterExpression, StaticType.DbParameter);
        var dbParameterScaleSetMethod = GetPropertyInfo<DbParameter>(p => p.Scale).SetMethod!;
        return Expression.Call(parameterExpression, dbParameterScaleSetMethod, scaleExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterExpression"></param>
    /// <returns></returns>
    private static MethodCallExpression EnsureTableValueParameterExpression(Expression dbParameterExpression)
    {
        var method = StaticType.DbCommandExtension.GetMethod("EnsureTableValueParameter",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        return Expression.Call(method, dbParameterExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbCommandExpression"></param>
    /// <param name="dbParameterExpression"></param>
    /// <returns></returns>
    private static MethodCallExpression GetDbCommandParametersAddExpression(Expression dbCommandExpression,
        Expression dbParameterExpression)
    {
        var dbCommandParametersProperty = GetPropertyInfo<DbCommand>(d => d.Parameters);
        var dbParameterCollection = Expression.Property(dbCommandExpression, dbCommandParametersProperty);
        var dbParameterCollectionAddMethod = GetMethodInfo<DbParameterCollection>(x => x.Add(null!));
        return Expression.Call(dbParameterCollection, dbParameterCollectionAddMethod, dbParameterExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbParameterCollectionExpression"></param>
    /// <returns></returns>
    private static Expression GetDbParameterCollectionClearMethodExpression(MemberExpression dbParameterCollectionExpression)
    {
        var dbParameterCollectionClearMethod = GetMethodInfo<DbParameterCollection>(x => x.Clear());
        return Expression.Call(dbParameterCollectionExpression, dbParameterCollectionClearMethod);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbCommandExpression"></param>
    /// <param name="entityExpression"></param>
    /// <param name="fieldDirection"></param>
    /// <param name="entityIndex"></param>
    /// <param name="dbSetting"></param>
    /// <param name="dbHelper"></param>
    /// <returns></returns>
    private static Expression GetPropertyFieldExpression(ParameterExpression dbCommandExpression,
        Expression entityExpression,
        FieldDirection fieldDirection,
        int entityIndex,
        IDbSetting dbSetting,
        IDbHelper dbHelper)
    {
        var propertyListExpression = new List<Expression>();
        var propertyVariableListExpression = new List<ParameterExpression>();
        ParameterExpression? propertyVariableExpression = null;
        Expression? propertyInstanceExpression = null;
        ClassProperty? classProperty = null;
        var propertyName = fieldDirection.DbField.Name;

        // Set the proper assignments (property)
        if (TypeCache.Get(entityExpression.Type).IsClassType() == false)
        {
            var typeGetPropertyMethod = GetMethodInfo<Type>(t => t.GetProperty("", BindingFlags.Instance));
            var objectGetTypeMethod = StaticType.Object.GetMethod(nameof(object.GetType));
            propertyVariableExpression = Expression.Variable(StaticType.PropertyInfo, string.Concat("propertyVariable", propertyName));
            propertyInstanceExpression = Expression.Call(Expression.Call(entityExpression, objectGetTypeMethod),
                typeGetPropertyMethod, new[]
                {
                    Expression.Constant(propertyName),
                    Expression.Constant(BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)
                });
        }
        else
        {
            var entityProperties = PropertyCache.Get(entityExpression.Type);
            classProperty = entityProperties.FirstOrDefault(property =>
                string.Equals(property.GetMappedName().AsUnquoted(true, dbSetting),
                    propertyName.AsUnquoted(true, dbSetting), StringComparison.OrdinalIgnoreCase));

            if (classProperty != null)
            {
                propertyVariableExpression = Expression.Variable(classProperty.PropertyInfo.PropertyType, string.Concat("propertyVariable", propertyName));
                propertyInstanceExpression = Expression.Property(entityExpression, classProperty.PropertyInfo);
            }
            else
            {
                throw new PropertyNotFoundException(nameof(entityExpression), $"The property '{propertyName}' is not found from type '{entityExpression.Type}'. The current operation could not proceed.");
            }
        }

        // Add the variables
        if (propertyVariableExpression != null && propertyInstanceExpression != null)
        {
            propertyVariableListExpression.Add(propertyVariableExpression);
            propertyListExpression.Add(Expression.Assign(propertyVariableExpression, propertyInstanceExpression));

            // Execute the function
            var parameterAssignment = GetDataEntityParameterAssignmentExpression(dbCommandExpression,
                entityIndex,
                entityExpression,
                propertyVariableExpression,
                fieldDirection.DbField,
                classProperty,
                fieldDirection.Direction,
                dbSetting,
                dbHelper);
            propertyListExpression.Add(parameterAssignment);
        }

        // Add the property block
        return Expression.Block(propertyVariableListExpression, propertyListExpression);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="dbCommandExpression"></param>
    /// <returns></returns>
    private static Expression GetDbCommandParametersClearExpression(ParameterExpression dbCommandExpression)
    {
        var dbParameterCollection = Expression.Property(dbCommandExpression, GetPropertyInfo<DbCommand>(c => c.Parameters));
        return GetDbParameterCollectionClearMethodExpression(dbParameterCollection);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="entitiesParameterExpression"></param>
    /// <param name="typeOfListEntity"></param>
    /// <param name="entityIndex"></param>
    /// <returns></returns>
    private static MethodCallExpression GetListEntityIndexerExpression(Expression entitiesParameterExpression,
        Type typeOfListEntity,
        int entityIndex)
    {
        var listIndexerMethod = typeOfListEntity.GetMethod("get_Item", new[] { StaticType.Int32 })!;
        return Expression.Call(entitiesParameterExpression, listIndexerMethod,
            Expression.Constant(entityIndex));
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="resultType"></param>
    /// <param name="expression"></param>
    /// <returns></returns>
    private static Expression ThrowIfNullAfterClassHandlerExpression(Type resultType,
        Expression expression)
    {
        var isNullExpression = Expression.Equal(Expression.Constant(null), expression);
        var exception = new ArgumentNullException($"Entity of type '{resultType}' must not be null. If you have defined a class handler, please check the 'Set' method.");
        return Expression.IfThen(isNullExpression, Expression.Throw(Expression.Constant(exception)));
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="dbCommandExpression"></param>
    /// <param name="entitiesParameterExpression"></param>
    /// <param name="fieldDirections"></param>
    /// <param name="entityIndex"></param>
    /// <param name="dbSetting"></param>
    /// <param name="dbHelper"></param>
    /// <returns></returns>
    private static Expression GetIndexDbParameterSetterExpression(Type entityType,
        ParameterExpression dbCommandExpression,
        Expression entitiesParameterExpression,
        IEnumerable<FieldDirection> fieldDirections,
        int entityIndex,
        IDbSetting dbSetting,
        IDbHelper dbHelper)
    {
        // Get the current instance
        var entityVariableExpression = Expression.Variable(StaticType.Object, "instance");
        var typeOfListEntity = typeof(IList<>).MakeGenericType(StaticType.Object);
        var entityParameter = (Expression)GetListEntityIndexerExpression(entitiesParameterExpression, typeOfListEntity, entityIndex);
        var entityExpressions = new List<Expression>();
        var entityVariables = new List<ParameterExpression>();

        // Class handler
        entityParameter = ConvertExpressionToClassHandlerSetExpression(dbCommandExpression, entityType, entityParameter);

        // Entity instance
        entityVariables.Add(entityVariableExpression);
        entityExpressions.Add(Expression.Assign(entityVariableExpression, entityParameter));

        // Throw if null
        entityExpressions.Add(ThrowIfNullAfterClassHandlerExpression(entityType, entityVariableExpression));

        // Iterate the input fields
        foreach (var fieldDirection in fieldDirections)
        {
            // Add the property block
            var propertyBlock = GetPropertyFieldExpression(dbCommandExpression,
                ConvertExpressionToTypeExpression(entityVariableExpression, entityType),
                fieldDirection,
                entityIndex,
                dbSetting,
                dbHelper);

            // Add to instance expression
            entityExpressions.Add(propertyBlock);
        }

        // Add to the instance block
        return Expression.Block(entityVariables, entityExpressions);
    }

    #endregion
}
