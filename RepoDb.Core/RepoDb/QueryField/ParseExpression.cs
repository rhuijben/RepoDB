﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using RepoDb.Enumerations;
using RepoDb.Exceptions;
using RepoDb.Extensions;

namespace RepoDb
{
    public partial class QueryField
    {
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="field"></param>
        /// <returns></returns>
        private static ClassProperty GetTargetProperty<TEntity>(Field field)
            where TEntity : class
        {
            var properties = PropertyCache.Get<TEntity>();

            // Failing at some point - for base interfaces
            var property = properties
                .FirstOrDefault(p =>
                    string.Equals(p.GetMappedName(), field.Name, StringComparison.OrdinalIgnoreCase));

            // Matches to the actual class properties
            if (property == null)
            {
                property = properties
                    .FirstOrDefault(p =>
                        string.Equals(p.PropertyInfo.Name, field.Name, StringComparison.OrdinalIgnoreCase));
            }

            // Return the value
            return property;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expressionType"></param>
        /// <returns></returns>
        internal static Operation GetOperation(ExpressionType expressionType)
        {
            return expressionType switch
            {
                ExpressionType.Equal => Operation.Equal,
                ExpressionType.GreaterThan => Operation.GreaterThan,
                ExpressionType.LessThan => Operation.LessThan,
                ExpressionType.NotEqual => Operation.NotEqual,
                ExpressionType.GreaterThanOrEqual => Operation.GreaterThanOrEqual,
                ExpressionType.LessThanOrEqual => Operation.LessThanOrEqual,
                _ => throw new NotSupportedException($"Operation: Expression '{expressionType}' is currently not supported.")
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="enumerable"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        private static QueryField ToIn(string fieldName,
            System.Collections.IEnumerable enumerable,
            ExpressionType? unaryNodeType = null)
        {
            var operation = unaryNodeType == ExpressionType.Not ? Operation.NotIn : Operation.In;
            return new QueryField(fieldName, operation, enumerable.WithType<object>().AsArray(), null, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="enumerable"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        private static IEnumerable<QueryField> ToQueryFields(string fieldName,
            System.Collections.IEnumerable enumerable,
            ExpressionType? unaryNodeType = null)
        {
            var operation = (unaryNodeType == ExpressionType.Not || unaryNodeType == ExpressionType.NotEqual) ?
                Operation.NotEqual : Operation.Equal;
            foreach (var item in enumerable)
            {
                yield return new QueryField(fieldName, operation, item, null, false);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        private static QueryField ToLike(string fieldName,
            object value,
            ExpressionType? unaryNodeType = null)
        {
            var operation = unaryNodeType == ExpressionType.Not ? Operation.NotLike : Operation.Like;
            return new QueryField(fieldName, operation, value, null, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="methodName"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private static string ConvertToLikeableValue(string methodName,
            string value)
        {
            if (methodName == "Contains")
            {
                value = value.StartsWith("%", StringComparison.OrdinalIgnoreCase) ? value : string.Concat("%", value);
                value = value.EndsWith("%", StringComparison.OrdinalIgnoreCase) ? value : string.Concat(value, "%");
            }
            else if (methodName == "StartsWith")
            {
                value = value.EndsWith("%", StringComparison.OrdinalIgnoreCase) ? value : string.Concat(value, "%");
            }
            else if (methodName == "EndsWith")
            {
                value = value.StartsWith("%", StringComparison.OrdinalIgnoreCase) ? value : string.Concat("%", value);
            }
            return value;
        }

        /*
         * Binary
         */

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal static QueryGroup Parse<TEntity>(BinaryExpression expression)
            where TEntity : class
        {
            // Only support the following expression type
            if (expression.IsExtractable() == false)
            {
                throw new NotSupportedException($"Expression '{expression}' is currently not supported.");
            }

            // Field
            var field = expression.GetField(out var coalesceValue);
            var property = GetTargetProperty<TEntity>(field);

            // Check
            if (property == null)
            {
                throw new InvalidExpressionException($"Invalid expression '{expression}'. The property {field.Name} is not defined on a target type '{typeof(TEntity).FullName}'.");
            }
            else
            {
                field = property.AsField();
            }

            // Value
            var value = expression.GetValue();

            // Operation
            var operation = GetOperation(expression.NodeType);

            if (value is { } && TypeCache.Get(property.PropertyInfo.PropertyType).GetUnderlyingType() is { } ut && ut.IsEnum)
            {
                value = ToEnumValue(ut, value);
            }

            var check = new QueryField(field, operation, value, null, false);

            if (coalesceValue is { })
            {
                if (operation is Operation.Equal && Equals(value, coalesceValue) && value is { })
                {
                    // X = @Y OR X IS NULL

                    return new QueryGroup(new[] { check, new QueryField(field, Operation.IsNull, value, null, false) }, Conjunction.Or);
                }
                else if (operation is Operation.NotEqual && !Equals(value, coalesceValue))
                {
                    // X <> @Y OR X IS NULL
                    return new QueryGroup(new[] { check, new QueryField(field, Operation.IsNull, value, null, false) }, Conjunction.Or);
                }
                else
                    throw new InvalidExpressionException($"Invalid expression '??' can only be applied in an Equals or NotEquals .");
            }
            else if (operation == Operation.Equal)
            {
                if (value == null)
                    check = new QueryField(field, Operation.IsNull, value, null, false);
                else if (GlobalConfiguration.Options.BooleanNotEquals)
                    return new QueryGroup(new[] { check, new QueryField(field, Operation.IsNotNull, value, null, false) }, Conjunction.And);
            }
            else if (operation == Operation.NotEqual)
            {
                if (value == null)
                    check = new QueryField(field, Operation.IsNotNull, value, null, false);
                else if (GlobalConfiguration.Options.BooleanNotEquals)
                {
                    // X != @Y OR X is NULL
                    return new QueryGroup(new[] { check, new QueryField(field, Operation.IsNull, value, null, false) }, Conjunction.Or);
                }
            }

            // Return the value
            return new QueryGroup(check.AsEnumerable());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="enumType"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private static object ToEnumValue(Type enumType,
            object value) =>
            (value != null ?
                ToEnumValue(enumType, Enum.GetName(enumType, value)) : null) ?? value;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="enumType"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        private static object ToEnumValue(Type enumType,
            string name) =>
            !string.IsNullOrEmpty(name) && Enum.IsDefined(enumType, name) ?
                Enum.Parse(enumType, name) : null;

        /*
         * Member
         */

        internal static IEnumerable<QueryField> Parse<TEntity>(MemberExpression expression,
            ExpressionType? unaryNodeType = null)
            where TEntity : class
        {
            // Property
            var property = GetProperty<TEntity>(expression);

            // Operation
            var operation = unaryNodeType == ExpressionType.Not ? Operation.NotEqual : Operation.Equal;

            // Value
            var value = (object)null;
            if (expression.Type == StaticType.Boolean)
            {
                value = true;
            }
            else
            {
                value = expression.GetValue();
            }

            // Return
            return new QueryField(property.GetMappedName(), operation, value, null, false).AsEnumerable();
        }

        /*
         * MethodCall
         */

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="expression"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        internal static IEnumerable<QueryField> Parse<TEntity>(MethodCallExpression expression,
        ExpressionType? unaryNodeType = null)
        where TEntity : class
        {
            if (expression.Method.Name == "Equals")
            {
                return ParseEquals<TEntity>(expression, unaryNodeType)?.AsEnumerable();
            }
            else if (expression.Method.Name == "CompareString")
            {
                // Usual case for VB.Net (Microsoft.VisualBasic.CompilerServices.Operators.CompareString #767)
                return ParseCompareString<TEntity>(expression, unaryNodeType)?.AsEnumerable();
            }
            else if (expression.Method.Name == "Contains")
            {
                return ParseContains<TEntity>(expression, unaryNodeType)?.AsEnumerable();
            }
            else if (expression.Method.Name == "StartsWith" ||
                expression.Method.Name == "EndsWith")
            {
                return ParseWith<TEntity>(expression, unaryNodeType)?.AsEnumerable();
            }
            else if (expression.Method.Name == "All")
            {
                return ParseAll<TEntity>(expression, unaryNodeType);
            }
            else if (expression.Method.Name == "Any")
            {
                return ParseAny<TEntity>(expression, unaryNodeType)?.AsEnumerable();
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="expression"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        internal static QueryField ParseEquals<TEntity>(MethodCallExpression expression,
            ExpressionType? unaryNodeType = null)
            where TEntity : class
        {
            // Property
            var property = GetProperty<TEntity>(expression);

            // Value
            if (expression?.Object != null)
            {
                if (expression.Object?.Type == StaticType.String)
                {
                    var value = Converter.ToType<string>(expression.Arguments.First().GetValue());
                    return new QueryField(property.GetMappedName(), value);
                }
            }

            // Return
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="expression"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        internal static QueryField ParseCompareString<TEntity>(MethodCallExpression expression,
            ExpressionType? unaryNodeType = null)
            where TEntity : class
        {
            // Property
            var property = expression.Arguments.First().ToMember().Member;

            // Value
            var value = Converter.ToType<string>(expression.Arguments.ElementAt(1).GetValue());

            // Return
            return new QueryField(property.GetMappedName(), value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="expression"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        internal static QueryField ParseContains<TEntity>(MethodCallExpression expression,
            ExpressionType? unaryNodeType = null)
            where TEntity : class
        {
            // Property
            var property = GetProperty<TEntity>(expression);

            // Value
            if (expression?.Object != null)
            {
                if (expression.Object?.Type == StaticType.String)
                {
                    var likeable = ConvertToLikeableValue("Contains", Converter.ToType<string>(expression.Arguments.First().GetValue()));
                    return ToLike(property.GetMappedName(), likeable, unaryNodeType);
                }
                else
                {
                    var enumerable = Converter.ToType<System.Collections.IEnumerable>(expression.Object.GetValue());
                    return ToIn(property.GetMappedName(), enumerable, unaryNodeType);
                }
            }
            else
            {
                var enumerable = Converter.ToType<System.Collections.IEnumerable>(expression.Arguments.First().GetValue());
                return ToIn(property.GetMappedName(), enumerable, unaryNodeType);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="expression"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        internal static QueryField ParseWith<TEntity>(MethodCallExpression expression,
            ExpressionType? unaryNodeType = null)
            where TEntity : class
        {
            // Property
            var property = GetProperty<TEntity>(expression);

            // Values
            var value = Converter.ToType<string>(expression.Arguments.First().GetValue());

            // Fields
            return ToLike(property.GetMappedName(),
                ConvertToLikeableValue(expression.Method.Name, value), unaryNodeType);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="expression"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        internal static IEnumerable<QueryField> ParseAll<TEntity>(MethodCallExpression expression,
            ExpressionType? unaryNodeType = null)
            where TEntity : class
        {
            // Property
            var property = GetProperty<TEntity>(expression);

            // Value
            var enumerable = Converter.ToType<System.Collections.IEnumerable>(expression.Arguments.First().GetValue());
            return ToQueryFields(property.GetMappedName(), enumerable, unaryNodeType);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="expression"></param>
        /// <param name="unaryNodeType"></param>
        /// <returns></returns>
        internal static IEnumerable<QueryField> ParseAny<TEntity>(MethodCallExpression expression,
            ExpressionType? unaryNodeType = null)
            where TEntity : class
        {
            // Property
            var property = GetProperty<TEntity>(expression);

            // Value
            var enumerable = Converter.ToType<System.Collections.IEnumerable>(expression.Arguments.First().GetValue());
            return ToQueryFields(property.GetMappedName(), enumerable, unaryNodeType);
        }

        #region GetProperty

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal static ClassProperty GetProperty<TEntity>(Expression expression)
            where TEntity : class
        {
            return expression switch
            {
                null => null,
                LambdaExpression lambdaExpression => GetProperty<TEntity>(lambdaExpression),
                BinaryExpression binaryExpression => GetProperty<TEntity>(binaryExpression),
                MethodCallExpression methodCallExpression => GetProperty<TEntity>(methodCallExpression),
                MemberExpression memberExpression => GetProperty<TEntity>(memberExpression),
                _ => null
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal static ClassProperty GetProperty<TEntity>(LambdaExpression expression)
            where TEntity : class =>
            GetProperty<TEntity>(expression.Body);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal static ClassProperty GetProperty<TEntity>(BinaryExpression expression)
            where TEntity : class =>
            GetProperty<TEntity>(expression.Left) ?? GetProperty<TEntity>(expression.Right);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal static ClassProperty GetProperty<TEntity>(MethodCallExpression expression)
            where TEntity : class =>
            expression?.Object?.Type == StaticType.String ?
            GetProperty<TEntity>(expression.Object.ToMember()) :
            GetProperty<TEntity>(expression.Arguments.LastOrDefault());

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal static ClassProperty GetProperty<TEntity>(MemberExpression expression)
            where TEntity : class =>
            GetProperty<TEntity>(expression.Member.ToPropertyInfo());

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="propertyInfo"></param>
        /// <returns></returns>
        internal static ClassProperty GetProperty<TEntity>(PropertyInfo propertyInfo)
            where TEntity : class
        {
            if (propertyInfo == null)
            {
                return null;
            }

            // Variables
            var properties = PropertyCache.Get<TEntity>();
            var name = PropertyMappedNameCache.Get(propertyInfo);

            // Failing at some point - for base interfaces
            var property = properties
                .FirstOrDefault(p =>
                    string.Equals(p.GetMappedName(), name, StringComparison.OrdinalIgnoreCase));

            // Matches to the actual class properties
            if (property == null)
            {
                property = properties
                    .FirstOrDefault(p =>
                        string.Equals(p.PropertyInfo.Name, name, StringComparison.OrdinalIgnoreCase));
            }

            // Return the value
            return property;
        }

        #endregion
    }
}
