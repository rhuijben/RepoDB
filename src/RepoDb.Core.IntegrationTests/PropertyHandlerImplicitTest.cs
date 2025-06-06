﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb.Extensions;
using RepoDb.IntegrationTests.Setup;
using RepoDb.Interfaces;
using System.Data;
using Microsoft.Data.SqlClient;
using RepoDb.Options;

namespace RepoDb.IntegrationTests;

[TestClass]
public class PropertyHandlerImplicitTest
{
    [ClassInitialize]
    public static void ClassInitialize(TestContext context)
    {
        Database.Initialize();
        Setup();
    }

    [TestInitialize]
    public void Initialize()
    {
        Database.Initialize();
        Cleanup();
    }

    [TestCleanup]
    public void Cleanup()
    {
        Database.Cleanup();
    }

    private static void Setup()
    {
        FluentMapper
            .Entity<EntityModelForClass>()
            .Table("[dbo].[PropertyHandler]")
            .Column(e => e.NVarCharAsClass, "ColumnNVarChar")
            .PropertyHandler<PropertyToClassHandler>(e => e.NVarCharAsClass);

        FluentMapper
            .Entity<ImmutableEntityModelForClass>()
            .Table("[dbo].[PropertyHandler]")
            .Column(e => e.NVarCharAsClass, "ColumnNVarChar")
            .PropertyHandler<PropertyToClassHandler>(e => e.NVarCharAsClass);

        FluentMapper
            .Entity<EntityModelForIntToStringType>()
            .Table("[dbo].[PropertyHandler]")
            .Column(e => e.IntAsString, "ColumnInt")
            .PropertyHandler<IntToStringTypeHandler>(e => e.IntAsString)
            .Column(e => e.IntNotNullAsString, "ColumnIntNotNull")
            .PropertyHandler<IntToStringTypeHandler>(e => e.IntNotNullAsString);

        FluentMapper
            .Entity<EntityModelForNumberPropertiesToLongType>()
            .Table("[dbo].[PropertyHandler]")
            .Column(e => e.DecimalAsLong, "ColumnDecimal")
            .Column(e => e.FloatAsLong, "ColumnFloat")
            .PropertyHandler<PropertiesToLongTypeHandler>(e => e.FloatAsLong);

        FluentMapper
            .Entity<EntityModelForDateTimeKind>()
            .Table("[dbo].[PropertyHandler]")
            .Column(e => e.DateTime, "ColumnDateTime")
            .Column(e => e.DateTimeNotNull, "ColumnDateTimeNotNull")
            .Column(e => e.DateTime2, "ColumnDateTime2")
            .DbType(e => e.DateTime2, DbType.DateTime2)
            .Column(e => e.DateTime2NotNull, "ColumnDateTime2NotNull")
            .DbType(e => e.DateTime2NotNull, DbType.DateTime2);

        FluentMapper
            .Type<decimal>()
            .PropertyHandler<DecimalToLongTypeHandler>();
    }

    #region Handlers

    /// <summary>
    /// A class used to handle the property transformation from a property to a class.
    /// </summary>
    private class PropertyToClassHandler : IPropertyHandler<string, TargetModel>
    {
        public TargetModel Get(string input,
            PropertyHandlerGetOptions options)
        {
            return new TargetModel { Value = input };
        }

        public string Set(TargetModel input,
            PropertyHandlerSetOptions options)
        {
            return input?.Value;
        }
    }

    /// <summary>
    /// A class used to handle the property transformation from Int to String.
    /// </summary>
    public class IntToStringTypeHandler : IPropertyHandler<int?, string>
    {
        public string Get(int? input,
            PropertyHandlerGetOptions options)
        {
            return input > 0 ? Convert.ToString(input) : null;
        }

        public int? Set(string input,
            PropertyHandlerSetOptions options)
        {
            return Convert.ToInt32(input);
        }
    }

    /// <summary>
    /// A class used to handle the property transformation from Decimal to Long. The values are nullable.
    /// </summary>
    public class DecimalToLongTypeHandler : IPropertyHandler<decimal?, long?>
    {
        public long? Get(decimal? input,
            PropertyHandlerGetOptions options)
        {
            if (input > 0)
            {
                return Convert.ToInt64(input);
            }
            else
            {
                if (options.ClassProperty.PropertyInfo.PropertyType.IsNullable())
                {
                    return null;
                }
                else
                {
                    return default(long);
                }
            }
        }

        public decimal? Set(long? input,
            PropertyHandlerSetOptions options)
        {
            return Convert.ToDecimal(input);
        }
    }

    /// <summary>
    /// A class used to handle the property transformation of any property to type long.
    /// </summary>
    public class PropertiesToLongTypeHandler : IPropertyHandler<object, long?>
    {
        public long? Get(object input,
            PropertyHandlerGetOptions options)
        {
            var value = Convert.ToInt64(input);
            if (value > 0)
            {
                return value;
            }
            else
            {
                if (options.ClassProperty.PropertyInfo.PropertyType.IsNullable())
                {
                    return null;
                }
                else
                {
                    return default(long);
                }
            }
        }

        public object Set(long? input,
            PropertyHandlerSetOptions options)
        {
            return input;
        }
    }

    /// <summary>
    /// A class used to handle the property transformation of <see cref="DateTime.Kind" /> property. The values are not nullable.
    /// </summary>
    public class DateTimeToUtcKindHandler : IPropertyHandler<DateTime?, DateTime?>
    {
        public DateTime? Get(DateTime? input,
            PropertyHandlerGetOptions options)
        {
            return input.HasValue ?
                DateTime.SpecifyKind(input.Value, DateTimeKind.Utc) :
                (DateTime?)null;
        }

        public DateTime? Set(DateTime? input,
            PropertyHandlerSetOptions options)
        {
            return input.HasValue ?
                DateTime.SpecifyKind(input.Value, DateTimeKind.Unspecified) :
                (DateTime?)null;
        }
    }

    #endregion

    #region Classes

    private class TargetModel
    {
        public string Value { get; set; }
    }

    private class EntityModelForClass
    {
        public long Id { get; set; }

        public TargetModel NVarCharAsClass { get; set; }

        // Other non-nullables

        public int ColumnIntNotNull { get; set; } = 0;

        public decimal ColumnDecimalNotNull { get; set; } = 0;

        public short ColumnFloatNotNull { get; set; } = 0;

        public DateTime ColumnDateTimeNotNull { get; set; } = System.DateTime.UtcNow.Date;

        public DateTime ColumnDateTime2NotNull { get; set; } = System.DateTime.UtcNow;
    }

    private class ImmutableEntityModelForClass
    {
        public ImmutableEntityModelForClass(long id,
            TargetModel nvarCharAsClass)
        {
            Id = id;
            NVarCharAsClass = nvarCharAsClass;
        }

        public long Id { get; }

        public TargetModel NVarCharAsClass { get; }

        // Other non-nullables

        public int ColumnIntNotNull { get; set; } = 0;

        public decimal ColumnDecimalNotNull { get; set; } = 0;

        public short ColumnFloatNotNull { get; set; } = 0;

        public DateTime ColumnDateTimeNotNull { get; set; } = System.DateTime.UtcNow.Date;

        public DateTime ColumnDateTime2NotNull { get; set; } = System.DateTime.UtcNow;
    }

    private class EntityModelForIntToStringType
    {
        public long Id { get; set; }

        public string IntAsString { get; set; }

        public string IntNotNullAsString { get; set; }

        // Other non-nullables

        public decimal ColumnDecimalNotNull { get; set; } = 0;

        public short ColumnFloatNotNull { get; set; } = 0;

        public DateTime ColumnDateTimeNotNull { get; set; } = System.DateTime.UtcNow.Date;

        public DateTime ColumnDateTime2NotNull { get; set; } = System.DateTime.UtcNow;
    }

    private class EntityModelForNumberPropertiesToLongType
    {
        public long Id { get; set; }

        public long? DecimalAsLong { get; set; }

        public long? FloatAsLong { get; set; }

        // Other non-nullables

        public int ColumnIntNotNull { get; set; } = 0;

        public decimal ColumnDecimalNotNull { get; set; } = 0;

        public short ColumnFloatNotNull { get; set; } = 0;

        public DateTime ColumnDateTimeNotNull { get; set; } = System.DateTime.UtcNow.Date;

        public DateTime ColumnDateTime2NotNull { get; set; } = System.DateTime.UtcNow;
    }

    private class EntityModelForDateTimeKind
    {
        public long Id { get; set; }

        public DateTime? DateTime { get; set; }

        public DateTime DateTimeNotNull { get; set; } = System.DateTime.UtcNow.Date;

        public DateTime? DateTime2 { get; set; }

        public DateTime DateTime2NotNull { get; set; }

        // Other non-nullables

        public int ColumnIntNotNull { get; set; } = 0;

        public decimal ColumnDecimalNotNull { get; set; } = 0;

        public short ColumnFloatNotNull { get; set; } = 0;
    }

    #endregion

    #region Helpers

    private IEnumerable<EntityModelForClass> CreateEntityModelForClasses(int count,
        bool isModelNull = false)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new EntityModelForClass
            {
                NVarCharAsClass = isModelNull ? null : new TargetModel
                {
                    Value = $"Value-{i}-{Guid.NewGuid()}",
                }
            };
        }
    }

    private IEnumerable<ImmutableEntityModelForClass> CreateImmutableEntityModelForClasses(int count,
        bool isModelNull = false)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new ImmutableEntityModelForClass((i + 1), isModelNull ? null :
                new TargetModel
                {
                    Value = $"Value-{i}-{Guid.NewGuid()}",
                });
        }
    }

    private IEnumerable<EntityModelForIntToStringType> CreateEntityModelForIntToStringTypes(int count,
        bool isIntNull = false)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new EntityModelForIntToStringType
            {
                IntAsString = isIntNull ? null : Convert.ToString(new Random().Next(int.MaxValue)),
                IntNotNullAsString = isIntNull ? null : Convert.ToString(new Random().Next(int.MaxValue))
            };
        }
    }

    private IEnumerable<EntityModelForDateTimeKind> CreateEntityModelForDateTimeKinds(int count,
        bool isDateTimeNull = false)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new EntityModelForDateTimeKind
            {
                DateTime = isDateTimeNull ? null : (DateTime?)DateTime.UtcNow.Date,
                DateTime2 = isDateTimeNull ? null : (DateTime?)DateTime.UtcNow
            };
        }
    }

    private IEnumerable<EntityModelForNumberPropertiesToLongType> CreateEntityModelForNumberPropertiesToLongTypes(int count,
        bool isIntNull = false)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new EntityModelForNumberPropertiesToLongType
            {
                DecimalAsLong = isIntNull ? null : (long?)100,
                FloatAsLong = isIntNull ? null : (long?)200
            };
        }
    }

    #endregion

    #region PropertyToClass

    [TestMethod]
    public void TestPropertyHandlerImplicitWithPropertyToClassHandler()
    {
        // Setup
        var models = CreateEntityModelForClasses(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<EntityModelForClass>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e.NVarCharAsClass, item.NVarCharAsClass);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithPropertyToClassHandlerAtomic()
    {
        // Setup
        var models = CreateEntityModelForClasses(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<EntityModelForClass>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e.NVarCharAsClass, item.NVarCharAsClass);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithPropertyToClassHandlerAsNull()
    {
        // Setup
        var models = CreateEntityModelForClasses(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<EntityModelForClass>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Assert.IsNull(item.NVarCharAsClass.Value);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithPropertyToClassHandlerAsNullAtomic()
    {
        // Setup
        var models = CreateEntityModelForClasses(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<EntityModelForClass>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Assert.IsNull(item.NVarCharAsClass.Value);
            });
        }
    }

    #endregion

    #region PropertyToImmutableClass

    [TestMethod]
    public void TestPropertyHandlerImplicitWithPropertyToImmutableClassHandler()
    {
        // Setup
        var models = CreateImmutableEntityModelForClasses(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<ImmutableEntityModelForClass>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e.NVarCharAsClass, item.NVarCharAsClass);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithPropertyToImmutableClassHandlerAtomic()
    {
        // Setup
        var models = CreateImmutableEntityModelForClasses(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<ImmutableEntityModelForClass>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e.NVarCharAsClass, item.NVarCharAsClass);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithPropertyToImmutableClassHandlerAsNull()
    {
        // Setup
        var models = CreateImmutableEntityModelForClasses(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<ImmutableEntityModelForClass>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Assert.IsNull(item.NVarCharAsClass.Value);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithPropertyToImmutableClassHandlerAsNullAtomic()
    {
        // Setup
        var models = CreateImmutableEntityModelForClasses(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<ImmutableEntityModelForClass>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Assert.IsNull(item.NVarCharAsClass.Value);
            });
        }
    }

    #endregion

    #region IntToString

    [TestMethod]
    public void TestPropertyHandlerImplicitWithIntToStringTypeHandler()
    {
        // Setup
        var models = CreateEntityModelForIntToStringTypes(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<EntityModelForIntToStringType>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithIntToStringTypeHandlerAtomic()
    {
        // Setup
        var models = CreateEntityModelForIntToStringTypes(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<EntityModelForIntToStringType>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithIntToStringTypeHandlerAsNull()
    {
        // Setup
        var models = CreateEntityModelForIntToStringTypes(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<EntityModelForIntToStringType>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithIntToStringTypeHandlerAsNullAtomic()
    {
        // Setup
        var models = CreateEntityModelForIntToStringTypes(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<EntityModelForIntToStringType>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    #endregion

    #region DecimalToLong/NumbersToLong

    [TestMethod]
    public void TestPropertyHandlerImplicitWithNumbersToLongHandler()
    {
        // Setup
        PropertyHandlerMapper.Add(typeof(decimal), new DecimalToLongTypeHandler(), true);

        // Setup
        var models = CreateEntityModelForNumberPropertiesToLongTypes(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<EntityModelForNumberPropertiesToLongType>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithNumbersToLongHandlerAtomic()
    {
        // Setup
        PropertyHandlerMapper.Add(typeof(decimal), new DecimalToLongTypeHandler(), true);

        // Setup
        var models = CreateEntityModelForNumberPropertiesToLongTypes(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<EntityModelForNumberPropertiesToLongType>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithNumbersToLongHandlerAsNull()
    {
        // Setup
        var models = CreateEntityModelForNumberPropertiesToLongTypes(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<EntityModelForNumberPropertiesToLongType>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitWithNumbersToLongHandlerAsNullAtomic()
    {
        // Setup
        var models = CreateEntityModelForNumberPropertiesToLongTypes(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<EntityModelForNumberPropertiesToLongType>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    #endregion

    #region DateTimeKind

    [TestMethod]
    public void TestPropertyHandlerImplicitForDateTimeKind()
    {
        // Setup
        PropertyHandlerMapper.Add(typeof(DateTime), new DateTimeToUtcKindHandler(), true);

        // Setup
        var models = CreateEntityModelForDateTimeKinds(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<EntityModelForDateTimeKind>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitForDateTimeKindAtomic()
    {
        // Setup
        PropertyHandlerMapper.Add(typeof(DateTime), new DateTimeToUtcKindHandler(), true);

        // Setup
        var models = CreateEntityModelForDateTimeKinds(10).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<EntityModelForDateTimeKind>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitForDateTimeKindAsNull()
    {
        // Setup
        PropertyHandlerMapper.Add(typeof(DateTime), new DateTimeToUtcKindHandler(), true);

        // Setup
        var models = CreateEntityModelForDateTimeKinds(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            connection.InsertAll(models);

            // Act
            var result = connection.QueryAll<EntityModelForDateTimeKind>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    [TestMethod]
    public void TestPropertyHandlerImplicitForDateTimeKindAsNullAtomic()
    {
        // Setup
        PropertyHandlerMapper.Add(typeof(DateTime), new DateTimeToUtcKindHandler(), true);

        // Setup
        var models = CreateEntityModelForDateTimeKinds(10, true).AsList();

        using (var connection = new SqlConnection(Database.ConnectionStringForRepoDb))
        {
            // Act
            models.ForEach(e => connection.Insert(e));

            // Act
            var result = connection.QueryAll<EntityModelForDateTimeKind>();

            // Assert
            models.ForEach(e =>
            {
                var item = result.First(obj => obj.Id == e.Id);
                Helper.AssertPropertiesEquality(e, item);
            });
        }
    }

    #endregion
}
