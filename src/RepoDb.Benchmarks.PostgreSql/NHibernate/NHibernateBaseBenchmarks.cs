﻿using System.ComponentModel;
using BenchmarkDotNet.Attributes;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Dialect;
using NHibernate.Mapping.ByCode;
using RepoDb.Benchmarks.PostgreSql.Configurations;
using RepoDb.Benchmarks.PostgreSql.Models;
using RepoDb.Benchmarks.PostgreSql.Setup;

namespace RepoDb.Benchmarks.PostgreSql.NHibernate;

[Description(OrmNameConstants.NHibernate)]
public class NHibernateBaseBenchmarks : BaseBenchmark
{
    protected ISessionFactory SessionFactory;

    [GlobalSetup]
    public void Setup()
    {
        BaseSetup();

        var configuration = new Configuration();
        configuration.DataBaseIntegration(properties =>
        {
            properties.Dialect<PostgreSQL83Dialect>();
            properties.ConnectionString = DatabaseHelper.ConnectionString;
        });

        var mapper = new ModelMapper();
        mapper.AddMapping<PersonMap>();
        var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
        configuration.AddMapping(mapping);

        SessionFactory = configuration.BuildSessionFactory();
    }

    protected override void Bootstrap()
    {
        // The compilation is explicity added at the Setup() method
    }
}