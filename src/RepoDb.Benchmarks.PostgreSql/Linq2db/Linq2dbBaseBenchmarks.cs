﻿using System.ComponentModel;
using System.Linq;
using BenchmarkDotNet.Attributes;
using DataModels;
using LinqToDB;
using RepoDb.Benchmarks.PostgreSql.Configurations;
using RepoDb.Benchmarks.PostgreSql.Setup;

namespace RepoDb.Benchmarks.PostgreSql.Linq2db;

[Description(OrmNameConstants.Linq2db)]
public class Linq2dbBaseBenchmarks : BaseBenchmark
{
    [GlobalSetup]
    public void Setup() => BaseSetup();

    protected override void Bootstrap()
    {
        using var db = GetDb();

        db.People.Select(x => x.Id == CurrentId).ToList();
    }
    
    protected static RepoDbDB GetDb()
    {
        var options = new DataOptions();
        options = options.UsePostgreSQL(DatabaseHelper.ConnectionString);
        
        return new RepoDbDB(options);
    }
}