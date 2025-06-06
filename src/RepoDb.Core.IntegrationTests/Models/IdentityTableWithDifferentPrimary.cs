﻿using RepoDb.Attributes;

namespace RepoDb.IntegrationTests.Models;

[Map("[sc].[IdentityTable]")]
public class IdentityTableWithDifferentPrimary
{
    public long Id { get; set; }
    [Primary]
    public Guid RowGuid { get; set; }
    public bool? ColumnBit { get; set; }
    public DateTime? ColumnDateTime { get; set; }
    public DateTime? ColumnDateTime2 { get; set; }
    public decimal? ColumnDecimal { get; set; }
    public double? ColumnFloat { get; set; }
    public int? ColumnInt { get; set; }
    public string ColumnNVarChar { get; set; }
}
