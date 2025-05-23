using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using RepoDb.Enumerations;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Resolvers;

namespace RepoDb.DbHelpers;
public sealed class OracleDbHelper : IDbHelper
{
    private readonly IDbSetting m_dbSetting = DbSettingMapper.Get<OracleConnection>();

    public OracleDbHelper()
        : this(new OracleDbTypeToClientTypeResolver())
    {

    }

    public OracleDbHelper(IResolver<string, Type> dbTypeResolver)
    {
        DbTypeResolver = dbTypeResolver ?? throw new ArgumentNullException(nameof(dbTypeResolver));
    }

    public IResolver<string, Type> DbTypeResolver { get; }

    public void DynamicHandler<TEventInstance>(TEventInstance instance, string key)
    {
        if (key == "RepoDb.Internal.Compiler.Events[AfterCreateDbParameter]")
        {
            HandleDbParameterPostCreation(instance as OracleParameter);
        }

        void HandleDbParameterPostCreation(OracleParameter oracleParameter)
        {
            if (oracleParameter.Value is string)
            {
                oracleParameter.OracleDbType = OracleDbType.Varchar2;
            }
        }
    }

    const string GetFieldsQuery = @"
        SELECT 
            C.COLUMN_NAME,
            CASE WHEN PK.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IsPrimary,
            CASE WHEN C.IDENTITY_COLUMN = 'YES' THEN 1 ELSE 0 END AS IsIdentity,
            CASE WHEN C.NULLABLE = 'Y' THEN 1 ELSE 0 END AS IsNullable,
            C.DATA_TYPE AS DataType,
    
            -- Return character length for character types, otherwise use byte length
            CASE 
                WHEN C.CHAR_USED = 'C' THEN C.CHAR_LENGTH
                ELSE C.DATA_LENGTH
            END AS ""Size"",

            -- Expose precision and scale for NUMBER, FLOAT, etc.
            C.DATA_PRECISION AS Precision,
            C.DATA_SCALE AS Scale,

            CASE WHEN C.DATA_DEFAULT IS NOT NULL THEN 1 ELSE 0 END AS HasDefaultValue,
            CASE WHEN C.VIRTUAL_COLUMN = 'YES' THEN 1 ELSE 0 END AS IsComputed
        FROM ALL_TAB_COLS C
        LEFT JOIN (
            SELECT CC.OWNER, CC.TABLE_NAME, CC.COLUMN_NAME
            FROM ALL_CONSTRAINTS CONS
            JOIN ALL_CONS_COLUMNS CC 
              ON CONS.CONSTRAINT_NAME = CC.CONSTRAINT_NAME 
             AND CONS.OWNER = CC.OWNER
            WHERE CONS.CONSTRAINT_TYPE = 'P'
        ) PK ON PK.OWNER = C.OWNER 
            AND PK.TABLE_NAME = C.TABLE_NAME 
            AND PK.COLUMN_NAME = C.COLUMN_NAME
        WHERE C.TABLE_NAME = :TableName
            AND C.OWNER = :Schema
ORDER BY C.COLUMN_ID
    ";

    public IEnumerable<DbField> GetFields(IDbConnection connection, string tableName, IDbTransaction? transaction = null)
    {
        // Variables
        var commandText = GetFieldsQuery;
        var param = new
        {
            Schema = DataEntityExtension.GetSchema(tableName, m_dbSetting).AsUnquoted(m_dbSetting).ToUpperInvariant(),
            TableName = DataEntityExtension.GetTableName(tableName, m_dbSetting).AsUnquoted(m_dbSetting)
        };
        var param2 = (param.Schema == "USER") ? (object)new { param.TableName } : null;
        if (param2 is { })
        {
            commandText = commandText.Replace(":Schema", "USER");
        }

        // Iterate and extract
        using var reader = (DbDataReader)connection.ExecuteReader(commandText, param2 ?? param, transaction: transaction);

        var dbFields = new List<DbField>();

        // Iterate the list of the fields
        while (reader.Read())
        {
            dbFields.Add(ReaderToDbField(reader));
        }

        // Return the list of fields
        return dbFields;
    }

    public async Task<IEnumerable<DbField>> GetFieldsAsync(IDbConnection connection, string tableName, IDbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        var commandText = GetFieldsQuery;
        var param = new
        {
            Schema = DataEntityExtension.GetSchema(tableName, m_dbSetting).AsUnquoted(m_dbSetting).ToUpperInvariant(),
            TableName = DataEntityExtension.GetTableName(tableName, m_dbSetting).AsUnquoted(m_dbSetting)
        };
        var param2 = (param.Schema == "USER") ? (object)new { param.TableName } : null;
        if (param2 is { })
        {
            commandText = commandText.Replace(":Schema", "USER");
        }

        // Iterate and extract
        using var reader = (DbDataReader)await connection.ExecuteReaderAsync(commandText, param2 ?? param, transaction: transaction,
            cancellationToken: cancellationToken);

        var dbFields = new List<DbField>();

        // Iterate the list of the fields
        while (await reader.ReadAsync(cancellationToken))
        {
            dbFields.Add(ReaderToDbField(reader));
        }

        // Return the list of fields
        return dbFields;
    }

    #region GetSchemaObjects
    const string GetSchemaQuery = @"
        SELECT
            object_type ""Type"",
            object_name ""Name"",
            owner ""Schema""
        FROM all_objects
        WHERE object_type IN ('TABLE', 'VIEW')
          AND owner NOT IN (
            'SYS', 'SYSTEM', 'CTXSYS', 'XDB', 'MDSYS', 'ORDSYS', 'WMSYS',
            'EXFSYS', 'DBSNMP', 'APPQOSSYS', 'OUTLN', 'AUDSYS', 'GSMADMIN_INTERNAL',
            'OJVMSYS', 'ANONYMOUS', 'DVSYS', 'DVF', 'REMOTE_SCHEDULER_AGENT',
            'MGMT_VIEW', 'SI_INFORMTN_SCHEMA', 'APEX_PUBLIC_USER', 'FLOWS_FILES',
            'APEX_040000', 'APEX_050000', 'XS$NULL'
)
";

    private DbField ReaderToDbField(DbDataReader reader)
    {
        var dbType = reader.IsDBNull(4) ? "VARCHAR2" : reader.GetString(4);

        return new DbField(
            reader.GetString(0),                                // COLUMN_NAME
            !reader.IsDBNull(1) && reader.GetInt32(1) == 1,    // IsPrimary
            !reader.IsDBNull(2) && reader.GetInt32(2) == 1,    // IsIdentity
            !reader.IsDBNull(3) && reader.GetInt32(3) == 1,    // IsNullable
            DbTypeResolver.Resolve(dbType),
            reader.IsDBNull(5) ? null : reader.GetInt32(5),    // Size
            reader.IsDBNull(6) ? null : (byte)reader.GetInt32(6), // Precision
            reader.IsDBNull(7) ? null : (byte)reader.GetInt32(7), // Scale
            dbType,
            !reader.IsDBNull(8) && reader.GetInt32(8) == 1,    // HasDefaultValue
            !reader.IsDBNull(9) && reader.GetInt32(9) == 1,    // IsComputed
            "ORACLE"
        );
    }

    public IEnumerable<DbSchemaObject> GetSchemaObjects(IDbConnection connection, IDbTransaction? transaction = null)
    {
        return connection.ExecuteQuery<(string Type, string Name, string Schema)>(GetSchemaQuery, transaction)
                         .Select(MapSchemaQueryResult);
    }

    public async Task<IEnumerable<DbSchemaObject>> GetSchemaObjectsAsync(IDbConnection connection, IDbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        var results = await connection.ExecuteQueryAsync<(string Type, string Name, string Schema)>(GetSchemaQuery, transaction);
        return results.Select(MapSchemaQueryResult);
    }

    private static DbSchemaObject MapSchemaQueryResult((string Type, string Name, string Schema) r) =>
        new DbSchemaObject
        {
            Type = r.Type switch
            {
                "TABLE" => DbSchemaType.Table,
                "VIEW" => DbSchemaType.View,
                _ => throw new NotSupportedException($"Unsupported schema object type: {r.Type}")
            },
            Name = r.Name,
            Schema = r.Schema
        };
    #endregion

    public T GetScopeIdentity<T>(IDbConnection connection, IDbTransaction? transaction = null)
    {
        throw new NotImplementedException();
    }

    public Task<T> GetScopeIdentityAsync<T>(IDbConnection connection, IDbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}
