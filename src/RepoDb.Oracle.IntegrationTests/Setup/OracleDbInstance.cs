using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using RepoDb.TestCore;

namespace RepoDb.Oracle.IntegrationTests.Setup;

public class OracleDbInstance : DbInstance<OracleConnection>
{
    static OracleDbInstance()
    {
        GlobalConfiguration.Setup(GlobalConfiguration.Options).UseOracle();
    }

    public OracleDbInstance()
    {
        // Master connection
        Environment.SetEnvironmentVariable("NLS_LANGUAGE", "AMERICAN.AL32UTF8");

        AdminConnectionString =
            Environment.GetEnvironmentVariable("REPODB_ORACLE_CONSTR_MASTER")
            ?? @"Data Source=127.0.0.1:41521/FREEPDB1;User Id=system;Password=oracle;"; // Docker Test Configuration

        // RepoDb connection
        ConnectionString = AdminConnectionString
            .Replace("=system;", "=repodb_user;")
            .Replace("=oracle;", "=0608B012-05D2-4023-A451;");
    }

    protected override async Task CreateUserDatabase(DbConnection sql)
    {
        await sql.ExecuteNonQueryAsync($@"
            DECLARE
              v_count INTEGER;
            BEGIN
              SELECT COUNT(*) INTO v_count FROM all_users WHERE username = 'REPODB_USER';
              IF v_count = 0 THEN
                EXECUTE IMMEDIATE 'CREATE USER repodb_user IDENTIFIED BY ""0608B012-05D2-4023-A451""';
                EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO repodb_user';
                EXECUTE IMMEDIATE 'CREATE TABLESPACE REPODB_TS DATAFILE ''repodb_ts01.dbf'' SIZE 100M';
                EXECUTE IMMEDIATE 'ALTER USER REPODB_USER DEFAULT TABLESPACE REPODB_TS';
                EXECUTE IMMEDIATE 'ALTER USER REPODB_USER QUOTA UNLIMITED ON REPODB_TS';
              END IF;
            END;");
    }
}
