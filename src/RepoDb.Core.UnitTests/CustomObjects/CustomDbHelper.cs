using System.Data;
using RepoDb.DbSettings;
using RepoDb.Interfaces;

namespace RepoDb.UnitTests.CustomObjects;

public class CustomDbHelper : BaseDbHelper
{
    public CustomDbHelper()
        : base(new MyResolver())
    {

    }

    public override IEnumerable<DbField> GetFields(IDbConnection connection,
        string tableName,
        IDbTransaction? transaction = null)
    {
        return new[]
        {
            new DbField("Id", true, true, false, typeof(int), null, null, null, null),
            new DbField("Name", false, false, true, typeof(string), null, null, null, null)
        };
    }

    public override T GetScopeIdentity<T>(IDbConnection connection,
        IDbTransaction? transaction = null)
    {
        return default;
    }

    public override IEnumerable<DbSchemaObject> GetSchemaObjects(IDbConnection connection, IDbTransaction? transaction = null)
    {
        return [];
    }

    private sealed class MyResolver : IResolver<string, Type>
    {
        public Type? Resolve(string input)
        {
            throw new NotImplementedException();
        }
    }
}
