using System.Reflection;
using RepoDb;

namespace FindDependencies;

internal class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Hello, World!");

        foreach (var type in typeof(RepoDb.BaseRepository<,>).Assembly.GetTypes())
        {
            if (!type.IsPublic)
            {
                continue;
            }

            foreach (var func in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
            {
                var rt = func.ReturnType;

                rt = UnwrapTask(rt);

                if (rt == typeof(IEnumerable<DbField>))
                {
                    Console.WriteLine($"M: {type.FullName}.{func.Name}");
                }
            }

            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
            {
                var rt = prop.PropertyType;

                rt = UnwrapTask(rt);

                rt = UnwrapTask(rt);

                if (rt == typeof(IEnumerable<DbField>))
                {
                    Console.WriteLine($"P: {type.FullName}.{prop.Name}");
                }
            }

            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
            {
                var rt = field.FieldType;
                rt = UnwrapTask(rt);

                if (rt == typeof(IEnumerable<DbField>))
                {
                    Console.WriteLine($"F: {type.FullName}.{field.Name}");
                }
            }
        }
    }


    private static Type UnwrapTask(Type type)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() is { } gt && (gt == typeof(Task<>) || gt == typeof(ValueTask<>)))
        {
            return type.GetGenericArguments()[0];
        }
        if (type == typeof(Task) || type == typeof(ValueTask))
        {
            return typeof(void);
        }
        return type;
    }
}
