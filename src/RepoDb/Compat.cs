
#if !NET
namespace System.Runtime.CompilerServices
{

// Required to allow init properties in netstandard
internal sealed class IsExternalInit : Attribute
{
}
}

namespace System
{

public static class CompatExtensions
{
    public static bool StartsWith(this string v, char value)
    {
        return v.StartsWith(value.ToString());
    }
}

}
#endif
