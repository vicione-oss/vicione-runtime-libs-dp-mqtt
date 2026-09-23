using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// Resolves the type a received message names in its <c>Type</c> user property. The name comes off
/// the broker, so it is looked up only among the assemblies already loaded — in the port's load
/// context, then in the default one the framework lives in — and never makes one load. Every name is
/// resolved once; past <see cref="Capacity"/> distinct names the rest are resolved each time, which
/// still loads nothing.
/// </summary>
internal sealed class NamedTypeResolver(AssemblyLoadContext loadContext)
{
    internal const int Capacity = 256;

    private readonly ConcurrentDictionary<string, Type?> _types = new(StringComparer.Ordinal);

    internal Type? Resolve(string assemblyQualifiedName)
    {
        if (_types.TryGetValue(assemblyQualifiedName, out var type))
            return type;

        type = Find(assemblyQualifiedName);

        if (_types.Count < Capacity)
            _types.TryAdd(assemblyQualifiedName, type);

        return type;
    }

    private Type? Find(string assemblyQualifiedName)
    {
        try
        {
            return Type.GetType(assemblyQualifiedName, FindLoadedAssembly, typeResolver: null, throwOnError: false);
        }
        catch (Exception ex) when (ex is TypeLoadException or IOException or BadImageFormatException or ArgumentException)
        {
            return null;
        }
    }

    private Assembly? FindLoadedAssembly(AssemblyName name)
        => FindLoadedAssembly(loadContext, name) ?? FindLoadedAssembly(AssemblyLoadContext.Default, name);

    private static Assembly? FindLoadedAssembly(AssemblyLoadContext context, AssemblyName name)
        => context.Assemblies.FirstOrDefault(a => AssemblyName.ReferenceMatchesDefinition(name, a.GetName()));
}
