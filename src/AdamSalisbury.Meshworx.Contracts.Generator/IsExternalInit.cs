namespace System.Runtime.CompilerServices;

/// <summary>
/// Polyfill for the marker type the compiler requires to emit an <c>init</c> accessor, which records
/// use for every positional property.
/// </summary>
/// <remarks>
/// .NET Standard 2.0 predates it, and a source generator must target .NET Standard 2.0 because that is
/// what the compiler loads analyzers into. Declaring it here is the standard workaround and costs
/// nothing at run time: the compiler only looks the type up by name.
/// </remarks>
internal static class IsExternalInit
{
}
