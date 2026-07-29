namespace AdamSalisbury.Meshworx.Contracts;

/// <summary>
/// Marks an interface as a Meshworx messaging contract, from which a client proxy and a dispatcher are
/// generated at compile time.
/// </summary>
/// <remarks>
/// The generator emits two types alongside the interface, in the same namespace:
/// <list type="bullet">
/// <item>
/// <description>
/// <c>{Name}Proxy</c>, implementing the interface. Calling a method serializes its arguments and sends
/// them to a fixed recipient.
/// </description>
/// </item>
/// <item>
/// <description>
/// <c>{Name}Dispatcher</c>, which takes an implementation of the interface, decodes an inbound message
/// and invokes the matching method on it.
/// </description>
/// </item>
/// </list>
/// <para>
/// Neither uses reflection: the generator has the interface's shape at compile time, so argument
/// packing and method selection are ordinary compiled code. A mistyped call is a build error rather
/// than a message that silently fails to dispatch at run time, which is the whole point of the layer.
/// </para>
/// <para>
/// Contract methods must return <see cref="Task"/> or <see cref="Task{TResult}"/> and may take an
/// optional trailing <see cref="CancellationToken"/>. Anything the generator cannot express — a
/// non-task return, a <see langword="ref"/> or <see langword="out"/> parameter, a generic method, a
/// property, an event, or an overloaded name — is reported as a build diagnostic rather than silently
/// skipped.
/// </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Interface)]
public sealed class MeshContractAttribute : Attribute
{
}
