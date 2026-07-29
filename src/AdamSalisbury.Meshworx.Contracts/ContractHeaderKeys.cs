namespace AdamSalisbury.Meshworx.Contracts;

/// <summary>
/// The well-known <see cref="Messages.MessageHeaders"/> key that names which contract method a message
/// is calling.
/// </summary>
/// <remarks>
/// Public, like the codec layer's content-type key and for the same reason: a contract is an agreement
/// between two endpoints, and both the generated proxy and the generated dispatcher must be able to
/// name the key. The hub neither reads nor writes it — a contract call is an ordinary message with an
/// ordinary header block, and the hub routes it without knowing a contract exists.
/// </remarks>
public static class ContractHeaderKeys
{
    /// <summary>
    /// The header key whose value is the name of the contract method being invoked.
    /// </summary>
    /// <remarks>
    /// The method's own name, unqualified. That is unambiguous because the generator refuses to emit a
    /// contract containing overloaded method names — two methods sharing a name would share a header
    /// value, and the dispatcher would have no way to tell which one the sender meant. Reporting that
    /// at build time is the honest alternative to picking one at run time and being wrong half the
    /// time.
    /// </remarks>
    public const string Method = "mesh.contract.method";
}
