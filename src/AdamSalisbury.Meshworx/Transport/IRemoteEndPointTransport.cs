using System.Net;

namespace AdamSalisbury.Meshworx.Transport;

/// <summary>
/// An optional capability a transport implements when it is connected over a network and can report
/// the remote peer's address. <see cref="MeshHub"/> uses it to cap how many connections it accepts
/// from the same remote address at once, guarding against a single source opening connections faster
/// than they can be evicted.
/// </summary>
/// <remarks>
/// Not part of <see cref="ITransport"/> itself because not every transport has a meaningful network
/// address to report — an in-process transport, for example, has none. A transport that does not
/// implement this is simply never subject to the per-remote-endpoint connection cap.
/// </remarks>
public interface IRemoteEndPointTransport
{
    /// <summary>
    /// Gets the remote network endpoint this transport is connected to, or <see langword="null"/> if
    /// it has none or is not currently known.
    /// </summary>
    EndPoint? RemoteEndPoint { get; }
}
