using System.Collections.Concurrent;

namespace AdamSalisbury.Meshworx.Backplane;

/// <summary>
/// A process-local <see cref="IHubBackplane"/> — every hub instance sharing one is expected to live in
/// this same process. Genuinely useful for two things: exercising scale-out behaviour end to end in a
/// test without a real external store, and demonstrating or developing against the backplane seam before
/// standing up a real one.
/// </summary>
/// <remarks>
/// Thread-safe. Every operation is synchronous under the hood and wrapped in an already-completed
/// <see cref="Task"/> — there is no I/O to actually await, unlike a real cross-process implementation
/// (Redis and friends).
/// </remarks>
public sealed class InMemoryHubBackplane : IHubBackplane
{
    private readonly ConcurrentDictionary<string, Guid> _directory = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Guid, Func<BackplaneMessage, CancellationToken, Task>> _subscribers = new();
    private int _disposed;

    /// <inheritdoc/>
    public Task StartAsync(
        Guid instanceId,
        Func<BackplaneMessage, CancellationToken, Task> onMessage,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentNullException.ThrowIfNull(onMessage);

        if (!_subscribers.TryAdd(instanceId, onMessage))
        {
            throw new InvalidOperationException($"Instance {instanceId} is already started on this backplane.");
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task StopAsync(Guid instanceId, CancellationToken cancellationToken = default)
    {
        _subscribers.TryRemove(instanceId, out _);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task PublishAsync(BackplaneMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        // A snapshot rather than iterating the live dictionary: a concurrent StartAsync/StopAsync must
        // not throw "collection was modified" out of an unrelated publish, and a subscriber that stops
        // mid-publish is safe either way — it either receives this one last message or does not, never a
        // torn one.
        foreach (Func<BackplaneMessage, CancellationToken, Task> subscriber in _subscribers.Values)
        {
            try
            {
                await subscriber(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // One subscriber's handler throwing must not stop the message reaching every other one —
                // a real bus (Redis included) delivers to each subscriber independently, and a fake
                // standing in for one has to preserve that property for a test to be meaningful. Callback
                // boundary; this type has no logger of its own to record it with.
            }
        }
    }

    /// <inheritdoc/>
    public Task RegisterClientAsync(string clientName, Guid clientId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientName);
        _directory[clientName] = clientId;
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task UnregisterClientAsync(string clientName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientName);
        _directory.TryRemove(clientName, out _);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<Guid?> TryResolveClientAsync(string clientName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientName);
        return Task.FromResult(_directory.TryGetValue(clientName, out Guid id) ? (Guid?)id : null);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _subscribers.Clear();
            _directory.Clear();
        }

        return ValueTask.CompletedTask;
    }
}
