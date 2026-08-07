using System.Collections.Concurrent;

namespace DotnetConcurrency.Concurrency;

/// <summary>
/// Coordinates writes to one Instance inside this process.
/// A controller claim prevents new workers from starting on that key. A worker
/// which already owns the lease always finishes normally. Reads do not use it.
/// </summary>
public sealed class InstanceGate
{
    private readonly ConcurrentDictionary<Guid, KeyState> _keys = new();

    internal int TrackedInstanceCount => _keys.Count;

    /// <summary>
    /// Claims controller priority and waits for exclusive update access. An
    /// active background update is never cancelled.
    /// </summary>
    public async Task<UpdateLease> AcquireForUpdateAsync(
        Guid instanceId,
        CancellationToken ct)
    {
        var key = RentKey(instanceId);

        lock (key.Sync)
        {
            key.PriorityClaims++;
        }

        try
        {
            await key.Mutex.WaitAsync(ct).ConfigureAwait(false);
            return new UpdateLease(this, instanceId, key, isPriority: true);
        }
        catch
        {
            ReleasePriorityClaim(key);
            ReleaseKey(instanceId, key);
            throw;
        }
    }

    /// <summary>
    /// Attempts to acquire exclusive update access for background work. Returns
    /// null when the instance is busy or a priority update is pending.
    /// </summary>
    public async Task<UpdateLease?> TryAcquireForUpdateAsync(
        Guid instanceId,
        TimeSpan timeout,
        CancellationToken ct)
    {
        var key = RentKey(instanceId);

        lock (key.Sync)
        {
            if (key.PriorityClaims > 0)
            {
                ReleaseKey(instanceId, key);
                return null;
            }
        }

        bool acquired;
        try
        {
            acquired = await key.Mutex.WaitAsync(timeout, ct).ConfigureAwait(false);
        }
        catch
        {
            ReleaseKey(instanceId, key);
            throw;
        }

        if (!acquired)
        {
            ReleaseKey(instanceId, key);
            return null;
        }

        lock (key.Sync)
        {
            if (key.PriorityClaims > 0)
            {
                key.Mutex.Release();
                ReleaseKey(instanceId, key);
                return null;
            }

            return new UpdateLease(this, instanceId, key, isPriority: false);
        }
    }

    private KeyState RentKey(Guid instanceId)
    {
        while (true)
        {
            var key = _keys.GetOrAdd(instanceId, static _ => new KeyState());

            lock (key.Sync)
            {
                if (key.Removing)
                {
                    continue;
                }

                key.References++;
                return key;
            }
        }
    }

    private void ReleaseKey(Guid instanceId, KeyState key)
    {
        var remove = false;

        lock (key.Sync)
        {
            key.References--;
            if (key.References == 0)
            {
                key.Removing = true;
                remove = true;
            }
        }

        if (remove)
        {
            _keys.TryRemove(instanceId, out _);
            key.Mutex.Dispose();
        }
    }

    private static void ReleasePriorityClaim(KeyState key)
    {
        lock (key.Sync)
        {
            key.PriorityClaims--;
        }
    }

    internal sealed class KeyState
    {
        public object Sync { get; } = new();
        public SemaphoreSlim Mutex { get; } = new(1, 1);
        public int PriorityClaims { get; set; }
        public int References { get; set; }
        public bool Removing { get; set; }
    }

    /// <summary>
    /// Represents exclusive access to an Instance for an update.
    /// </summary>
    public sealed class UpdateLease : IAsyncDisposable
    {
        private readonly InstanceGate _owner;
        private readonly Guid _instanceId;
        private readonly KeyState _key;
        private readonly bool _isPriority;
        private int _disposed;

        internal UpdateLease(
            InstanceGate owner,
            Guid instanceId,
            KeyState key,
            bool isPriority)
        {
            _owner = owner;
            _instanceId = instanceId;
            _key = key;
            _isPriority = isPriority;
        }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return ValueTask.CompletedTask;
            }

            if (_isPriority)
            {
                ReleasePriorityClaim(_key);
            }

            _key.Mutex.Release();
            _owner.ReleaseKey(_instanceId, _key);
            return ValueTask.CompletedTask;
        }
    }
}
