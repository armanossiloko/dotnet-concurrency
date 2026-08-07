using System.Collections.Concurrent;

namespace DotnetConcurrency.Concurrency;

/// <summary>
/// Coordinates writes to one Instance inside this process.
/// A controller claim prevents new workers from starting on that key and cancels
/// the worker, if any, which currently owns the lease. Reads do not use it.
/// </summary>
public sealed class InstanceGate
{
    private readonly ConcurrentDictionary<Guid, KeyState> _keys = new();

    /// <summary>
    /// Acquires exclusive access for a pending Instance update. Controllers wait
    /// and claim priority; workers wait only briefly and receive null when the
    /// instance should be requeued.
    /// </summary>
    public Task<UpdateLease?> AcquireForUpdateAsync(
        Guid instanceId,
        UpdateRequester requester,
        CancellationToken ct,
        TimeSpan? workerTimeout = null) =>
        requester switch
        {
            UpdateRequester.Controller => AcquireControllerUpdateAsync(instanceId, ct),
            UpdateRequester.Worker => TryAcquireWorkerUpdateAsync(
                instanceId,
                workerTimeout ?? TimeSpan.FromMilliseconds(50),
                ct),
            _ => throw new ArgumentOutOfRangeException(nameof(requester), requester, null)
        };

    private async Task<UpdateLease?> AcquireControllerUpdateAsync(
        Guid instanceId,
        CancellationToken ct)
    {
        var key = GetKey(instanceId);

        lock (key.Sync)
        {
            key.UserClaims++;
            key.ActiveWorker?.Cancel();
        }

        try
        {
            await key.Mutex.WaitAsync(ct).ConfigureAwait(false);
            return new UpdateLease(key, workerCancellation: null);
        }
        catch
        {
            ReleaseUserClaim(key);
            throw;
        }
    }

    /// <summary>
    /// Returns null when another writer holds the key or a user has claimed it.
    /// A user claim is checked both before and after waiting, so a worker cannot
    /// slip in ahead of a newly-arrived controller request.
    /// </summary>
    private async Task<UpdateLease?> TryAcquireWorkerUpdateAsync(
        Guid instanceId,
        TimeSpan timeout,
        CancellationToken ct)
    {
        var key = GetKey(instanceId);

        lock (key.Sync)
        {
            if (key.UserClaims > 0)
            {
                return null;
            }
        }

        if (!await key.Mutex.WaitAsync(timeout, ct).ConfigureAwait(false))
        {
            return null;
        }

        lock (key.Sync)
        {
            if (key.UserClaims > 0)
            {
                key.Mutex.Release();
                return null;
            }

            var workerCancellation = new CancellationTokenSource();
            key.ActiveWorker = workerCancellation;
            return new UpdateLease(key, workerCancellation);
        }
    }

    private KeyState GetKey(Guid instanceId) =>
        _keys.GetOrAdd(instanceId, _ => new KeyState());

    private static void ReleaseUserClaim(KeyState key)
    {
        lock (key.Sync)
        {
            key.UserClaims--;
        }
    }

    internal sealed class KeyState
    {
        public object Sync { get; } = new();
        public SemaphoreSlim Mutex { get; } = new(1, 1);
        public int UserClaims { get; set; }
        public CancellationTokenSource? ActiveWorker { get; set; }
    }

    /// <summary>
    /// Represents exclusive access to an Instance for an update. Worker leases
    /// carry a cancellation token that is cancelled when a controller claims
    /// priority for the same Instance.
    /// </summary>
    public sealed class UpdateLease : IAsyncDisposable
    {
        private readonly KeyState _key;
        private readonly CancellationTokenSource? _workerCancellation;
        private int _disposed;

        internal UpdateLease(KeyState key, CancellationTokenSource? workerCancellation)
        {
            _key = key;
            _workerCancellation = workerCancellation;
        }

        public CancellationToken CancellationToken =>
            _workerCancellation?.Token ?? CancellationToken.None;

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return ValueTask.CompletedTask;
            }

            if (_workerCancellation is null)
            {
                _key.Mutex.Release();
                ReleaseUserClaim(_key);
                return ValueTask.CompletedTask;
            }

            lock (_key.Sync)
            {
                if (ReferenceEquals(_key.ActiveWorker, _workerCancellation))
                {
                    _key.ActiveWorker = null;
                }
            }

            _workerCancellation.Dispose();
            _key.Mutex.Release();
            return ValueTask.CompletedTask;
        }
    }
}

public enum UpdateRequester
{
    Controller,
    Worker
}
