using System.Collections.Concurrent;

namespace DotnetConcurrency.Concurrency;

/// <summary>
/// Coordinates writes to one Instance inside this process.
/// A user claim prevents new workers from starting on that key and cancels
/// the worker, if any, which currently owns the lease. Reads do not use it.
/// </summary>
public sealed class InstanceGate
{
    private readonly ConcurrentDictionary<Guid, KeyState> _keys = new();

    public async Task<IAsyncDisposable> AcquireForUserAsync(Guid instanceId, CancellationToken ct)
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
            return new UserLease(key);
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
    public async Task<WorkerLease?> TryAcquireForWorkerAsync(
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
            return new WorkerLease(key, workerCancellation);
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

    public sealed class WorkerLease : IAsyncDisposable
    {
        private readonly KeyState _key;
        private readonly CancellationTokenSource _cancellation;
        private int _disposed;

        internal WorkerLease(KeyState key, CancellationTokenSource cancellation)
        {
            _key = key;
            _cancellation = cancellation;
        }

        public CancellationToken CancellationToken => _cancellation.Token;

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return ValueTask.CompletedTask;
            }

            lock (_key.Sync)
            {
                if (ReferenceEquals(_key.ActiveWorker, _cancellation))
                {
                    _key.ActiveWorker = null;
                }
            }

            _cancellation.Dispose();
            _key.Mutex.Release();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class UserLease(KeyState key) : IAsyncDisposable
    {
        private int _disposed;

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                key.Mutex.Release();
                ReleaseUserClaim(key);
            }

            return ValueTask.CompletedTask;
        }
    }
}
