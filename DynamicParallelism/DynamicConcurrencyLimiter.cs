namespace DynamicParallelism;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// A concurrency limiter that can be adjusted at runtime.
/// Each AcquireAsync() call asynchronously waits for a permit.
/// Disposing the returned object releases that permit.
/// </summary>
public sealed class DynamicConcurrencyLimiter : IDisposable
{
    private volatile bool _disposed;          // Volatile to reduce some race conditions
    private readonly SemaphoreSlim _semaphore;
    private readonly object _lock = new object();

    // How many total permits (slots) we are *trying* to have
    private int _maxConcurrency;

    // How many permits still need to be "removed" to lower concurrency
    private int _pendingRemovals;

    /// <summary>
    /// Initializes a new instance of the <see cref="DynamicConcurrencyLimiter"/> class
    /// with the specified initial concurrency level.
    /// </summary>
    public DynamicConcurrencyLimiter(int initialMaxConcurrency)
    {
        if (initialMaxConcurrency < 0)
            throw new ArgumentOutOfRangeException(nameof(initialMaxConcurrency));

        _maxConcurrency = initialMaxConcurrency;
        _semaphore = new SemaphoreSlim(initialMaxConcurrency, int.MaxValue);
    }

    /// <summary>
    /// Current "target" concurrency level (number of permits).
    /// Note that if concurrency was recently decreased and all permits
    /// were in use, the *effective* concurrency may be higher until tasks finish.
    /// </summary>
    public int MaxConcurrency
    {
        get
        {
            lock (_lock)
            {
                return _maxConcurrency;
            }
        }
    }

    /// <summary>
    /// Increase or decrease the concurrency limit at runtime.
    /// </summary>
    public void UpdateMaxConcurrency(int newMaxConcurrency)
    {
        if (newMaxConcurrency < 0)
            throw new ArgumentOutOfRangeException(nameof(newMaxConcurrency));

        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DynamicConcurrencyLimiter));

            // Avoid potential overflow when adding to _pendingRemovals
            if (newMaxConcurrency > int.MaxValue - _pendingRemovals)
                throw new OverflowException("New concurrency value would cause overflow.");

            int diff = checked(newMaxConcurrency - _maxConcurrency);
            _maxConcurrency = newMaxConcurrency;

            if (diff > 0)
            {
                // Increase concurrency: release additional permits
                _semaphore.Release(diff);
            }
            else if (diff < 0)
            {
                // Decrease concurrency: we cannot forcibly yank permits
                // from tasks already running, so we accumulate "pendingRemovals".
                // This is effectively _pendingRemovals += -diff.
                _pendingRemovals = checked(_pendingRemovals - diff);
            }
        }
    }

    /// <summary>
    /// Acquire one permit (slot) asynchronously. Dispose the returned object to release the permit.
    /// Throws <see cref="ObjectDisposedException"/> if the limiter has already been disposed.
    /// </summary>
    public async Task<IDisposable> AcquireAsync(CancellationToken cancellationToken = default)
    {
        // First check outside the lock (lightweight). If it’s disposed, fail fast.
        if (_disposed)
            throw new ObjectDisposedException(nameof(DynamicConcurrencyLimiter));

        try
        {
            // Attempt to acquire a semaphore permit.
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            // Double-check after acquiring in case someone disposed during the wait.
            if (_disposed)
            {
                // We did actually acquire a permit, so release it immediately.
                _semaphore.Release();
                throw new ObjectDisposedException(nameof(DynamicConcurrencyLimiter));
            }

            return new Releaser(this);
        }
        catch (OperationCanceledException)
        {
            // If WaitAsync is canceled, typically no permit has actually been granted by the semaphore.
            // So there's no permit to release, but just re-throw the cancellation.
            throw;
        }
    }

    /// <summary>
    /// Internal helper to release or swallow permits, depending on whether we have pending removals.
    /// </summary>
    private void Release()
    {
        lock (_lock)
        {
            // If the limiter is disposed, do nothing: no need to put the permit back.
            if (_disposed)
                return;

            if (_pendingRemovals > 0)
            {
                // We wanted to lower concurrency, so we "swallow" this release
                // instead of returning it to the SemaphoreSlim pool.
                _pendingRemovals--;
            }
            else
            {
                _semaphore.Release();
            }
        }
    }

    /// <summary>
    /// Disposes the limiter. Any future calls to AcquireAsync() will throw.
    /// Existing holders can still call Dispose on the acquired permit,
    /// but it will just no-op if the limiter is already disposed.
    /// </summary>
    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
        }

        // Dispose the semaphore.
        _semaphore.Dispose();
    }

    /// <summary>
    /// Disposable "lease" that calls back to the parent limiter upon Dispose.
    /// </summary>
    private sealed class Releaser : IDisposable
    {
        private DynamicConcurrencyLimiter? _owner;
        private bool _disposed;

        public Releaser(DynamicConcurrencyLimiter owner)
        {
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            // Release permit back to the owner (unless it was disposed).
            _owner?.Release();
            _owner = null;
        }
    }
}
