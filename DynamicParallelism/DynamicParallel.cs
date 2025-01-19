namespace DynamicParallelism;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Provides a dynamic parallelization method similar to Parallel.ForEachAsync,
/// but uses a <see cref="DynamicConcurrencyLimiter"/> to allow changing
/// concurrency on the fly.
/// </summary>
public static class DynamicParallel
{
    /// <summary>
    /// Executes an asynchronous for-each operation over <paramref name="source"/>,
    /// honoring a <see cref="DynamicConcurrencyLimiter"/> so that the degree of
    /// parallelism can be updated at runtime.
    /// </summary>
    /// <typeparam name="TSource">Type of items in the source.</typeparam>
    /// <param name="source">An <see cref="IEnumerable{TSource}"/> sequence to process.</param>
    /// <param name="concurrencyLimiter">
    /// The <see cref="DynamicConcurrencyLimiter"/> controlling concurrency. You can call
    /// <see cref="DynamicConcurrencyLimiter.UpdateMaxConcurrency(int)"/> at any time to adjust concurrency.
    /// </param>
    /// <param name="body">
    /// An asynchronous delegate invoked once per element in <paramref name="source"/>.
    /// </param>
    /// <param name="cancellationToken">
    /// A cancellation token that may be used to cancel the overall operation.
    /// </param>
    /// <returns>A task that represents the entire operation.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown if <paramref name="source"/>, <paramref name="concurrencyLimiter"/>, or <paramref name="body"/> is null.
    /// </exception>
    /// <exception cref="ObjectDisposedException">
    /// Thrown if <paramref name="concurrencyLimiter"/> has already been disposed.
    /// </exception>
    public static Task ForEachAsync<TSource>(
        IEnumerable<TSource>         source,
        DynamicConcurrencyLimiter    concurrencyLimiter,
        Func<TSource, CancellationToken, ValueTask> body,
        CancellationToken            cancellationToken = default)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (concurrencyLimiter == null) throw new ArgumentNullException(nameof(concurrencyLimiter));
        if (body == null) throw new ArgumentNullException(nameof(body));

        // If the user has already signaled cancellation, bail out immediately.
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        // Initialize the shared state object.
        DynamicForEachAsyncState<TSource> state = new(
            source,
            concurrencyLimiter,
            body,
            cancellationToken);

        // Start the first worker. More workers may be spawned automatically
        // if there are more items to process (see the workerBody loop).
        state.QueueNextWorker();

        // Return the task that represents the entire operation.
        return state.Task;
    }

    /// <summary>
    /// Encapsulates all shared state for the dynamic ForEachAsync operation:
    /// - The shared enumerator
    /// - The concurrency limiter
    /// - A task completion source to signal overall completion
    /// - Exceptions tracking
    /// - The number of workers currently running
    /// </summary>
    private sealed class DynamicForEachAsyncState<TSource> : TaskCompletionSource
    {
        private readonly object _lockObj = new object(); // Protects enumerator and exception list
        private readonly IEnumerator<TSource> _enumerator;
        private readonly DynamicConcurrencyLimiter _concurrencyLimiter;
        private readonly Func<TSource, CancellationToken, ValueTask> _body;

        private readonly CancellationTokenSource _internalCancellationSource;
        private readonly CancellationTokenRegistration _externalReg;

        // Keep track of how many active workers are running.
        // When this hits 0, we know the entire operation has finished
        // (either by exhausting the source or by cancellation/error).
        private int _completionRefCount;

        // We'll collect any exceptions that occur. We fail the entire
        // operation with the first or aggregated set of exceptions we see.
        private List<Exception>? _exceptions;

        public DynamicForEachAsyncState(
            IEnumerable<TSource> source,
            DynamicConcurrencyLimiter concurrencyLimiter,
            Func<TSource, CancellationToken, ValueTask> body,
            CancellationToken externalCancellationToken)
        {
            try
            {
                _enumerator = source.GetEnumerator();
            }
            catch (Exception e)
            {
                // If enumerator creation fails, fail immediately.
                // This can happen if the IEnumerable throws from GetEnumerator().
                SetException(e);
                throw;
            }

            _concurrencyLimiter = concurrencyLimiter;
            _body = body;

            _internalCancellationSource = new CancellationTokenSource();
            // Link the external token to our internal token so that if the user signals
            // cancellation, we cancel our internal operations too.
            _externalReg = externalCancellationToken.Register(
                static s => ((DynamicForEachAsyncState<TSource>)s!).CancelOperation(),
                this);
        }

        /// <summary>
        /// Queues up the next worker (task) -- if there's more data to process and
        /// we haven't triggered cancellation yet.
        ///
        /// This method unconditionally queues one more worker. The concurrency limiter
        /// itself will throttle how many can run in parallel (via AcquireAsync).
        ///
        /// We keep track of the total number of active workers via <see cref="_completionRefCount"/>.
        /// The new worker won't actually do any meaningful work until it successfully
        /// acquires a permit from <see cref="_concurrencyLimiter"/>.
        /// </summary>
        public void QueueNextWorker()
        {
            Interlocked.Increment(ref _completionRefCount);

            // Queue the worker to the ThreadPool. We do not pass a CancellationToken
            // to Task.Run, because we *always* want this worker to run so that it can
            // properly decrement the worker count and handle completion logic.
            _ = Task.Run(WorkerBody);
        }

        /// <summary>
        /// The main loop for each worker. Each worker:
        /// 1. Acquires a concurrency slot (permit).
        /// 2. Tries to grab the next element from the enumerator.
        /// 3. If an element exists, processes it, then spawns the "next" worker.
        /// 4. If enumerator is exhausted (or canceled), it breaks.
        /// 5. Releases the concurrency slot.
        /// </summary>
        private async Task WorkerBody()
        {
            bool launchedNext = false;
            try
            {
                while (true)
                {
                    // First check if we've already been canceled/failed.
                    if (_internalCancellationSource.IsCancellationRequested)
                    {
                        break;
                    }

                    // Acquire a concurrency slot from the limiter. This may block
                    // if we've reached the current concurrency limit.
                    IDisposable? permit;
                    try
                    {
                        permit = await _concurrencyLimiter
                            .AcquireAsync(_internalCancellationSource.Token)
                            .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        // If cancellation is triggered while waiting for the slot,
                        // we exit the loop.
                        break;
                    }

                    TSource current = default!;
                    bool hasNext;
                    // Lock around enumerator access
                    lock (_lockObj)
                    {
                        if (_internalCancellationSource.IsCancellationRequested)
                        {
                            // We were canceled while waiting for the lock. We can stop now.
                            hasNext = false;
                        }
                        else
                        {
                            hasNext = _enumerator.MoveNext();
                            if (hasNext)
                            {
                                current = _enumerator.Current;
                            }
                            else
                            {
                                current = default!; // won't be used
                            }
                        }
                    }

                    if (!hasNext)
                    {
                        // No more items. We're done.  Release the permit
                        // and exit the loop.
                        permit.Dispose();
                        break;
                    }

                    // If we got an item and haven't spawned the next worker yet,
                    // do so exactly once. That next worker will eventually
                    // do the same, thus forming a "chain" of workers.
                    if (!launchedNext)
                    {
                        launchedNext = true;
                        QueueNextWorker();
                    }

                    // Now actually process the item. We do this outside the lock
                    // so other workers can advance the enumerator concurrently.
                    try
                    {
                        await _body(current, _internalCancellationSource.Token).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        // Record the exception and trigger cancellation so that
                        // all other workers stop ASAP.
                        RecordException(ex);
                        CancelOperation();
                    }
                    finally
                    {
                        // Important to release the concurrency slot after
                        // we finish processing the item.
                        permit.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                // If an unhandled exception escapes our loop, record it and trigger cancellation.
                RecordException(ex);
                CancelOperation();
            }
            finally
            {
                // If we're the last worker to complete, that means everything is done
                // (enumerator is exhausted or canceled). Complete the operation.
                if (Interlocked.Decrement(ref _completionRefCount) == 0)
                {
                    Complete();
                }
            }
        }

        /// <summary>
        /// Cancels the entire operation (both internal and external).
        /// </summary>
        private void CancelOperation()
        {
            try
            {
                _internalCancellationSource.Cancel();
            }
            catch
            {
                // We generally don't expect an exception from Cancel().
                // Even if we do, we can just eat it.
            }
        }

        /// <summary>
        /// Records an exception into our shared list and will be used for the final
        /// exception of the whole operation.
        /// </summary>
        private void RecordException(Exception e)
        {
            lock (_lockObj)
            {
                (_exceptions ??= new List<Exception>()).Add(e);
            }
        }

        /// <summary>
        /// Completes the operation. We decide if it's a success or failure
        /// based on whether we have exceptions or cancellation.
        /// </summary>
        private void Complete()
        {
            // We dispose the external reg so that we don't keep
            // receiving notifications.
            _externalReg.Dispose();

            // Clean up the enumerator
            lock (_lockObj)
            {
                _enumerator.Dispose();
            }
            _internalCancellationSource.Dispose();

            // If the external token was canceled, prefer that as the result
            // (so the final task can show IsCanceled = true).
            // Otherwise, if we collected any exceptions, fail with them.
            // Otherwise, we succeeded normally.
            if (_exceptions == null)
            {
                // Check if the external token is canceled
                // or if we canceled internally due to an exception in another worker.
                if (TrySetCanceledIfNeeded())
                {
                    return;
                }

                // Everything completed with no exceptions and no external cancellation.
                SetResult();
            }
            else
            {
                // One or more errors occurred.
                TrySetException(_exceptions);
            }
        }

        private bool TrySetCanceledIfNeeded()
        {
            // If the external token is canceled, we propagate that.
            // If not, we check the internal token.
            // This ensures that if the user requested cancellation,
            // the final Task sees TaskStatus.Canceled.
            if (_externalReg.Token.IsCancellationRequested)
            {
                return TrySetCanceled(_externalReg.Token);
            }
            else if (_internalCancellationSource.IsCancellationRequested)
            {
                // We must pick some token to indicate cancellation.
                // It's often best to pick the internal token (or a new token).
                return TrySetCanceled(_internalCancellationSource.Token);
            }
            return false;
        }
    }
}
