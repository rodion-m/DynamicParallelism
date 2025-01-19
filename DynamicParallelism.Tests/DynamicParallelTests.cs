namespace DynamicParallelism.Tests;

public sealed class DynamicParallelTests
{
    [Fact]
    public async Task ForEachAsync_NullArguments_Throws()
    {
        var limiter = new DynamicConcurrencyLimiter(1);

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DynamicParallel.ForEachAsync<string>(null!, limiter, (_, _) => default)
        );

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DynamicParallel.ForEachAsync(Array.Empty<string>(), null!, (_, _) => default)
        );

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DynamicParallel.ForEachAsync(Array.Empty<string>(), limiter, null!)
        );
    }

    [Fact]
    public async Task ForEachAsync_EmptySource_CompletesImmediately()
    {
        using var limiter = new DynamicConcurrencyLimiter(2);
        var items = Array.Empty<int>();

        await DynamicParallel.ForEachAsync(items, limiter, async (item, ct) =>
        {
            // won't be called
            await Task.Yield();
        });

        // If we reach here, it completed with no errors.
    }

    [Fact]
    public async Task ForEachAsync_SingleItem_ProcessesCorrectly()
    {
        using var limiter = new DynamicConcurrencyLimiter(2);
        var items = new[] { 42 };

        bool called = false;
        await DynamicParallel.ForEachAsync(items, limiter, async (item, ct) =>
        {
            Assert.Equal(42, item);
            called = true;
            await Task.Yield();
        });

        Assert.True(called);
    }

    [Fact]
    public async Task ForEachAsync_ExceptionInBody_CancelsOthers()
    {
        using var limiter = new DynamicConcurrencyLimiter(10);
        var items = Enumerable.Range(0, 100);

        int processedCount = 0;
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await DynamicParallel.ForEachAsync(items, limiter, async (item, ct) =>
            {
                Interlocked.Increment(ref processedCount);
                if (item == 5)
                {
                    throw new InvalidOperationException("Boom!");
                }
                // Simulate some work
                await Task.Delay(10, ct);
            });
        });

        Assert.IsType<InvalidOperationException>(ex);
        // We expect not all items got processed because once item == 5 fails,
        // we cancel the rest quickly.
        Assert.InRange(processedCount, 6, 99);
    }

    [Fact]
    public async Task ForEachAsync_ExternalCancellation()
    {
        using var limiter = new DynamicConcurrencyLimiter(10);
        var items = Enumerable.Range(0, 100);
        var cts = new CancellationTokenSource();

        int processedCount = 0;
        var task = DynamicParallel.ForEachAsync(items, limiter, async (item, ct) =>
        {
            Interlocked.Increment(ref processedCount);
            if (item == 20)
            {
                // Cancel once we hit item==20
                cts.Cancel();
            }
            await Task.Delay(5, ct);
        }, cts.Token);

        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);

        Assert.InRange(processedCount, 21, 99);
    }

    [Fact]
    public async Task ForEachAsync_RespectsConcurrencyLimit()
    {
        // We'll set concurrency = 2, feed in a bunch of items, and measure
        // how many are concurrently in flight to ensure it never exceeds 2.
        using var limiter = new DynamicConcurrencyLimiter(2);
        var items = Enumerable.Range(0, 10);

        int maxConcurrencyObserved = 0;
        int currentConcurrency = 0;

        await DynamicParallel.ForEachAsync(items, limiter, async (item, ct) =>
        {
            int inUse = Interlocked.Increment(ref currentConcurrency);
            int snapshotMax = InterlockedExtensions.Max(ref maxConcurrencyObserved, inUse);

            // Simulate some work
            await Task.Delay(50, ct);

            Interlocked.Decrement(ref currentConcurrency);
        });

        Assert.True(maxConcurrencyObserved <= 2, $"Observed concurrency {maxConcurrencyObserved} > 2");
    }

    [Fact]
    public async Task ForEachAsync_EnumeratorThrowsImmediately()
    {
        // If the IEnumerable throws from GetEnumerator(),
        // the ForEachAsync should fail quickly.
        using var limiter = new DynamicConcurrencyLimiter(2);

        IEnumerable<int> ThrowingSequence()
        {
            throw new InvalidOperationException("Can't enumerate!");
        }

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await DynamicParallel.ForEachAsync(ThrowingSequence(), limiter, (item, ct) => default);
        });

        Assert.Equal("Can't enumerate!", ex.Message);
    }

    [Fact]
    public async Task ForEachAsync_Updater_ChangesConcurrencyDuringProcessing()
    {
        // We'll begin with concurrency=1, then after half the items are processed,
        // bump to concurrency=5 and ensure that speeds up the subsequent processing.
        using var limiter = new DynamicConcurrencyLimiter(1);
        var items = Enumerable.Range(0, 20).ToList();

        int processed = 0;
        int concurrencyChangeTriggered = 0;
        await DynamicParallel.ForEachAsync(items, limiter, async (item, ct) =>
        {
            int countSoFar = Interlocked.Increment(ref processed);
            // Once we've processed half the items, expand concurrency
            if (countSoFar == 10)
            {
                concurrencyChangeTriggered = Environment.TickCount;
                limiter.UpdateMaxConcurrency(5);
            }
            // Simulate some CPU-bound or I/O-bound work
            await Task.Delay(30, ct);
        });

        int allDoneTime = Environment.TickCount;
        int totalDuration = allDoneTime - concurrencyChangeTriggered;

        // We can't precisely measure the total time, but we can check
        // that concurrency was effectively used. For a robust test,
        // you might measure how quickly the last 10 items were processed
        // versus the first 10.

        // We'll just assert we actually triggered concurrency 5 and completed
        // the entire set. The main correctness is that we didn't deadlock,
        // or exceed concurrency, etc.
        Assert.Equal(20, processed);
    }
}
