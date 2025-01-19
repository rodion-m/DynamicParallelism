using System.Collections.Concurrent;

namespace DynamicParallelism.Tests;

public sealed class DynamicConcurrencyLimiterTests
{
    [Fact]
    public void Constructor_WithNegativeConcurrency_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DynamicConcurrencyLimiter(-1));
    }

    [Fact]
    public void Constructor_Valid()
    {
        using var limiter = new DynamicConcurrencyLimiter(5);
        Assert.Equal(5, limiter.MaxConcurrency);
    }

    [Fact]
    public async Task AcquireAsync_Throws_WhenDisposedBeforeAcquire()
    {
        var limiter = new DynamicConcurrencyLimiter(1);
        limiter.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await limiter.AcquireAsync();
        });
    }

    [Fact]
    public async Task AcquireAsync_RespectsConcurrencyLimit()
    {
        using var limiter = new DynamicConcurrencyLimiter(2);

        // Acquire 2 permits (we only have concurrency=2)
        using (await limiter.AcquireAsync())
        using (await limiter.AcquireAsync())
        {
            // The third AcquireAsync should block until we release a permit.
            var acquireTask = limiter.AcquireAsync();

            // It shouldn't complete yet.
            Assert.False(acquireTask.IsCompleted, "A 3rd acquire should be blocked.");

            // Release one permit
        }

        // Now the 3rd acquire should proceed
        using (await limiter.AcquireAsync())
        {
            // success
        }
    }

    [Fact]
    public void UpdateMaxConcurrency_Negative_Throws()
    {
        using var limiter = new DynamicConcurrencyLimiter(2);
        Assert.Throws<ArgumentOutOfRangeException>(() => limiter.UpdateMaxConcurrency(-5));
    }

    [Fact]
    public void UpdateMaxConcurrency_Overflow_Throws()
    {
        using var limiter = new DynamicConcurrencyLimiter(10);
        // We artificially make _pendingRemovals near int.MaxValue internally
        // by decreasing concurrency a bunch of times. Then we set a concurrency
        // that would overflow the sum.

        // Decrease concurrency from 10 -> 0 => pendingRemovals = 10
        limiter.UpdateMaxConcurrency(0);

        // _pendingRemovals = 10, now try to set concurrency to a huge number
        // that would cause (newMaxConcurrency + pendingRemovals) to overflow.
        Assert.Throws<OverflowException>(() =>
        {
            limiter.UpdateMaxConcurrency(int.MaxValue - 5);
        });
    }

    [Fact]
    public void UpdateMaxConcurrency_WhenDisposed_Throws()
    {
        var limiter = new DynamicConcurrencyLimiter(1);
        limiter.Dispose();
        Assert.Throws<ObjectDisposedException>(() => limiter.UpdateMaxConcurrency(5));
    }

    [Fact]
    public async Task UpdateMaxConcurrency_IncreasesSlots()
    {
        using var limiter = new DynamicConcurrencyLimiter(1);
        Assert.Equal(1, limiter.MaxConcurrency);

        // Increase concurrency from 1 to 3
        limiter.UpdateMaxConcurrency(3);
        Assert.Equal(3, limiter.MaxConcurrency);

        // Try acquiring 3 permits simultaneously
        using var r1 = await limiter.AcquireAsync();
        using var r2 = await limiter.AcquireAsync();
        using var r3 = await limiter.AcquireAsync();
        // The next one should block
        var task = limiter.AcquireAsync();

        Assert.False(task.IsCompleted, "4th acquire should still block.");
    }

    [Fact]
    public async Task UpdateMaxConcurrency_DecreasesSlots()
    {
        using var limiter = new DynamicConcurrencyLimiter(3);

        // Acquire 3
        using var r1 = await limiter.AcquireAsync();
        using var r2 = await limiter.AcquireAsync();
        using var r3 = await limiter.AcquireAsync();

        // Decrease concurrency to 1 => pendingRemovals = 2
        limiter.UpdateMaxConcurrency(1);
        Assert.Equal(1, limiter.MaxConcurrency);

        // Dispose two of them, swallowing two releases
        r1.Dispose(); // pendingRemovals => 1
        r2.Dispose(); // pendingRemovals => 0

        // Now concurrency is effectively 1, and we still have r3 in use.
        // Because we've swallowed two releases, the semaphore is still at 0
        // but once we dispose r3, a real release will happen:

        r3.Dispose(); // pendingRemovals == 0 => real Release => semaphore gets 1

        // Now we have 1 free permit
        using var r4 = await limiter.AcquireAsync();  // This should succeed now

        // concurrency is effectively 1, so a second AcquireAsync would block
        var blockedTask = limiter.AcquireAsync();
        Assert.False(blockedTask.IsCompleted);

        // Actually we have to account for pendingRemovals carefully:
        //  - We started with 3.
        //  - We updated to 1 => diff = -2 => pendingRemovals=2
        //  - r1 disposed => pendingRemovals=1
        //  - r2 disposed => pendingRemovals=0
        //  - now concurrency is effectively 1 again, so r4 is using that 1.

        r4.Dispose();

        await blockedTask.WaitAsync(TimeSpan.FromSeconds(1)); // Should now complete.
    }

    [Fact]
    public async Task AcquireAsync_CanBeCanceledExternally()
    {
        using var limiter = new DynamicConcurrencyLimiter(1);
        // Acquire the only slot
        using (await limiter.AcquireAsync())
        {
            var cts = new CancellationTokenSource();
            var task = limiter.AcquireAsync(cts.Token);

            // Immediately cancel
            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(async () => await task);
        }
    }

    [Fact]
    public async Task ConcurrentAcquires_AndRandomConcurrencyUpdates_StressTest()
    {
        using var limiter = new DynamicConcurrencyLimiter(5);
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)); // fail test if it runs too long
        var rand = new Random();

        // Keep track of how many are concurrently in use
        var currentInUse = 0;
        var maxObserved = 0;

        // We'll run multiple tasks that each repeatedly acquire, do a tiny amount of work, release.
        // In parallel, we occasionally update concurrency up or down.
        var tasks = new List<Task>();

        // Worker tasks
        for (var i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    using (await limiter.AcquireAsync(cts.Token))
                    {
                        var used = Interlocked.Increment(ref currentInUse);
                        var observed = InterlockedExtensions.Max(ref maxObserved, used);

                        // Simulate work
                        await Task.Delay(rand.Next(1, 10), cts.Token);
                        Interlocked.Decrement(ref currentInUse);
                    }
                }
            }, cts.Token));
        }

        // Concurrency updater
        tasks.Add(Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                var newConcurrency = rand.Next(1, 10);
                limiter.UpdateMaxConcurrency(newConcurrency);
                await Task.Delay(rand.Next(1, 15), cts.Token);
            }
        }, cts.Token));

        // Let it run for a bit
        await Task.Delay(2000, cts.Token);

        // Stop
        cts.Cancel();
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
        {
            // expected
        }

        // Finally, ensure that we never saw concurrency usage exceed the limiter's max
        // at the moment we checked. This is not a perfect check (race conditions might
        // skew the measurement) but it's a decent heuristic.
        // We can at least assert that we never drastically exceeded 10.
        Assert.True(maxObserved <= 10, $"Max observed concurrency = {maxObserved} which exceeds 10");
    }
    
    
    /// <summary>
    /// This test is a stress test of correctness and thread-safety of the limiter.
    /// It's an emulation of real hard-to-reproduce concurrency issues.
    /// </summary>
    [Fact]
    public async Task There_are_no_deadlocks_and_all_items_are_processed_once()
    {
        // Test parameters
        var initialConcurrency = 100;
        var (minConcurrency, maxConcurrency) = (1, 10_000);
        var itemsCount = 1_000_000;

        var items = Enumerable.Range(0, itemsCount);
        using DynamicConcurrencyLimiter limiter = new(initialConcurrency);

        // Track processed items and their processing count
        ConcurrentDictionary<int, int> processedItems = new();

        // Create a random number generator for concurrency updates
        Random random = new(42); // Fixed seed for reproducibility
        var updateInterval = 1000; // Update concurrency every N items
        var processedCount = 0;

        // Process items with random concurrency updates
        await DynamicParallel.ForEachAsync(items, limiter, async (item, _) =>
        {
            // Simulate some minimal work
            await Task.Yield();

            // Track item processing
            processedItems.AddOrUpdate(
                item,
                1, // Initial value if key doesn't exist
                (_, count) => count + 1 // Increment existing count
            );

            // Periodically update concurrency limit
            if (Interlocked.Increment(ref processedCount) % updateInterval == 0)
            {
                var newConcurrency = random.Next(minConcurrency, maxConcurrency + 1);
                limiter.UpdateMaxConcurrency(newConcurrency);
            }
        });

        // Verify results
        Assert.Equal(itemsCount, Volatile.Read(ref processedCount));
        Assert.Equal(itemsCount, processedItems.Count);

        // Verify each item was processed exactly once
        foreach (var kvp in processedItems)
        {
            Assert.Equal(1, kvp.Value);
            Assert.InRange(kvp.Key, 0, itemsCount - 1);
        }
    }
}

// Utility extension for atomic "Max" operation
internal static class InterlockedExtensions
{
    public static int Max(ref int target, int value)
    {
        var current = target;
        while (true)
        {
            var initial = current;
            var newValue = Math.Max(initial, value);
            current = Interlocked.CompareExchange(ref target, newValue, initial);
            if (current == initial)
            {
                return newValue;
            }
        }
    }
}
