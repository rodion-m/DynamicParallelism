# DynamicParallelism

**DynamicParallelism** is a .NET library designed for executing dynamic parallel operations with adjustable concurrency levels at runtime. It provides a flexible alternative to `Parallel.ForEachAsync`, allowing for real-time control over concurrency levels while processing collections asynchronously.

This library is written by o1-pro in a pair with Sonnet 3.5.

## Features
- Dynamically adjust concurrency limits at runtime.
- Fine-grained control of asynchronous task execution.
- Handles exceptions and supports graceful cancellation.
- Lightweight and thread-safe implementation.

---

## Installation
To use the **DynamicParallelism** library in your project, add the source code directly to your project or package it into a reusable library.

---

## Usage

### DynamicConcurrencyLimiter
The `DynamicConcurrencyLimiter` is a core component that limits and dynamically adjusts the concurrency of tasks.

#### Example: Basic Usage of `DynamicConcurrencyLimiter`
```csharp
using DynamicParallelism;
using System;
using System.Threading.Tasks;

var limiter = new DynamicConcurrencyLimiter(initialMaxConcurrency: 3);

// Acquire a concurrency slot asynchronously
var permit = await limiter.AcquireAsync();
try
{
    // Perform work here
    Console.WriteLine("Work in progress");
}
finally
{
    // Release the concurrency slot when done
    permit.Dispose();
}

// Update the concurrency level dynamically
limiter.UpdateMaxConcurrency(5);

// Dispose the limiter when done
limiter.Dispose();
```

---

### DynamicParallel.ForEachAsync
The `DynamicParallel.ForEachAsync` method enables processing collections with dynamically adjustable concurrency levels.

#### Example: Processing a Collection with Dynamic Concurrency
```csharp
using DynamicParallelism;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

var items = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
var limiter = new DynamicConcurrencyLimiter(initialMaxConcurrency: 3);

// Define the asynchronous processing logic
async ValueTask ProcessItem(int item, CancellationToken token)
{
    Console.WriteLine($"Processing item {item}");
    await Task.Delay(1000, token); // Simulate work
}

// Process the items with dynamic concurrency
await DynamicParallel.ForEachAsync(
    items,
    limiter,
    ProcessItem,
    CancellationToken.None
);

// Adjust concurrency dynamically
limiter.UpdateMaxConcurrency(5);

// Dispose the limiter when done
limiter.Dispose();
```

---

## Handling Exceptions
Exceptions encountered during task execution are aggregated and reported back once all tasks have completed or canceled. You can use `try-catch` blocks to handle these exceptions.

#### Example: Exception Handling
```csharp
try
{
    await DynamicParallel.ForEachAsync(
        items,
        limiter,
        async (item, token) =>
        {
            if (item == 5)
                throw new InvalidOperationException("Error processing item 5");
            Console.WriteLine($"Processed {item}");
        },
        CancellationToken.None
    );
}
catch (AggregateException ex)
{
    foreach (var inner in ex.InnerExceptions)
    {
        Console.WriteLine($"Exception: {inner.Message}");
    }
}
```

---

## Advanced Scenarios

### Dynamically Adjusting Concurrency at Runtime
Concurrency levels can be adjusted dynamically during task execution, allowing you to throttle or expand parallelism as needed.

#### Example: Real-Time Concurrency Adjustment
```csharp
var adjusterTask = Task.Run(async () =>
{
    await Task.Delay(5000); // Wait 5 seconds
    limiter.UpdateMaxConcurrency(10); // Increase concurrency to 10
    Console.WriteLine("Concurrency level updated to 10");
});

await DynamicParallel.ForEachAsync(
    items,
    limiter,
    async (item, token) =>
    {
        Console.WriteLine($"Processing {item} with dynamic concurrency");
        await Task.Delay(1000, token);
    },
    CancellationToken.None
);

await adjusterTask;
```

---

## License
This project is licensed under the MIT License. See the LICENSE file for details.
