# Chapter 9: Putting It All Together

## 9.1 The Three Primitives

After three books, you now understand every primitive in the Spark programming model:

| Primitive | Direction | What it does | Books |
|-----------|-----------|-------------|-------|
| **RDD** | Distributed | A lazy collection spread across machines. Transformations build a graph. Actions trigger execution. | Book 1 + 2 |
| **Broadcast** | Driver → Workers | Sends a large read-only variable to every machine once, shared by all tasks. | Book 3, Part 1 |
| **Accumulator** | Workers → Driver | Collects write-only values from tasks back to the driver. | Book 3, Part 2 |

```
                    Driver
                   ╱      ╲
        Broadcast ╱        ╲ Accumulator
        (read)   ╱          ╲ (write)
               ╱              ╲
          Workers ──── RDD ──── Workers
              (distributed computation)
```

---

## 9.2 The Common Trick

All three primitives use the same underlying mechanism: **Java serialization hooks**.

| Primitive | What `readObject` does |
|-----------|----------------------|
| RDD (in tasks) | Nothing special — standard deserialization |
| Broadcast | Checks cache → downloads data if missing → caches it |
| Accumulator | Registers a thread-local copy starting at zero |

The scheduler doesn't know about broadcast or accumulators. It just serializes tasks and deserializes results. The magic happens inside `readObject` — invisible to the rest of the system.

---

## 9.3 A Complete Example

Let's trace a program that uses all three:

```scala
val sc = new SparkContext("local[4]", "Example")

// Broadcast: a lookup table sent to every machine
val stopWords = sc.broadcast(Set("the", "a", "an", "is"))

// Accumulator: count malformed lines
val badLines = sc.accumulator(0)

val counts = sc.textFile("data.txt")
    .flatMap(line => {
        if (line.isEmpty) {
            badLines += 1              // accumulator: write
            Iterator.empty
        } else {
            line.split(" ").iterator
        }
    })
    .filter(word => !stopWords.value.contains(word))  // broadcast: read
    .map(word => (word, 1))
    .reduceByKey(_ + _)

val result = counts.collect()
println("Bad lines: " + badLines.value)   // accumulator: read on driver
```

What happens:

1. `sc.broadcast(Set(...))` — the stop words set is written to a file on the driver's HTTP server.

2. `sc.accumulator(0)` — an Accumulator with id=1 is created and registered as an original on the driver.

3. `counts.collect()` triggers the scheduler. Tasks are created, serialized, and shipped to workers.

4. On each worker, task deserialization triggers:
   - `stopWords.readObject` → cache miss → downloads the set via HTTP → caches it
   - `badLines.readObject` → registers a thread-local copy starting at 0

5. The task runs:
   - Empty lines increment the local `badLines` copy
   - Non-empty lines are split into words
   - Words are filtered against `stopWords.value` (the cached broadcast copy)
   - Remaining words are mapped to `(word, 1)` pairs

6. Task finishes:
   - Result (the pairs) is sent back as the task result
   - Accumulator values `{1 → N}` are sent back as `accumUpdates`

7. On the driver:
   - Results are collected into the final array
   - Accumulator values are merged: `badLines.value = sum of all tasks' counts`

8. `println("Bad lines: " + badLines.value)` prints the total.

---

## 9.4 What's Left in Spark 0.5.0

With three books, you've covered:

- **Book 1**: RDDs — the data model (splits, compute, dependencies, partitioners, every RDD subclass)
- **Book 2**: The Scheduler — the execution engine (stages, tasks, DAGScheduler, LocalScheduler, MesosScheduler, shuffle I/O, serialization, ClosureCleaner)
- **Book 3**: Shared Variables — broadcast (HTTP, Tree, BitTorrent) and accumulators

One topic remains: **the caching system** — BoundedMemoryCache, DiskSpillingCache, CacheTracker, and SizeEstimator. That's Book 4 — the final piece of the Spark 0.5.0 puzzle.

---

## 9.5 The Design Philosophy

Looking back across all three books, a pattern emerges. Spark 0.5.0 is built on a few simple ideas composed together:

- **Lazy descriptions** (RDDs) that separate what-to-compute from how-to-compute
- **Serialization hooks** (`readObject`) that trigger side effects during deserialization
- **Plugin architectures** (Serializer, BroadcastFactory, ShuffleFetcher) that swap implementations via config
- **Master/worker patterns** (MapOutputTracker, CacheTracker, Broadcast Guide) for coordination
- **Thread-local isolation** (SparkEnv, Accumulators) for safe concurrent execution

Each piece is simple on its own. The power comes from their composition. The entire core of Spark 0.5.0 — the system that launched a revolution in big data processing — is about 5,000 lines of Scala.
