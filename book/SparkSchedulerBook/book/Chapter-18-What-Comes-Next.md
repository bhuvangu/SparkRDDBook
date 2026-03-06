# Chapter 18: What Comes Next

## 18.1 What You Now Know

You've seen the complete path from `rdd.collect()` to actual results. Book 1 showed you the data model — what an RDD is, how transformations and actions work, how data flows through partitions. This book showed you the execution engine — how stages are built, how tasks are created and dispatched, how failures are recovered, how data moves across the network.

Together, the two books cover the two halves of Spark's core:

| Book 1 (RDD) | Book 2 (Scheduler) |
|---------------|-------------------|
| "An RDD has splits" | "Each split becomes a Task" |
| "Compute produces data for a partition" | "ShuffleMapTask and ResultTask call compute" |
| "ShuffleDependency creates a stage boundary" | "DAGScheduler walks dependencies to find boundaries" |
| "Actions call sc.runJob()" | "runJob() is the event loop that orchestrates everything" |
| "Lineage enables fault recovery" | "DAGScheduler resubmits failed stages using the lineage" |
| "ShuffledRDD fetches data from the network" | "ShuffleManager writes files, MapOutputTracker tracks them, ShuffleFetcher retrieves them" |

---

## 18.2 What We Didn't Cover

There are parts of the Spark 0.5.0 codebase we deliberately skipped. Each could be its own book.

**The Caching System.** We mentioned `BoundedMemoryCache` and `CacheTracker` but never opened them up. The caching system is a complete memory management subsystem — it estimates object sizes via reflection, evicts entries under memory pressure, and can spill to disk. The `CacheTracker` is a master/worker actor system that enables data-locality decisions.

**Broadcast Variables.** Spark 0.5.0 has four broadcast implementations — HTTP, tree-based, chained, and BitTorrent-style. Each is a complete distributed data distribution protocol. The BitTorrent implementation alone has its own tracker, block management, and peer selection logic.

**Accumulators.** Write-only shared variables that aggregate across tasks. The implementation is small but clever — each task gets a thread-local copy via Java's deserialization callback, and the driver merges them after task completion.

---

## 18.3 The Core Insight

The scheduler in Spark 0.5.0 is about 250 lines of event-loop logic in `DAGScheduler.runJob()`. From those 250 lines, Spark gets:

- Stage-oriented scheduling with automatic pipeline optimization
- Data locality — tasks run where their data lives
- Fault recovery — failed stages are resubmitted, lost shuffle outputs are recomputed
- Local and distributed execution from the same code

The design is a clean separation of concerns:

- **RDDs** describe *what* to compute
- **DAGScheduler** decides *when* and *in what order*
- **LocalScheduler/MesosScheduler** decides *where*
- **SparkEnv** provides *the tools*

Each layer is simple on its own. The power comes from their composition.

---

## 18.4 From 0.5.0 to Modern Spark

Spark has evolved enormously since 0.5.0, but the core architecture we studied remains:

- RDDs still have the same five properties
- The DAGScheduler still splits the graph at shuffle boundaries
- Tasks are still ShuffleMapTasks and ResultTasks
- The event loop still processes completion events

What changed: Mesos was joined by YARN and Kubernetes. The shuffle system was rewritten multiple times. DataFrames and Datasets added a higher-level API. Catalyst and Tungsten added SQL-style optimization. Structured Streaming added continuous processing.

But if you understand the 0.5.0 codebase, you understand the foundation that everything else was built on.

---

## 18.5 Thank You

From `sc.textFile("data.txt")` to the final `collect()` result, you can now trace every stage created, every task dispatched, and every byte that moves. That's a deep understanding that will serve you well — whether you're using Spark, building distributed systems, or just appreciating elegant software design.

Happy Sparking.
