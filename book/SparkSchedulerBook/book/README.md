# 📘 Understanding Spark's Scheduler — From RDD Graph to Running Tasks

A guide to how Spark turns a lazy RDD graph into actual distributed execution, explained for Java developers.

Based on the actual source code of **Apache Spark 0.5.0** — the same codebase as Book 1, where the scheduler is a beautifully readable ~250-line event loop.

---

## Prerequisites

This is **Book 2** in the series. You should have read **Book 1: Understanding RDD — From Java to Spark's Core**.

You need to know:
- An RDD is a lazy description with 5 properties (splits, compute, dependencies, partitioner, preferredLocations)
- Transformations build a DAG of RDD objects; actions trigger execution
- NarrowDependency means data stays on the same machine; ShuffleDependency means data moves across the network
- The lineage graph gets split into stages at shuffle boundaries

**Book 1 ended at the moment `.collect()` is called. This book picks up exactly there.**

```
Book 1 covered this:                    Book 2 covers this:
─────────────────────                   ─────────────────────

  User writes:                            What happens inside:
  val rdd = sc.textFile(...)              
    .flatMap(...)                          SparkContext.runJob()
    .map(...)                                    │
    .reduceByKey(...)                      DAGScheduler builds stages
                                                 │
  rdd.collect()  ──── HANDOFF ────→       Tasks created and dispatched
                                                 │
  "computation happens"                    Workers execute tasks
  (Book 1 waved hands here)                      │
                                           Results returned to driver
```

---

## Table of Contents

### Part 1: The Foundation — What Gets Executed
- [Chapter 1: SparkContext — The Front Door](Chapter-01-SparkContext.md)
- [Chapter 2: SparkEnv — The Toolbox](Chapter-02-SparkEnv.md)
- [Chapter 3: The Scheduler Trait — The Contract](Chapter-03-Scheduler-Trait.md)

### Part 2: The Brain — How the DAG Becomes Stages
- [Chapter 4: Stage — A Group of Pipelined Tasks](Chapter-04-Stage.md)
- [Chapter 5: Task — The Unit of Work](Chapter-05-Task.md)
- [Chapter 6: ShuffleMapTask — Writing Shuffle Output](Chapter-06-ShuffleMapTask.md)
- [Chapter 7: ResultTask — Returning Results to the Driver](Chapter-07-ResultTask.md)
- [Chapter 8: DAGScheduler — Building Stages](Chapter-08-DAGScheduler-Stages.md)
- [Chapter 9: DAGScheduler — Running the Job](Chapter-09-DAGScheduler-RunJob.md)

### Part 3: The Executors — Where Tasks Actually Run
- [Chapter 10: LocalScheduler — Running Tasks in Threads](Chapter-10-LocalScheduler.md)
- [Chapter 11: MesosScheduler — Running Tasks on a Cluster](Chapter-11-MesosScheduler.md)
- [Chapter 12: SimpleJob — Scheduling Tasks Within a Stage](Chapter-12-SimpleJob.md)

### Part 4: The Supporting Cast — Infrastructure That Makes It Work
- [Chapter 13: MapOutputTracker — Where Did the Shuffle Data Go?](Chapter-13-MapOutputTracker.md)
- [Chapter 14: Shuffle I/O — Moving Data Across the Network](Chapter-14-Shuffle-IO.md)
- [Chapter 15: Serialization — Turning Objects into Bytes](Chapter-15-Serialization.md)
- [Chapter 16: ClosureCleaner — Making Functions Serializable](Chapter-16-ClosureCleaner.md)

### Part 5: The Big Picture
- [Chapter 17: Putting It All Together — A Complete Trace](Chapter-17-Full-Trace.md)
- [Chapter 18: What Comes Next](Chapter-18-What-Comes-Next.md)

---

## How This Book Connects to Book 1

| Book 1 (RDD) | Book 2 (Scheduler) |
|---------------|-------------------|
| "An RDD has splits" | "Each split becomes a Task" |
| "Compute produces data for a partition" | "ShuffleMapTask and ResultTask call compute" |
| "ShuffleDependency creates a stage boundary" | "DAGScheduler walks dependencies to find stage boundaries" |
| "Actions call sc.runJob()" | "runJob() is the event loop that orchestrates everything" |
| "Lineage enables fault recovery" | "DAGScheduler resubmits failed stages using the lineage" |
| "ShuffledRDD fetches data from the network" | "ShuffleManager writes files, MapOutputTracker tracks them, ShuffleFetcher retrieves them" |
