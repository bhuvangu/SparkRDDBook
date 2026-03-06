# Chapter 5: Task — The Unit of Work

## 5.1 The Packaging Problem

A stage has 16,000 partitions. Each partition needs to be processed — possibly on a different machine in the cluster. But a remote machine doesn't have your RDD objects. It doesn't have your functions. It doesn't know which partition to process.

So the scheduler needs to create a **self-contained package** for each partition — something that carries everything needed to do the work: the RDD, the partition identifier, the function to apply. This package gets converted to bytes, shipped over the network, unpacked on the remote machine, and executed.

That package is a **Task**.

Think of it like a work order in a factory. The work order doesn't say "go ask the manager what to do." It says: "Here's the blueprint (RDD). Here's which piece to build (partition). Here's the spec (function). Everything you need is on this sheet. Go."

---

## 5.2 What's in the Package?

Every task needs:

1. **What to compute** — which RDD, which partition
2. **Where it prefers to run** — for data locality (run near the data, not across the network)
3. **How to execute** — a `run()` method
4. **How to be shipped** — it must be serializable (convertible to bytes)

```scala
abstract class Task[T] extends Serializable {
  def run(id: Int): T
  def preferredLocations: Seq[String] = Nil
  def generation: Option[Long] = None
}
```

`run(attemptId)` does the work and returns a result. The `attemptId` distinguishes retries — if a task fails and gets retried, the retry gets a different ID.

`preferredLocations` tells the scheduler: "I'd like to run on these machines because that's where my data is." For a task reading HDFS block 3, this would be the machines that have copies of block 3. The scheduler *tries* to honor this, but doesn't guarantee it.

`generation` is a version number for shuffle data locations — we'll explore this in Chapter 13.

---

## 5.3 The Identity Card: TaskContext

Every task also carries a small identity card that gets passed to the user's function:

```scala
class TaskContext(val stageId: Int, val splitId: Int, val attemptId: Int)
```

"I am processing partition 5 of stage 2, and this is my first attempt." Most user code ignores it, but it's there if needed.

---

## 5.4 Two Kinds of Tasks

Just as there are two kinds of stages, there are two kinds of tasks:

**ShuffleMapTask** — used in shuffle map stages. Its job is hardcoded: read the partition, sort data into buckets by key, write each bucket to a file, return the server URI. It doesn't run a user function — the bucketing logic is built in.

**ResultTask** — used in the final result stage. Its job is flexible: read the partition, apply whatever function the user's action provided (`iter.toArray` for collect, a counter for count, etc.), return the result.

```
                    ┌──────────┐
                    │  Task[T] │
                    └────┬─────┘
                         │
            ┌────────────┼────────────┐
            │                         │
  ┌─────────┴──────────┐   ┌─────────┴──────────┐
  │ ShuffleMapTask     │   │ ResultTask          │
  │                    │   │                     │
  │ Hardcoded logic:   │   │ Flexible:           │
  │ bucket + write     │   │ runs user's function│
  │                    │   │                     │
  │ Returns: URI       │   │ Returns: user value │
  └────────────────────┘   └─────────────────────┘
```

---

## 5.5 The Journey of a Task

```
1. DAGScheduler creates the Task
   (packages the RDD, partition, function, preferred locations)
        │
2. Scheduler serializes it to bytes
   (the entire package — RDD, function, everything)
        │
3. Bytes shipped to a worker
   (thread pool locally, or network to a cluster machine)
        │
4. Worker deserializes the Task
   (unpacks the package)
        │
5. Worker calls task.run(attemptId)
   (the actual computation — rdd.iterator(split), apply function)
        │
6. Result serialized and sent back
   (URI for ShuffleMapTask, user value for ResultTask)
        │
7. DAGScheduler receives the result
   (updates tracking, wakes up waiting stages)
```

Steps 2–4 are why `Task extends Serializable`. The entire task — RDD references, user functions, partition info — must be convertible to bytes. Even LocalScheduler does this serialize-deserialize round-trip (to catch bugs early and isolate accumulators).

---

## 5.6 Only Missing Partitions Get Tasks

One important detail: the DAGScheduler only creates tasks for partitions that haven't been computed yet. If a stage partially completed before a failure, only the missing partitions get new tasks:

```
Stage 0 has 16,000 partitions.
15,999 completed. Partition 7,342 failed.

On retry: only 1 ShuffleMapTask is created (for partition 7,342).
The other 15,999 are skipped — their output already exists.
```

This is why the Stage tracks `outputLocs` per partition — so the scheduler knows exactly which partitions still need work.

---

## 5.7 Summary

| Question | Answer |
|----------|--------|
| What is a Task? | A self-contained package that carries everything needed to process one partition on a remote machine. |
| Why self-contained? | Because it gets serialized and shipped. It can't reach back to the driver for more info. |
| What does `run()` do? | Executes the task and returns a result. |
| What is `preferredLocations`? | Hosts where this task's data lives — the scheduler tries to run the task there. |
| How many task types? | Two: ShuffleMapTask (writes shuffle files) and ResultTask (returns results). |
| Are tasks created for all partitions? | No — only for partitions that haven't been computed yet. |

Now let's look inside the two concrete task types, starting with ShuffleMapTask.

---

**Next Chapter**: [Chapter 6: ShuffleMapTask — Writing Shuffle Output →](Chapter-06-ShuffleMapTask.md)
