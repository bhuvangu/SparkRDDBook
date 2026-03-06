# Chapter 10: LocalScheduler — Running Tasks in Threads

## 10.1 The Simplest Executor

The DAGScheduler builds stages and creates tasks. But it doesn't run them — it calls `submitTasks()`. For `master = "local"`, that call lands in the LocalScheduler.

LocalScheduler is the simplest possible executor. It has a thread pool. When tasks arrive, it submits them to the pool. When they finish, it reports back to the DAGScheduler. That's the whole story.

This simplicity makes it the perfect place to understand how tasks actually execute. No network, no cluster manager, no remote machines — just threads running tasks in the same JVM.

---

## 10.2 The Setup

When you write `new SparkContext("local[4]", "MyApp")`, the LocalScheduler is created with a 4-thread pool:

```scala
private class LocalScheduler(threads: Int, maxFailures: Int) extends DAGScheduler {
  var threadPool = Executors.newFixedThreadPool(threads, DaemonThreadFactory)
}
```

`start()`, `waitForRegister()`, and `stop()` are all empty — there's no cluster to connect to. The thread pool is ready immediately.

---

## 10.3 What Happens When Tasks Arrive

When the DAGScheduler calls `submitTasks(tasks)`, the LocalScheduler does one thing: submit each task to the thread pool as a Runnable.

```
submitTasks([task0, task1, task2])
  → threadPool.submit(runTask(task0))
  → threadPool.submit(runTask(task1))
  → threadPool.submit(runTask(task2))
```

If you have 4 threads and 3 tasks, all 3 run in parallel. If you have 2 threads and 3 tasks, 2 run immediately and the third waits for a thread to free up.

---

## 10.4 The Surprising Serialize-Deserialize Step

Inside `runTask()`, something unexpected happens. Before running the task, the LocalScheduler serializes it to bytes and immediately deserializes it back:

```scala
val ser = SparkEnv.get.closureSerializer.newInstance()
val bytes = ser.serialize(task)
val deserializedTask = ser.deserialize[Task[_]](bytes)
val result = deserializedTask.run(attemptId)
val resultToReturn = ser.deserialize[Any](ser.serialize(result))
```

Wait — the task is already right here in memory. Why convert it to bytes and back?

Two reasons:

**Catch serialization bugs early.** If your task contains a non-serializable object (like a database connection captured in a closure), this will fail here on your laptop with a clear error — rather than failing later on a remote cluster machine where debugging is harder.

**Isolate accumulators.** When a task is deserialized, its accumulators get registered as thread-local copies. This prevents different tasks running in parallel from interfering with each other's accumulator values.

The same serialize-deserialize happens to the result. This mirrors exactly what happens on a real cluster, where the task is serialized on the driver, sent over the network, deserialized on the worker, executed, and the result is serialized back. LocalScheduler simulates this entire round-trip locally.

---

## 10.5 The Complete Flow

```
DAGScheduler.submitMissingTasks(Stage 0)
    │
    │ creates [ShuffleMapTask(p=0), ShuffleMapTask(p=1), ShuffleMapTask(p=2)]
    │
    ▼
LocalScheduler.submitTasks(tasks)
    │
    ├── Thread 1: serialize → deserialize → task0.run() → serialize result → taskEnded(Success)
    ├── Thread 2: serialize → deserialize → task1.run() → serialize result → taskEnded(Success)
    └── Thread 3: serialize → deserialize → task2.run() → serialize result → taskEnded(Success)
                                                                │
                                                                ▼
                                                    DAGScheduler event queue
                                                    (wakes up the event loop)
```

Each thread:
1. Sets `SparkEnv` for itself (ThreadLocal — so `SparkEnv.get` works)
2. Serializes and deserializes the task
3. Calls `task.run(attemptId)` — the actual computation
4. Serializes and deserializes the result
5. Calls `taskEnded()` — puts a CompletionEvent on the DAGScheduler's queue

---

## 10.6 Failure Handling

If a task throws an exception, the LocalScheduler checks the `maxFailures` parameter:

- **`"local"` (maxFailures=0)**: Any failure is immediately fatal. The exception is reported to the DAGScheduler, which throws a SparkException.
- **`"local[4,3]"` (maxFailures=3)**: The task is retried up to 3 times. Only if it fails 4 times does the job abort.

```scala
catch {
    case t: Throwable =>
      failCount(idInJob) += 1
      if (failCount(idInJob) <= maxFailures) {
        submitTask(task, idInJob)    // retry
      } else {
        taskEnded(task, new ExceptionFailure(t), null, null)  // give up
      }
}
```

This is useful for testing fault tolerance locally — you can simulate flaky tasks without needing a cluster.

---

## 10.7 Why LocalScheduler Matters

LocalScheduler is more than a development convenience. It's a teaching tool. Everything that happens on a real cluster — task serialization, execution, result reporting, failure handling — happens here too, just without the network.

If you understand LocalScheduler, you understand the execution model. MesosScheduler adds network transport and resource negotiation, but the core flow is identical: serialize task → run task → serialize result → report to DAGScheduler.

---

## 10.8 Summary

| Question | Answer |
|----------|--------|
| What is LocalScheduler? | A scheduler that runs tasks in a local thread pool. Used for `master = "local"`. |
| How many threads? | Configurable: `"local"` = 1, `"local[4]"` = 4. |
| Why serialize/deserialize locally? | To catch serialization bugs early and to isolate accumulator state between tasks. |
| How does it handle failures? | Retries up to `maxFailures` times, then reports failure to DAGScheduler. |
| How does it report completion? | Calls `taskEnded()`, which puts a CompletionEvent on the DAGScheduler's queue. |

LocalScheduler is great for learning and development. But real Spark jobs run on clusters. Let's see how MesosScheduler handles that.

---

**Next Chapter**: [Chapter 11: MesosScheduler — Running Tasks on a Cluster →](Chapter-11-MesosScheduler.md)
