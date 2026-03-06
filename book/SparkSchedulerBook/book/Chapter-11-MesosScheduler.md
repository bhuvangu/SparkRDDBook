# Chapter 11: MesosScheduler — Running Tasks on a Cluster

## 11.1 The Leap from Local to Distributed

LocalScheduler runs tasks in threads on your machine. MesosScheduler runs tasks on a cluster of machines managed by Apache Mesos. The DAGScheduler doesn't know the difference — it calls `submitTasks()` either way.

But the execution model changes fundamentally. On your laptop, "run a task" means submitting a Runnable to a thread pool. On a cluster, it means:

1. Waiting for a machine to become available
2. Deciding which task should run on which machine (data locality)
3. Serializing the task and sending it over the network
4. Running it on a remote JVM
5. Getting the result back over the network

The interesting question isn't "how does Mesos work?" (that's a separate system). The interesting question is: **how does Spark decide which task runs where?**

---

## 11.2 The Resource Offer Model

Mesos uses a **push model** for resources. Instead of Spark asking "give me a machine," Mesos says "here's what's available — what do you want to do with it?"

```
Mesos Master: "Machine A has 4 CPUs free. Machine B has 2 CPUs free."
     │
     ▼
MesosScheduler: "I'll run Task 0 on Machine A (that's where its data is).
                 I'll run Task 1 on Machine B."
```

This is called a **resource offer**. Mesos pushes offers to Spark. Spark decides which tasks to place on which machines. Mesos launches them.

---

## 11.3 The Lifecycle

```
1. start()
   Spark connects to the Mesos master in a background thread.
   Mesos calls registered() when the connection is established.

2. submitTasks(tasks)
   Spark wraps the tasks in a SimpleJob and asks Mesos for resources.

3. resourceOffers(offers)     [Mesos calls this]
   For each offered machine, Spark picks a task to run there.
   Tasks are launched on the offered machines.

4. statusUpdate(status)       [Mesos calls this]
   A task finished (or failed). Spark routes the result to the DAGScheduler.

5. stop()
   Disconnect from Mesos.
```

---

## 11.4 `submitTasks()` — Not Running, Just Queuing

Unlike LocalScheduler (which runs tasks immediately), MesosScheduler just queues them:

```scala
def submitTasks(tasks: Seq[Task[_]], runId: Int) {
    val myJob = new SimpleJob(this, tasks, runId, jobId)
    activeJobs(jobId) = myJob
    activeJobsQueue += myJob
    driver.reviveOffers()  // "Hey Mesos, I have work — send me offers!"
}
```

The tasks are wrapped in a `SimpleJob` (which we'll explore in Chapter 12) and added to a queue. Then Spark pokes Mesos: "I need resources." The actual task launching happens later, when Mesos sends resource offers.

---

## 11.5 `resourceOffers()` — Matching Tasks to Machines

When Mesos has available resources, it calls `resourceOffers()`. This is where the magic happens.

For each offered machine, Spark asks its active jobs: "Do you have a task that should run here?" Jobs are checked in FIFO order (earlier jobs get first pick). Within each job, the SimpleJob picks tasks based on data locality — preferring tasks whose data is on the offered machine.

```
Mesos offers: Machine A (4 CPUs), Machine B (2 CPUs)

Job 0 (Stage 0, 3 tasks):
  Task 0 prefers Machine A → place on Machine A ✓
  Task 1 prefers Machine C → Machine C not offered, skip for now
  Task 2 has no preference → place on Machine B ✓

Launch: Task 0 on Machine A, Task 2 on Machine B
Task 1 waits for a future offer from Machine C (or gives up after a timeout)
```

---

## 11.6 The Executor — Running Tasks on Workers

On each worker machine, a Spark **Executor** process runs. When Mesos tells it to run a task, the Executor does exactly what LocalScheduler does — just on a remote machine:

1. Set `SparkEnv` for this thread
2. Deserialize the task from the bytes that came over the network
3. Update the MapOutputTracker generation (for stale data detection)
4. Call `task.run(attemptId)` — the actual computation
5. Serialize the result
6. Send it back to the driver via Mesos

The task doesn't know it's running remotely. It calls `rdd.iterator(split)`, which chains through the compute pipeline, which might read from HDFS or fetch shuffle data — all using the local SparkEnv's tools. The same code runs identically on your laptop (via LocalScheduler) and on a cluster machine (via Executor).

---

## 11.7 Results Come Back via `statusUpdate()`

When a task finishes on a worker, Mesos calls `statusUpdate()` on the driver. The MesosScheduler routes the result to the right SimpleJob, which deserializes it and calls `taskEnded()` — the same DAGScheduler method that LocalScheduler calls.

From the DAGScheduler's perspective, it doesn't matter whether the task ran locally or remotely. A CompletionEvent appears on the queue either way.

---

## 11.8 LocalScheduler vs MesosScheduler

| | LocalScheduler | MesosScheduler |
|---|---|---|
| Where tasks run | Thread pool on your machine | Worker machines in a cluster |
| How tasks are dispatched | `threadPool.submit(runnable)` | Mesos resource offers → `launchTasks()` |
| How results come back | Direct `taskEnded()` call | Mesos `statusUpdate()` → `taskEnded()` |
| Data locality | Not meaningful (everything is local) | Critical — SimpleJob places tasks near their data |
| Task serialization | Serialize/deserialize locally (for testing) | Serialize on driver, send over network, deserialize on worker |
| `start()` | No-op | Connects to Mesos master |

The DAGScheduler doesn't care about these differences. It calls `submitTasks()` and waits for `taskEnded()`. The execution layer is pluggable.

---

## 11.9 Summary

| Question | Answer |
|----------|--------|
| What is MesosScheduler? | A scheduler that runs tasks on a Mesos cluster. |
| How does it get resources? | Mesos pushes resource offers. Spark decides which tasks to place where. |
| What is the Executor? | A process on each worker that deserializes and runs tasks. |
| How do results get back? | Worker sends status update via Mesos → MesosScheduler → `taskEnded()`. |
| How is it similar to LocalScheduler? | Both call `taskEnded()` to notify the DAGScheduler. The event loop is identical. |

The MesosScheduler delegates per-stage task management to SimpleJob. That's where data locality and retry logic live. Let's look at it next.

---

**Next Chapter**: [Chapter 12: SimpleJob — Scheduling Tasks Within a Stage →](Chapter-12-SimpleJob.md)
