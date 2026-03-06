# Chapter 3: The Scheduler Trait — The Contract

## 3.1 The Design Problem

Spark needs to run on your laptop for development and on a 1,000-machine cluster for production. The same word count program, the same `collect()` call, the same result — but radically different execution environments.

How do you write the scheduling logic once and have it work in both cases?

You separate **what to do** from **how to do it**.

The "what to do" is always the same: take the RDD graph, split it into stages at shuffle boundaries, create tasks for each partition, run them in the right order, handle failures, return results.

The "how to do it" differs: on your laptop, "run a task" means submitting a Runnable to a thread pool. On a cluster, it means serializing the task, sending it over the network to a remote machine, and waiting for a result via a cluster manager protocol.

Spark solves this with three layers:

```
                    ┌──────────────┐
                    │  Scheduler   │  The contract: "I can run jobs"
                    └──────┬───────┘
                           │
                    ┌──────┴───────┐
                    │ DAGScheduler │  The brain: "I know how to build
                    │              │   stages and run the event loop"
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │                         │
    ┌─────────┴──────────┐   ┌─────────┴──────────┐
    │  LocalScheduler    │   │  MesosScheduler     │
    │  "I run tasks in   │   │  "I run tasks on    │
    │   a thread pool"   │   │   a cluster"        │
    └────────────────────┘   └─────────────────────┘
```

---

## 3.2 The Contract: Five Methods

The `Scheduler` trait is tiny — just five methods:

```scala
private trait Scheduler {
  def start()
  def waitForRegister()
  def runJob[T, U: ClassManifest](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int], allowLocal: Boolean): Array[U]
  def stop()
  def defaultParallelism(): Int
}
```

That's the entire file. Let's understand each one:

- **`start()`** — Initialize. For LocalScheduler: nothing to do. For MesosScheduler: connect to the cluster.
- **`waitForRegister()`** — Block until ready. For LocalScheduler: instant. For MesosScheduler: wait for the cluster handshake.
- **`runJob()`** — The big one. "Here's an RDD, a function, and some partitions. Give me results." This is what SparkContext calls.
- **`stop()`** — Shut down. For LocalScheduler: nothing. For MesosScheduler: disconnect from the cluster.
- **`defaultParallelism()`** — How many partitions by default? For LocalScheduler: the number of threads. For MesosScheduler: a configurable number (default 8).

---

## 3.3 The Brain: DAGScheduler

Here's the clever part. LocalScheduler and MesosScheduler don't implement `runJob()` themselves. There's a layer in between — `DAGScheduler` — that does all the thinking:

- Walk the RDD graph backwards
- Find shuffle boundaries → create stages
- Submit stages in the right order (parents first)
- Create tasks for each partition
- Run an event loop: wait for completions, handle failures, wake up waiting stages
- Return results

This logic is identical whether you're running locally or on a cluster. So it lives in DAGScheduler, shared by both.

DAGScheduler defines one abstract method that the concrete schedulers must implement:

```scala
def submitTasks(tasks: Seq[Task[_]], runId: Int): Unit
```

"Here are some tasks. Run them." **How** they get run is up to the subclass.

---

## 3.4 The Split: `runJob()` vs `submitTasks()`

This is the key architectural insight. Let's make it concrete:

```
User calls: rdd.collect()
    │
    ▼
SparkContext.runJob()
    │
    ▼
DAGScheduler.runJob()          ← SAME for both schedulers
    │
    │  1. Build stages from the RDD graph
    │  2. Find which stages need to run
    │  3. Create Task objects for each partition
    │  4. Call submitTasks(tasks)  ──────────┐
    │  5. Wait for completion events         │
    │  6. Handle successes and failures      │
    │  7. Return results                     │
    │                                        │
    └────────────────────────────────────────┘
                                             │
                    ┌────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
  LocalScheduler          MesosScheduler
  .submitTasks():         .submitTasks():
  Run in thread pool      Send to cluster
```

Steps 1–3 and 5–7 are identical for both schedulers. Only step 4 — "actually run these tasks somewhere" — differs. The stage-building logic is written once. The task-execution mechanism is pluggable.

---

## 3.5 Why This Matters for You

This layered design means we can learn the scheduling logic using LocalScheduler (simple, no network) and know that the same logic applies on a real cluster. The only thing that changes is the delivery mechanism for tasks.

It also means the rest of this book has a natural structure:

- **Chapters 4–7**: What are Stages and Tasks? (The objects DAGScheduler creates)
- **Chapters 8–9**: How does DAGScheduler work? (The brain — building stages and running the event loop)
- **Chapters 10–12**: How do LocalScheduler and MesosScheduler run tasks? (The execution layer)
- **Chapters 13–16**: How do the supporting services work? (The tools in the toolbox)

But first, we need to understand the objects that the DAGScheduler creates and manages. The most fundamental one is the **Stage**.

---

## 3.6 Summary

| Question | Answer |
|----------|--------|
| What is the Scheduler trait? | A five-method contract: start, waitForRegister, runJob, stop, defaultParallelism. |
| What is DAGScheduler? | The brain. Implements `runJob()` — builds stages, creates tasks, runs the event loop. Shared by all schedulers. |
| What do LocalScheduler and MesosScheduler implement? | `submitTasks()` — the "how to actually run tasks" part. |
| Why this layered design? | Separation of concerns. The scheduling logic is written once. Only the execution mechanism differs. |

---

**Next Chapter**: [Chapter 4: Stage — A Group of Pipelined Tasks →](Chapter-04-Stage.md)
