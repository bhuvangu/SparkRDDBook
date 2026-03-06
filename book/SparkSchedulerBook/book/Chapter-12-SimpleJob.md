# Chapter 12: SimpleJob — Scheduling Tasks Within a Stage

## 12.1 The Data Locality Problem

When Mesos offers a machine, someone needs to decide: "Which task should run here?" This is a surprisingly important decision.

Imagine Task 0 reads HDFS block 0, which lives on Machine A. If we run Task 0 on Machine A, it reads data from local disk — fast. If we run it on Machine B, it reads data over the network from Machine A — slow.

This is **data locality**: running computation where the data already is. It can make a 10x difference in performance. SimpleJob is the component that makes these decisions.

---

## 12.2 Three Lists of Pending Tasks

SimpleJob organizes pending tasks into three lists:

```
pendingTasksForHost:     Tasks that prefer a specific machine
                         {"host-A": [task 0, task 2], "host-B": [task 1]}

pendingTasksWithNoPrefs: Tasks with no location preference
                         [task 3, task 4]

allPendingTasks:         ALL pending tasks (fallback)
                         [task 0, task 1, task 2, task 3, task 4]
```

When a task is created, it's placed in the appropriate list based on its `preferredLocations`. A task that reads HDFS block 0 on host-A goes into `pendingTasksForHost["host-A"]`. A task with no preference goes into `pendingTasksWithNoPrefs`.

---

## 12.3 The Three-Level Priority

When Mesos offers Machine A, SimpleJob picks a task using three levels:

**Level 1: Local task.** Is there a task that *prefers* Machine A? If so, run it — the data is right there.

**Level 2: No-preference task.** Is there a task with no location preference? It can run anywhere, so Machine A is fine.

**Level 3: Any task.** Just grab any pending task, even if it prefers a different machine. The data will be fetched over the network.

```
Mesos offers Machine A:
  Level 1: pendingTasksForHost["host-A"] = [task 0] → pick task 0 ✓

Mesos offers Machine C:
  Level 1: pendingTasksForHost["host-C"] = [] → empty
  Level 2: pendingTasksWithNoPrefs = [task 3] → pick task 3 ✓

Mesos offers Machine D:
  Level 1: empty for host-D
  Level 2: empty (no more no-pref tasks)
  Level 3: allPendingTasks = [task 1] → pick task 1 (non-local, but better than waiting)
```

---

## 12.4 Delay Scheduling — The Art of Waiting

Here's a subtle problem. Mesos offers machines in whatever order they become available. What if Machine A becomes available first, but the only pending task prefers Machine B?

The naive approach: run the task on Machine A anyway (Level 3). But Machine B might become available in 2 seconds. If we wait, we get data locality. If we don't wait, we waste network bandwidth.

SimpleJob uses **delay scheduling**: wait up to 5 seconds for a data-local offer before accepting a non-local one.

```
Time 0s: Task 0 prefers host-B. Mesos offers host-A.
         → Wait (still hoping for host-B)

Time 2s: Mesos offers host-C.
         → Wait (still hoping)

Time 4s: Mesos offers host-B!
         → Match! Run Task 0 on host-B (local!) ✓

--- OR ---

Time 5s: Still no offer from host-B.
         → Give up waiting. Run Task 0 on the next available machine (non-local).
```

The 5-second window (`LOCALITY_WAIT`) is a tradeoff. Too short and you miss locality opportunities. Too long and you waste time waiting. The default works well in practice — most cluster managers cycle through machines within a few seconds.

---

## 12.5 Failure Handling

When a task fails, SimpleJob distinguishes two cases:

**FetchFailed** — the task couldn't download shuffle data because a machine died. This isn't a problem with *this* task — it's a problem with a *previous* stage. SimpleJob reports it to the DAGScheduler, which resubmits the map stage. The task is marked as finished (it won't be retried at this level).

**Other failures** — a bug in user code, an out-of-memory error, etc. SimpleJob puts the task back in the pending lists for retry. If it fails more than `MAX_TASK_FAILURES` times (default: 4), the entire job is aborted.

```
Task fails with FetchFailed:
  → Report to DAGScheduler (stage resubmission)
  → Don't retry this task

Task fails with NullPointerException:
  → Put back in pending lists
  → numFailures[task]++
  → If numFailures > 4 → abort the job
```

---

## 12.6 Summary

| Question | Answer |
|----------|--------|
| What is SimpleJob? | Manages task scheduling within a single stage for MesosScheduler. |
| How does it pick tasks? | Three-level priority: local tasks → no-preference tasks → any task. |
| What is delay scheduling? | Wait up to 5 seconds for a data-local offer before accepting a non-local one. |
| How does it handle FetchFailed? | Reports to DAGScheduler for stage resubmission. Doesn't retry the task. |
| How does it handle other failures? | Retries up to 4 times, then aborts the job. |

We've now covered the complete scheduling stack: DAGScheduler builds stages and runs the event loop → LocalScheduler/MesosScheduler dispatches tasks → SimpleJob handles data locality and retries. The next chapters cover the supporting infrastructure that makes it all work.

---

**Next Chapter**: [Chapter 13: MapOutputTracker — Where Did the Shuffle Data Go? →](Chapter-13-MapOutputTracker.md)
