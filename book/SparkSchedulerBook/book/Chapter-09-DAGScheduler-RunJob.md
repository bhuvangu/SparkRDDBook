# Chapter 9: DAGScheduler — Running the Job

## 9.1 The Event Loop

Chapter 8 showed how the DAGScheduler builds stages. This chapter shows how it **runs** them. This is the heart of Spark's execution engine — a surprisingly simple event loop that orchestrates everything.

The core idea: the DAGScheduler submits stages, then sits in a loop waiting for tasks to complete. When a task finishes, it reacts — recording results, waking up waiting stages, or handling failures. The loop continues until all result partitions are done.

---

## 9.2 The Three Sets

The DAGScheduler tracks stages using three sets:

```
waiting  — stages whose parents haven't finished yet
running  — stages whose tasks are currently executing
failed   — stages that need to be resubmitted after a failure
```

Every stage moves through these sets:

```
                    submitStage()
                         │
                         ▼
  ┌──────────┐    ┌──────────┐    ┌──────────┐
  │ waiting  │───→│ running  │───→│ completed│
  │          │    │          │    │ (removed) │
  └──────────┘    └────┬─────┘    └──────────┘
                       │
                  fetch failure
                       │
                       ▼
                  ┌──────────┐
                  │  failed  │──→ resubmit → back to waiting/running
                  └──────────┘
```

A stage starts in `waiting` if its parents aren't done. When all parents complete, it moves to `running`. When all its tasks complete, it's removed (done). If a failure occurs, it goes to `failed` and eventually gets resubmitted.

---

## 9.3 Submitting Stages — Parents First

The `submitStage` method is recursive. When asked to submit a stage, it first checks: "Are all my parent stages done?"

- **Yes** → Create tasks and submit them. Move to `running`.
- **No** → Submit the missing parents first (recursively). Put this stage in `waiting`.

Let's trace this for word count:

```
submitStage(Stage 1):                          // the result stage
  Missing parents = [Stage 0]                  // Stage 0 isn't done
  → submitStage(Stage 0):                      // submit the parent first
      Missing parents = []                     // no parents — root stage
      → Create 3 ShuffleMapTasks, submit them
      → running = {Stage 0}
  → waiting = {Stage 1}                        // Stage 1 waits for Stage 0
```

After this, Stage 0 is running (its tasks are in the thread pool or on the cluster) and Stage 1 is waiting. The event loop takes over from here.

---

## 9.4 The Loop

The event loop is a `while` loop that runs until all result partitions have been computed:

```
while (numFinished != numOutputParts) {
    wait for a task completion event
    
    if (event is Success) {
        if (task was a ResultTask) {
            store the result
            numFinished++
        }
        if (task was a ShuffleMapTask) {
            record the output location
            if (all tasks in this stage are done) {
                register shuffle outputs
                check if any waiting stages can now run
                submit newly runnable stages
            }
        }
    }
    
    if (event is FetchFailed) {
        mark stages for resubmission
    }
    
    if (there are failed stages and enough time has passed) {
        resubmit them
    }
}
```

Let's walk through each case.

---

## 9.5 When a ResultTask Succeeds

This is the simplest case. The task computed a result for one partition. Store it and increment the counter:

```
ResultTask(outputId=0) completes with result [("apple", 3), ("banana", 1)]
  → results[0] = [("apple", 3), ("banana", 1)]
  → finished[0] = true
  → numFinished = 1

ResultTask(outputId=1) completes with result [("cherry", 2)]
  → results[1] = [("cherry", 2)]
  → finished[1] = true
  → numFinished = 2

numFinished == numOutputParts → EXIT LOOP!
Return results to the user.
```

---

## 9.6 When a ShuffleMapTask Succeeds

This is more interesting because completing a shuffle map stage can unlock waiting stages.

```
ShuffleMapTask(partition=0) completes → "http://host-A:45678"
  stage.addOutputLoc(0, "http://host-A:45678")
  pendingTasks still has tasks 1 and 2 → stage not done yet

ShuffleMapTask(partition=1) completes → "http://host-B:45679"
  stage.addOutputLoc(1, "http://host-B:45679")
  pendingTasks still has task 2 → not done yet

ShuffleMapTask(partition=2) completes → "http://host-C:45680"
  stage.addOutputLoc(2, "http://host-C:45680")
  pendingTasks is EMPTY → Stage 0 is done!
```

When a stage finishes, the DAGScheduler:

1. **Registers the shuffle outputs** with the MapOutputTracker — so reduce tasks can find the files
2. **Checks the waiting set** — are any waiting stages now runnable?
3. **Submits newly runnable stages** — creates their tasks and moves them to `running`

```
Stage 0 finished!
  → mapOutputTracker.registerMapOutputs(shuffleId=0,
      ["http://host-A:45678", "http://host-B:45679", "http://host-C:45680"])
  → Check waiting: Stage 1 is waiting
    → getMissingParentStages(Stage 1) = []  ← Stage 0 is now available!
  → Submit Stage 1: create 2 ResultTasks
  → running = {Stage 1}, waiting = {}
```

This is the cascade: Stage 0 finishes → Stage 1 becomes runnable → Stage 1's tasks are submitted → eventually Stage 1 finishes → results returned.

---

## 9.7 When a Fetch Fails

This is the failure recovery path. When a reduce task tries to fetch shuffle data and the machine that wrote it has crashed, a `FetchFailed` event arrives.

The DAGScheduler's response:

1. **Mark the reduce stage as failed** — it can't continue without the data
2. **Mark the map stage as failed too** — its output on the dead machine is lost
3. **Remove the dead machine's output location** from the map stage
4. **Wait 2 seconds** — because when a machine dies, multiple fetch failures arrive in quick succession. Waiting lets them all arrive before resubmitting.
5. **Resubmit both stages** — the map stage re-runs its lost partitions, then the reduce stage retries

```
ResultTask on host-D can't fetch from host-B (host-B crashed!)
  → FetchFailed(serverUri="http://host-B:45679", shuffleId=0, mapId=1)
  
  → Mark Stage 1 (reduce) as failed
  → Mark Stage 0 (map) as failed
  → Remove host-B's output for partition 1
  → Wait 2 seconds...
  
  → Resubmit Stage 0: only partition 1 needs recomputing
    (partitions 0 and 2 still have their output on host-A and host-C)
  → When Stage 0 finishes again → resubmit Stage 1
```

Notice: only the lost partition gets recomputed, not the entire stage. The `outputLocs` tracking from Chapter 4 makes this possible — the DAGScheduler knows exactly which partitions still have valid output.

For non-fetch failures (like a bug in user code), the DAGScheduler doesn't retry — it throws a SparkException and the job fails.

---

## 9.8 How Events Get Into the Queue

The event loop waits for `CompletionEvent` objects. Who puts them there?

The `taskEnded` method, which is called by LocalScheduler or MesosScheduler when a task finishes:

```scala
def taskEnded(task: Task[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any]) {
    lock.synchronized {
      eventQueues(task.runId) += CompletionEvent(task, reason, result, accumUpdates)
      lock.notifyAll()  // wake up the event loop!
    }
}
```

The event loop is blocked in `waitForEvent()`. When `taskEnded` is called, it adds an event to the queue and calls `notifyAll()` to wake up the loop. The loop processes the event and goes back to waiting.

This is the bridge between the execution layer (LocalScheduler/MesosScheduler) and the scheduling layer (DAGScheduler). The execution layer runs tasks and reports results. The scheduling layer reacts to those results.

---

## 9.9 The Complete Timeline — Word Count

```
Time    Action                                          State
────    ──────                                          ─────
t=0     runJob() called                                 
t=0     Build stages: Stage 0, Stage 1                  
t=0     submitStage(Stage 1)                            
t=0       → submitStage(Stage 0) [parents first]        
t=0         → 3 ShuffleMapTasks submitted               running={S0}, waiting={S1}
t=0     Enter event loop                                

t=1     ShuffleMapTask(p=0) completes                   S0: 1/3 done
t=2     ShuffleMapTask(p=1) completes                   S0: 2/3 done
t=3     ShuffleMapTask(p=2) completes                   S0: 3/3 done!
t=3       Register shuffle outputs                      
t=3       Stage 1 now runnable → 2 ResultTasks          running={S1}, waiting={}

t=4     ResultTask(outputId=0) completes                numFinished=1
t=5     ResultTask(outputId=1) completes                numFinished=2

t=5     numFinished == numOutputParts → EXIT            
t=5     Return results                                  
```

---

## 9.10 Summary

| Question | Answer |
|----------|--------|
| What does `runJob()` do? | Builds stages, submits them in order, runs an event loop until all results are collected. |
| What are the three sets? | `waiting` (parents not done), `running` (tasks submitted), `failed` (needs resubmission). |
| How does `submitStage()` work? | Recursively: submit parents first, then this stage. |
| What happens when a ShuffleMapTask succeeds? | Record output location. If stage is done, register outputs and wake up waiting stages. |
| What happens when a ResultTask succeeds? | Store the result. Increment counter. If all done, exit the loop. |
| What happens on FetchFailed? | Mark both stages as failed. Wait 2 seconds. Resubmit — only lost partitions. |
| What is the local optimization? | If `allowLocal=true`, no parent stages, and 1 partition, compute directly on the driver. No tasks. |

The DAGScheduler is the brain. It builds stages, creates tasks, and orchestrates the event loop. But it doesn't actually *run* tasks — it calls `submitTasks()`. Let's see how LocalScheduler handles that.

---

**Next Chapter**: [Chapter 10: LocalScheduler — Running Tasks in Threads →](Chapter-10-LocalScheduler.md)
