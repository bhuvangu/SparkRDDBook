# 📘 Book 2 Plan: Understanding Spark's Scheduler — From RDD Graph to Running Tasks

## Writing Style Guide (Learned from Book 1)

### Pacing & Structure
- Every chapter opens with a **real-world analogy** or **motivating question** (not code)
- Concepts are introduced **one at a time**, never two new ideas in the same paragraph
- Every Scala code block is followed by a **Java translation** with comments
- ASCII diagrams are used **heavily** — at least 2-3 per chapter
- Each chapter ends with a **summary table** (Question | Answer format)
- Chapters link forward ("we'll see this in Chapter X") and backward ("remember from Chapter Y")
- The tone is conversational: "Let's look at...", "Notice what this does NOT contain", "Great question."

### Code Presentation
- Show the **actual Spark 0.5.0 source code** first, then explain it
- Always provide a **Java equivalent** immediately after Scala code
- Use inline comments in Java translations to explain what's happening
- Bold the file name and path when introducing source code
- Walk through code **line by line** for complex methods
- Use concrete examples with actual data (not abstract "element A, element B")

### Explanation Style
- Never assume the reader knows a concept — define it when first used
- Use **tables** for comparisons (Narrow vs Shuffle, ResultTask vs ShuffleMapTask)
- Use **"What happens"** sections with numbered steps for processes
- End-to-end traces with concrete data (like the word count in Chapter 19)
- Frequently ask rhetorical questions the reader would ask, then answer them

### What NOT to do
- Don't introduce Scala syntax — Book 1 already covered that (Chapter 2)
- Don't re-explain RDD basics — reference Book 1 chapters instead
- Don't skip steps — if there's a chain of 5 method calls, trace all 5
- Don't use abstract examples — always use concrete data like "apple", "banana", partition numbers

---

## Reader's Mental State at Start of This Book

The reader has finished Book 1. They know:
- An RDD is a lazy description with 5 properties (splits, compute, dependencies, partitioner, preferredLocations)
- Transformations build a DAG of RDD objects; actions trigger execution
- NarrowDependency = same machine, ShuffleDependency = network transfer
- The lineage graph gets split into stages at shuffle boundaries
- Chapter 18 showed stages/tasks at a HIGH level (boxes in a diagram)
- Chapter 19 traced word count through the RDD layer but WAVED HANDS at "the scheduler creates stages and tasks"

**The gap**: They know WHAT stages and tasks are conceptually, but not HOW they get created, scheduled, dispatched, executed, and recovered. They've never seen the scheduler code.

**The question in their mind**: "OK, I understand the RDD graph. But when I call `.collect()`, what ACTUALLY happens inside Spark? How does it go from a graph of RDD objects to actual code running on machines?"

---

## Book Structure

### Prerequisites Recap (NOT a full chapter — just a 1-page section in the Introduction)
- Brief reminder: RDD = lazy description, action triggers execution, shuffle = stage boundary
- "In Book 1, we stopped at the moment `.collect()` is called. This book picks up exactly there."
- Diagram showing where Book 1 ended and Book 2 begins

---

### Part 1: The Foundation — What Gets Executed

**Chapter 1: SparkContext — The Front Door**
- Motivation: "Every Spark program starts with `new SparkContext(...)`. What does it actually create?"
- Source: `SparkContext.scala` — the constructor
- What it creates: scheduler, environment (SparkEnv), broadcast system
- The `runJob` method — the single gateway from actions to the scheduler
- How `collect()`, `count()`, `reduce()` all funnel into `runJob`
- Trace: `rdd.collect()` → `sc.runJob(rdd, func, partitions, allowLocal)` → scheduler.runJob(...)
- Key files: `SparkContext.scala`
- Ends with: "runJob hands off to the scheduler. But what IS the scheduler? That's next."

**Chapter 2: SparkEnv — The Toolbox**
- Motivation: "SparkContext creates something called SparkEnv. What's in it?"
- Source: `SparkEnv.scala` — the class and the factory method
- Walk through each component: cache, serializer, closureSerializer, cacheTracker, mapOutputTracker, shuffleFetcher, shuffleManager
- Don't explain each one deeply yet — just name them and say what they do in one sentence
- The ThreadLocal pattern — why each thread gets its own SparkEnv reference
- Analogy: SparkEnv is like a toolbox. The scheduler is the carpenter. The tools are cache, serializer, etc.
- Key files: `SparkEnv.scala`
- Ends with: "Now we know the tools. Let's meet the carpenter — the Scheduler."

**Chapter 3: The Scheduler Trait — The Contract**
- Motivation: "Spark can run locally or on a cluster. How does it handle both?"
- Source: `Scheduler.scala` — the trait (tiny, ~15 lines)
- The 4 methods: start(), waitForRegister(), runJob(), stop(), defaultParallelism()
- The Strategy pattern — one interface, multiple implementations
- Two implementations: LocalScheduler (threads on your laptop) and MesosScheduler (real cluster)
- Both extend DAGScheduler (the stage-oriented logic)
- Inheritance diagram: Scheduler ← DAGScheduler ← LocalScheduler / MesosScheduler
- Key files: `Scheduler.scala`
- Ends with: "Both schedulers share the same stage-creation logic in DAGScheduler. Let's understand stages first."

---

### Part 2: The Brain — How the DAG Becomes Stages

**Chapter 4: Stage — A Group of Pipelined Tasks**
- Motivation: "Book 1 said 'stages are groups of narrow dependencies.' But what IS a Stage object?"
- Source: `Stage.scala` — the class (~40 lines)
- Fields: id, rdd, shuffleDep, parents, isShuffleMap, numPartitions, outputLocs, numAvailableOutputs
- Two kinds of stages: ShuffleMapStage (writes shuffle files) vs ResultStage (returns results to driver)
- The `isAvailable` method — how Spark knows if a stage's output is ready
- `addOutputLoc` / `removeOutputLoc` — tracking where shuffle outputs live
- Concrete example: word count creates 2 stages, trace the Stage objects
- Key files: `Stage.scala`

**Chapter 5: Task — The Unit of Work**
- Motivation: "A stage has many partitions. Each partition becomes a Task. What does a Task look like?"
- Source: `Task.scala` — the base class (tiny)
- TaskContext: stageId, splitId, attemptId — the identity of a running task
- Two concrete task types (preview — detailed in next two chapters)
- The `run(attemptId)` method — what actually executes on a worker
- `preferredLocations` — where the task wants to run
- `generation` — how tasks know about stale shuffle data (preview for MapOutputTracker chapter)
- Key files: `Task.scala`

**Chapter 6: ShuffleMapTask — Writing Shuffle Output**
- Motivation: "Stage 1 tasks don't return results to the driver. They write shuffle files. How?"
- Source: `ShuffleMapTask.scala`
- Walk through `run()` line by line:
  1. Iterate over the RDD partition
  2. For each (key, value), compute bucket = partitioner.getPartition(key)
  3. Map-side combining using Aggregator (createCombiner / mergeValue)
  4. Write each bucket to a separate file using the Serializer
  5. Return the ShuffleManager's server URI (so reducers know where to fetch)
- Concrete example: 3 words going into 2 buckets, show the actual HashMap contents
- The connection to ShuffledRDD.compute() from Book 1 Chapter 13
- Key files: `ShuffleMapTask.scala`

**Chapter 7: ResultTask — Returning Results to the Driver**
- Motivation: "The final stage doesn't write shuffle files. It runs the user's function and sends results back. How?"
- Source: `ResultTask.scala`
- Walk through `run()`: create TaskContext, call `rdd.iterator(split)`, apply user function
- How `collect()` function becomes `iter => iter.toArray` inside a ResultTask
- The `outputId` field — mapping task results back to the right partition slot
- Contrast with ShuffleMapTask in a comparison table
- Key files: `ResultTask.scala`

**Chapter 8: DAGScheduler — The Brain (Part 1: Building Stages)**
- Motivation: "We know what stages and tasks are. Now: how does Spark CREATE them from the RDD graph?"
- Source: `DAGScheduler.scala` — the stage-building methods
- `newStage()` — creating a Stage object, registering with cacheTracker and mapOutputTracker
- `getParentStages(rdd)` — walking the RDD graph backwards, stopping at shuffle boundaries
  - Trace through word count: start at ShuffledRDD, walk back, find ShuffleDependency, create parent stage
  - The recursive `visit()` function — depth-first traversal
- `getShuffleMapStage(shuf)` — caching stages so the same shuffle doesn't create duplicate stages
- `getMissingParentStages(stage)` — which parent stages still need to run?
  - Checks cache locations (is the data already cached?)
  - Checks stage availability (have all shuffle outputs been produced?)
- Concrete example: build the stage graph for word count, show every method call
- Key files: `DAGScheduler.scala` (stage-building portion)

**Chapter 9: DAGScheduler — The Brain (Part 2: Running the Job)**
- Motivation: "Stages are built. Now how does Spark actually run them in the right order?"
- Source: `DAGScheduler.scala` — the `runJob()` method
- This is the BIG chapter — the event loop. Go VERY slow.
- The three sets: `waiting`, `running`, `failed`
- `submitStage(stage)` — recursive: submit parents first, then this stage
- `submitMissingTasks(stage)` — create actual Task objects (ShuffleMapTask or ResultTask)
- The event loop: `while (numFinished != numOutputParts)`
  - `waitForEvent()` — blocking wait for task completion
  - Handling Success:
    - ResultTask: store result, increment numFinished
    - ShuffleMapTask: record output location, check if stage is done, wake up waiting stages
  - Handling FetchFailed: mark stages for resubmission, wait for timeout, resubmit
  - Handling other failures: throw SparkException
- The `allowLocal` optimization — short-circuit for `first()` and `take()`
- Concrete example: trace the ENTIRE event loop for word count, event by event
- Key files: `DAGScheduler.scala` (runJob portion)

---

### Part 3: The Executors — Where Tasks Actually Run

**Chapter 10: LocalScheduler — Running Tasks in Threads**
- Motivation: "When you run Spark with `master = 'local'`, there's no cluster. How do tasks run?"
- Source: `LocalScheduler.scala`
- Thread pool with configurable number of threads
- `submitTasks()` — submit each task to the thread pool
- `runTask()` — the actual execution:
  1. Set SparkEnv for this thread
  2. Serialize and deserialize the task (why? to simulate network transfer — catch serialization bugs early!)
  3. Call `task.run(attemptId)`
  4. Serialize and deserialize the result (same reason)
  5. Report success via `taskEnded()`
- Failure handling: retry up to `maxFailures` times
- Why LocalScheduler is great for learning — same logic, no network complexity
- Key files: `LocalScheduler.scala`

**Chapter 11: MesosScheduler — Running Tasks on a Cluster**
- Motivation: "On a real cluster, Spark uses Mesos to get machines. How does that work?"
- Source: `MesosScheduler.scala`
- What is Mesos? (brief — just enough to understand the callbacks)
- The lifecycle: registered → resourceOffers → statusUpdate
- `resourceOffers()` — Mesos offers CPU/memory, Spark decides which tasks to run where
- FIFO job ordering via priority queue
- `submitTasks()` — creates a SimpleJob and adds it to the active queue
- `statusUpdate()` — routes task completion/failure to the right Job
- The Executor: how tasks actually run on worker machines
  - Deserialize task, run it, serialize result, send status update
- Key files: `MesosScheduler.scala`, `Executor.scala`

**Chapter 12: SimpleJob — Scheduling Tasks Within a Stage**
- Motivation: "MesosScheduler manages jobs. But who decides WHICH task runs on WHICH machine?"
- Source: `SimpleJob.scala`
- Data structures: `pendingTasksForHost`, `pendingTasksWithNoPrefs`, `allPendingTasks`
- `findTask(host, localOnly)` — the task selection algorithm
  1. First try: task with preferred location matching this host (data locality!)
  2. Second try: task with no location preference
  3. Third try (if !localOnly): any pending task
- Delay scheduling: `LOCALITY_WAIT` — wait a bit for a local task before accepting a non-local one
- `slaveOffer()` — responding to a Mesos resource offer
- Task serialization and the Mesos protobuf format
- `taskFinished()` / `taskLost()` — handling completion and failure
- Retry logic: `MAX_TASK_FAILURES` — how many times a task can fail before the job aborts
- FetchFailed handling — special case that triggers stage resubmission
- Key files: `SimpleJob.scala`, `Job.scala`

---

### Part 4: The Supporting Cast — Infrastructure That Makes It Work

**Chapter 13: MapOutputTracker — Where Did the Shuffle Data Go?**
- Motivation: "After ShuffleMapTasks write files, how do the reduce tasks know WHERE to fetch them?"
- Source: `MapOutputTracker.scala`
- Master/worker architecture using Scala remote actors
- `registerShuffle()` / `registerMapOutputs()` — master records locations
- `getServerUris()` — workers ask master for shuffle output locations
- The `generation` number — invalidating stale locations after failures
- `unregisterMapOutput()` — removing a location when a machine dies
- The fetching synchronization — only one thread fetches locations for a given shuffle
- Key files: `MapOutputTracker.scala`

**Chapter 14: ShuffleManager and ShuffleFetcher — Moving Data Across the Network**
- Motivation: "We know shuffle data gets written and tracked. But HOW does it physically move?"
- Source: `ShuffleManager.scala`, `ShuffleFetcher.scala`, `SimpleShuffleFetcher.scala`
- ShuffleManager: creates local directories, starts HTTP server, serves shuffle files
- The file layout: `shuffleDir/shuffleId/mapId/reduceId`
- SimpleShuffleFetcher: fetches shuffle data via HTTP, one server at a time
- ParallelShuffleFetcher: fetches from multiple servers concurrently
- FetchFailedException — what happens when a fetch fails
- The complete shuffle data path: write → track → fetch → merge
- Key files: `ShuffleManager.scala`, `ShuffleFetcher.scala`, `SimpleShuffleFetcher.scala`

**Chapter 15: Serialization — Turning Objects into Bytes**
- Motivation: "Tasks get sent to workers. Results come back. Shuffle data moves over HTTP. All of this requires turning objects into bytes. How?"
- Source: `Serializer.scala`, `JavaSerializer.scala`, `KryoSerializer.scala`
- The Serializer trait — newInstance() returns a thread-safe SerializerInstance
- JavaSerializer: uses Java's built-in ObjectOutputStream (simple, slow)
- KryoSerializer: uses Kryo library (fast, requires registration)
  - Pre-registered types: arrays, tuples, collections, singletons
  - Custom serializers for Scala Maps and singletons (None, Nil)
  - User registration via `spark.kryo.registrator`
- Two serializers in SparkEnv: `serializer` (for data) and `closureSerializer` (for tasks)
- Why closures need special serialization — ClosureCleaner
- Key files: `Serializer.scala`, `JavaSerializer.scala`, `KryoSerializer.scala`

**Chapter 16: ClosureCleaner — Making Functions Serializable**
- Motivation: "When you write `rdd.map(x => x + 1)`, that lambda gets sent to worker machines. But lambdas can capture variables from their enclosing scope. How does Spark handle that?"
- Source: `ClosureCleaner.scala`
- The problem: Scala closures capture `$outer` references to enclosing objects
- If the enclosing object isn't Serializable, the task can't be sent to workers
- ClosureCleaner's solution:
  1. Use ASM bytecode analysis to find which outer fields the closure ACTUALLY uses
  2. Clone the closure chain, nulling out unused fields
  3. Replace the closure's `$outer` with the cleaned clone
- The `getOuterClasses()` / `getOuterObjects()` / `getInnerClasses()` methods
- `FieldAccessFinder` — ASM visitor that tracks GETFIELD instructions
- Why this matters: without ClosureCleaner, many Spark programs would fail with NotSerializableException
- Key files: `ClosureCleaner.scala`

---

### Part 5: The Big Picture

**Chapter 17: Putting It All Together — A Complete Trace**
- Take the word count from Book 1 Chapter 19
- But this time, trace through the SCHEDULER layer, not just the RDD layer
- Step by step:
  1. `sc.runJob()` called
  2. DAGScheduler builds stages (show every `newStage`, `getParentStages` call)
  3. `submitStage()` recursion — Stage 1 submitted first
  4. `submitMissingTasks()` — 3 ShuffleMapTasks created
  5. LocalScheduler runs them in thread pool
  6. Each ShuffleMapTask: iterate RDD, bucket by key, write shuffle files
  7. `taskEnded()` called 3 times — Stage 1 complete
  8. Waiting stages checked — Stage 2 now runnable
  9. `submitMissingTasks()` — 2 ResultTasks created
  10. Each ResultTask: fetch shuffle data, merge in HashMap, apply collect function
  11. `taskEnded()` called 2 times — job complete
  12. Results returned to driver
- Show the complete timeline with all objects created
- Show what happens if a task FAILS midway (fetch failure scenario)

**Chapter 18: What Comes Next**
- Brief chapter — what this book didn't cover and why
- Caching system (Cache, BoundedMemoryCache, CacheTracker) — "Book 3: Understanding Spark's Memory"
- Broadcast variables — how large read-only data gets distributed efficiently
- Accumulators — write-only shared variables for metrics
- The evolution from Spark 0.5.0 to modern Spark — what changed, what stayed the same
- The core insight: the scheduler is a ~250-line event loop that turns a lazy DAG into distributed execution

---

## Source Code Files by Chapter

| Chapter | Primary Files | Lines of Code |
|---------|--------------|---------------|
| 1. SparkContext | `SparkContext.scala` | ~250 |
| 2. SparkEnv | `SparkEnv.scala` | ~50 |
| 3. Scheduler Trait | `Scheduler.scala` | ~20 |
| 4. Stage | `Stage.scala` | ~40 |
| 5. Task | `Task.scala` | ~10 |
| 6. ShuffleMapTask | `ShuffleMapTask.scala` | ~50 |
| 7. ResultTask | `ResultTask.scala` | ~20 |
| 8. DAGScheduler (Stages) | `DAGScheduler.scala` (first half) | ~80 |
| 9. DAGScheduler (RunJob) | `DAGScheduler.scala` (second half) | ~120 |
| 10. LocalScheduler | `LocalScheduler.scala` | ~70 |
| 11. MesosScheduler | `MesosScheduler.scala`, `Executor.scala` | ~250 + 150 |
| 12. SimpleJob | `SimpleJob.scala`, `Job.scala` | ~250 + 15 |
| 13. MapOutputTracker | `MapOutputTracker.scala` | ~130 |
| 14. Shuffle I/O | `ShuffleManager.scala`, `ShuffleFetcher.scala`, `SimpleShuffleFetcher.scala` | ~90 + 10 + 40 |
| 15. Serialization | `Serializer.scala`, `JavaSerializer.scala`, `KryoSerializer.scala` | ~40 + 50 + 200 |
| 16. ClosureCleaner | `ClosureCleaner.scala` | ~170 |
| 17. Full Trace | All of the above | — |
| 18. What's Next | — | — |

---

## Key Principles for This Book

1. **Start from what the reader knows.** They know `rdd.collect()` triggers execution. Start there and follow the code path inward.

2. **One new concept per chapter.** Don't introduce Stage and Task in the same chapter. Don't explain DAGScheduler and LocalScheduler together.

3. **Always show the actual code.** Every chapter centers on a real source file. The reader should be able to open the file and follow along.

4. **Concrete examples, always.** Never say "Task A processes partition X." Say "ShuffleMapTask for partition 0 reads ('apple', 1), ('banana', 1) and writes them to bucket files."

5. **Trace the full call chain.** When explaining `submitStage()`, show every recursive call with actual stage IDs and RDD names.

6. **Connect back to Book 1.** "Remember ShuffleDependency from Chapter 5? That's what the DAGScheduler looks for when splitting stages."

7. **Don't rush the DAGScheduler.** Chapters 8 and 9 are the heart of the book. The `runJob()` event loop deserves a full chapter with line-by-line walkthrough.

8. **The LocalScheduler is the teaching tool.** Cover it before MesosScheduler. It's the same logic without network complexity. The reader can mentally "run" it.

9. **End with a complete trace.** Just like Book 1's Chapter 19, but this time through the scheduler layer. The reader should be able to trace every method call from `collect()` to final result.

10. **Keep the same voice.** Conversational, patient, never condescending. "Let's look at...", "Notice that...", "Great question."
