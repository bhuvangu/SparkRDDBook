# Chapter 8: DAGScheduler — Building Stages

## 8.1 The Central Question

We've seen the pieces: Stages (Chapter 4), Tasks (Chapters 5–7), and the Scheduler architecture (Chapter 3). Now it's time to see how they come together.

When you call `rdd.collect()`, the DAGScheduler faces a question: **"I have a graph of RDD objects connected by dependencies. How do I turn this into stages and tasks that can actually execute?"**

The answer is a graph walk. The DAGScheduler starts at the final RDD and walks backwards through the dependency chain. Every time it hits a ShuffleDependency, it draws a line: "New stage boundary here." Everything connected by NarrowDependencies stays in the same stage.

This chapter covers the stage-building half. Chapter 9 covers the event loop that runs the job.

---

## 8.2 The Algorithm in Plain English

Imagine you're looking at the RDD graph for a word count:

```
HadoopRDD → FlatMappedRDD → MappedRDD ──shuffle──→ ShuffledRDD
```

The DAGScheduler starts at `ShuffledRDD` (the final RDD) and asks: "What are your dependencies?"

ShuffledRDD says: "I have a ShuffleDependency on MappedRDD."

**Stop.** A ShuffleDependency means a stage boundary. Everything on the other side of that shuffle is a separate stage. So the DAGScheduler creates a new stage for MappedRDD and its narrow-dependency ancestors.

Then it asks MappedRDD: "What are your dependencies?" MappedRDD says: "OneToOneDependency on FlatMappedRDD." That's narrow — keep walking. FlatMappedRDD says: "OneToOneDependency on HadoopRDD." Still narrow. HadoopRDD says: "No dependencies — I'm a root." Done.

Result: two stages.

```
Stage 0: HadoopRDD → FlatMappedRDD → MappedRDD    (all narrow)
Stage 1: ShuffledRDD                                (starts after shuffle)
```

The rule is simple: **walk backwards, stop at shuffles.** Everything between two shuffles (or between a shuffle and a root) is one stage.

---

## 8.3 Why Walk Backwards?

You might wonder: why not walk forwards from the source RDDs?

Because the DAGScheduler doesn't know where the sources are. It only knows the final RDD — the one the action was called on. The dependency links point backwards (child → parent), not forwards. So backwards is the natural direction.

This is also the right direction for another reason: the DAGScheduler needs to build the final stage first (because that's the one whose results the user wants), then discover what parent stages it needs. It's demand-driven: "I need this result. What do I need to compute first?"

---

## 8.4 The Key Methods

The DAGScheduler has four methods that work together to build stages. Let's understand each one conceptually before looking at code.

### `getParentStages(rdd)` — "What stages must finish before this one?"

This is the graph walk. Starting from `rdd`, walk backwards through dependencies:
- **NarrowDependency?** Keep walking — same stage.
- **ShuffleDependency?** Stop. Create a stage for the parent RDD. That stage is a "parent stage."

### `getShuffleMapStage(shuffleDep)` — "Give me the stage for this shuffle"

A cache. If we've already created a stage for this shuffle, return it. Otherwise, create a new one. This prevents duplicate stages when multiple RDDs depend on the same shuffle.

### `newStage(rdd, shuffleDep)` — "Create a Stage object"

Assigns a unique ID, registers with the cache tracker and map output tracker, calls `getParentStages(rdd)` to find parent stages, and creates the Stage object.

### `getMissingParentStages(stage)` — "Which parent stages haven't finished?"

Similar to `getParentStages`, but smarter. It checks:
- Is the data cached? If so, skip — no need to recompute.
- Has the parent stage already completed? If so, skip — its output is available.

Only truly missing stages are returned.

---

## 8.5 Tracing the Word Count

Let's trace the complete stage-building process:

```scala
val counts = sc.textFile("data.txt")
    .flatMap(_.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
counts.collect()
```

**Step 1**: `collect()` calls `sc.runJob(ShuffledRDD, ...)`, which calls `DAGScheduler.runJob()`.

**Step 2**: `runJob` creates the final stage: `newStage(ShuffledRDD, None)`.

**Step 3**: Inside `newStage`, `getParentStages(ShuffledRDD)` is called:

```
Walk backwards from ShuffledRDD:
  ShuffledRDD's dependency = ShuffleDependency on MappedRDD
  → STOP! Shuffle boundary.
  → getShuffleMapStage(shuffleDep)
    → Not cached yet → newStage(MappedRDD, Some(shuffleDep))
      → getParentStages(MappedRDD):
          Walk backwards from MappedRDD:
            MappedRDD → NarrowDep → FlatMappedRDD → NarrowDep → HadoopRDD → no deps
          No shuffles found → parents = []
      → Stage 0 created (rdd=MappedRDD, parents=[])
  → parents = [Stage 0]
```

**Step 4**: Final stage created: `Stage 1 (rdd=ShuffledRDD, parents=[Stage 0])`.

**Result**:
```
Stage 0: rdd=MappedRDD, shuffleDep=Some(0), parents=[], numPartitions=3
Stage 1: rdd=ShuffledRDD, shuffleDep=None, parents=[Stage 0], numPartitions=2
```

---

## 8.6 Preferred Locations — Where Should Tasks Run?

When the DAGScheduler creates tasks, it needs to decide: "Where should each task ideally run?" This is data locality — running computation where the data already is.

The `getPreferredLocs` method uses a three-level priority:

**Priority 1: Cache.** "Partition 5 of this RDD is cached on host-B" → run on host-B.

**Priority 2: RDD's own preferences.** HadoopRDD knows which machines have its HDFS blocks. "Partition 3 reads a block on host-A and host-C" → run on host-A or host-C.

**Priority 3: Walk back through narrow dependencies.** MappedRDD doesn't know about HDFS blocks, but its parent FlatMappedRDD doesn't either, but *its* parent HadoopRDD does. The method walks back through the narrow dependency chain until it finds an RDD with location preferences.

```
MappedRDD(partition 3) → no preferences
  → FlatMappedRDD(partition 3) → no preferences
    → HadoopRDD(partition 3) → ["host-A", "host-C"]  ← found!
```

This is how a `map` task inherits the data locality of the HDFS block it ultimately reads from, even though the MappedRDD itself has no idea about HDFS.

---

## 8.7 How Caching Short-Circuits Stage Building

`getMissingParentStages` is smarter than `getParentStages`. When walking backwards, it checks cache locations for each partition. If a partition is cached, there's no need to look at its parents — the data is already available.

This means: if you cache an RDD before a shuffle, and then a failure forces a stage to be resubmitted, the resubmission might find the cached data and skip recomputing from scratch. Caching doesn't just speed up repeated computations — it also speeds up failure recovery.

---

## 8.8 The Code — For Reference

Here's `getParentStages`, the core graph walk. After the conceptual explanation above, it should read clearly:

```scala
def getParentStages(rdd: RDD[_]): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_,_,_] =>
              parents += getShuffleMapStage(shufDep)   // STOP — new stage
            case _ =>
              visit(dep.rdd)                            // CONTINUE — same stage
          }
        }
      }
    }
    visit(rdd)
    parents.toList
}
```

A depth-first walk. ShuffleDependency = stop and create a parent stage. NarrowDependency = keep walking. The `visited` set prevents processing the same RDD twice (important for diamond-shaped graphs).

---

## 8.9 Summary

| Method | What It Does |
|--------|-------------|
| `getParentStages(rdd)` | Walks the RDD graph backwards, creates a stage at each ShuffleDependency |
| `getShuffleMapStage(shuf)` | Returns the stage for a shuffle, creating it if needed (caches to avoid duplicates) |
| `newStage(rdd, shuffleDep)` | Creates a Stage object, registers with trackers, finds parent stages |
| `getMissingParentStages(stage)` | Like getParentStages, but skips cached partitions and already-completed stages |
| `getPreferredLocs(rdd, partition)` | Three-level priority: cache → RDD prefs → parent prefs (recursive) |

| Question | Answer |
|----------|--------|
| How does Spark find stage boundaries? | Walks the RDD graph backwards. ShuffleDependency = new stage. NarrowDependency = same stage. |
| Why walk backwards? | Because dependency links point child → parent, and the DAGScheduler starts from the final RDD. |
| How does caching affect stages? | `getMissingParentStages` skips cached partitions — parent stages may not need to run. |
| How are preferred locations found? | Cache location → RDD's own preferences → walk back through narrow deps to find an ancestor with preferences. |

The stages are built. Now the event loop takes over — that's Chapter 9.

---

**Next Chapter**: [Chapter 9: DAGScheduler — Running the Job →](Chapter-09-DAGScheduler-RunJob.md)
