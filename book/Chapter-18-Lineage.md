# Chapter 18: Lineage — The RDD Graph

Every RDD remembers how it was created. This memory — the complete chain of transformations from the original data source to the current RDD — is called the **lineage**. It's a directed acyclic graph (DAG) of RDD objects, and it's the secret to Spark's fault tolerance.

---

## 18.1 The DAG

When you write:
```scala
val lines = sc.textFile("data.txt")                    // ① HadoopRDD
val words = lines.flatMap(_.split(" "))                // ② FlatMappedRDD
val pairs = words.map(word => (word, 1))               // ③ MappedRDD
val counts = pairs.reduceByKey(_ + _)                  // ④ ShuffledRDD
val big = counts.filter(_._2 > 100)                    // ⑤ FilteredRDD
```

Spark builds this graph **in memory** (no computation yet):

```
① HadoopRDD ──narrow──→ ② FlatMappedRDD ──narrow──→ ③ MappedRDD
                                                          │
                                                      shuffle
                                                          │
                                                          ↓
                                               ④ ShuffledRDD ──narrow──→ ⑤ FilteredRDD
```

Each arrow is a Dependency object. Each node is an RDD object. The graph is a DAG — Directed (arrows point one way), Acyclic (no loops), Graph.

---

## 18.2 Stages — Splitting at Shuffle Boundaries

When you call an action like `big.collect()`, Spark's scheduler walks the DAG and splits it into **stages**:

```
┌─────────────────────────────────────────┐
│ STAGE 1 (all narrow dependencies)       │
│                                         │
│ HadoopRDD → FlatMappedRDD → MappedRDD  │
│                                         │
│ One task per partition. All pipelined.   │
└──────────────────┬──────────────────────┘
                   │
              SHUFFLE BOUNDARY
              (write to disk)
                   │
                   ↓
┌─────────────────────────────────────────┐
│ STAGE 2 (starts after shuffle)          │
│                                         │
│ ShuffledRDD → FilteredRDD              │
│                                         │
│ One task per partition. Pipelined.       │
└─────────────────────────────────────────┘
```

**Rules for stage creation:**
1. Start from the final RDD and walk backwards
2. Every time you hit a **ShuffleDependency**, start a new stage
3. All **NarrowDependencies** within a stage get pipelined together

---

## 18.3 Tasks — One Per Partition Per Stage

Each stage creates **one task per partition**:

```
Stage 1 (3 partitions from HadoopRDD):
  Task 0: Read HDFS block 0 → flatMap → map → write shuffle output
  Task 1: Read HDFS block 1 → flatMap → map → write shuffle output
  Task 2: Read HDFS block 2 → flatMap → map → write shuffle output

Stage 2 (2 partitions from ShuffledRDD):
  Task 0: Fetch shuffle data for partition 0 → filter → return results
  Task 1: Fetch shuffle data for partition 1 → filter → return results
```

Within each task, the pipeline of narrow transformations is **fused** — data streams through flatMap → map without any intermediate storage. This is the beauty of the iterator-based compute chain from Chapter 4.

---

## 18.4 Fault Recovery — Replaying the Lineage

Suppose Machine B crashes, and Stage 2's Task 1 data is lost. How does Spark recover?

**Step 1**: Check if ShuffledRDD partition 1 is cached → No

**Step 2**: Check if the shuffle output files for partition 1 are on disk → Yes, on Machines A, B, C (but Machine B is dead)

**Step 3**: Re-fetch shuffle data from Machines A and C. Machine B's portion is missing.

**Step 4**: Need to recompute Machine B's Stage 1 output. Look at the lineage:
- `MappedRDD` partition 1 ← `FlatMappedRDD` partition 1 ← `HadoopRDD` partition 1
- All narrow dependencies! Only need to recompute **one parent partition**.

**Step 5**: Recompute HadoopRDD partition 1 on another machine → pipeline through flatMap → map → write shuffle output → re-fetch.

**Step 6**: Stage 2 Task 1 can now proceed.

```
Recovery plan:

                    ✓ ok        ✓ ok        ✗ LOST      
Stage 1 results:  [Machine A] [Machine B] [Machine C]
                      │            │            │
                      │         RECOMPUTE       │
                      │         on Machine D    │
                      │            │            │
                      ↓            ↓            ↓
Stage 2 Task 1:  ←──fetch────←──fetch────←──fetch────→ merged result
```

**Key insight**: For narrow dependencies, only the specific lost partition needs recomputation. The lineage tells Spark exactly which parent partition to replay.

For shuffle dependencies, recovery is more expensive — it may require recomputing multiple parent partitions. This is why caching is especially valuable before shuffles.

---

## 18.5 Caching and the Lineage

When you call `.cache()`, you're adding a **checkpoint** in the lineage:

```
HadoopRDD → FlatMappedRDD → MappedRDD.cache() → ShuffledRDD → FilteredRDD
                                  ↑
                          Cached in memory
```

Now if `FilteredRDD` partition 0 is lost:
1. Need `ShuffledRDD` partition 0 → need shuffle data
2. Need `MappedRDD` partitions → **found in cache!** Don't need to go back to disk.

Without caching, Spark would need to re-read from HDFS and replay the entire pipeline. Caching short-circuits the lineage.

---

## 18.6 The Complete Mental Model

```
┌───────────────────────────────────────────────────────────────┐
│                      USER'S CODE                               │
│                                                                │
│  val a = sc.textFile(...)                                     │
│  val b = a.flatMap(...)        ← Builds DAG (lazy)            │
│  val c = b.reduceByKey(...)                                   │
│  val d = c.filter(...)                                        │
│  d.collect()                   ← Triggers execution (eager)   │
│                                                                │
├───────────────────────────────────────────────────────────────┤
│                      SPARK SCHEDULER                           │
│                                                                │
│  1. Walk the DAG backwards from 'd'                           │
│  2. Find shuffle boundaries → create stages                   │
│  3. For each stage: create tasks (one per partition)           │
│  4. Respect preferred locations (data locality)               │
│  5. Submit tasks to workers                                    │
│                                                                │
├───────────────────────────────────────────────────────────────┤
│                      WORKER MACHINES                           │
│                                                                │
│  Stage 1 tasks: read → transform → write shuffle files        │
│  Stage 2 tasks: fetch shuffle → transform → return results    │
│                                                                │
│  If a task fails: scheduler re-runs it on another machine     │
│  using the lineage to recompute any lost data                 │
│                                                                │
├───────────────────────────────────────────────────────────────┤
│                      DRIVER MACHINE                            │
│                                                                │
│  Receives final results from all partitions                   │
│  Combines them (e.g., concatenation for collect)              │
│  Returns to user                                              │
└───────────────────────────────────────────────────────────────┘
```

---

## 18.7 Summary

| Concept | What It Is |
|---------|-----------|
| **Lineage** | The complete chain of RDD dependencies from source to current RDD |
| **DAG** | The graph of all RDDs and their dependencies |
| **Stage** | A group of narrow-dependency RDDs that can be pipelined together |
| **Stage boundary** | A shuffle dependency — requires data exchange between stages |
| **Task** | One unit of work: compute one partition of one stage |
| **Fault recovery** | Re-execute the lineage for lost partitions. Narrow deps = cheap. Shuffle deps = expensive. |
| **Caching** | Short-circuits the lineage — avoids replaying from the beginning |

The lineage is what makes RDDs **resilient**. It's not redundant copies of data (like HDFS replication). It's redundant **knowledge of how to recreate the data**. The recipe is the backup plan.

---

**Next Chapter**: [Chapter 19: Putting It All Together — A Full Example →](Chapter-19-Full-Example.md)
