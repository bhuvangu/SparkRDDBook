# Chapter 4: Stage — A Group of Pipelined Tasks

## 4.1 What We Know So Far

From Chapter 1, we know the scheduler splits the RDD graph into stages at shuffle boundaries. From Chapter 3, we know the DAGScheduler builds stages and creates tasks.

But what IS a stage, concretely? Not as a box in a diagram — as an actual object that the scheduler creates and manages. What does it need to track? Let's think about it from first principles.

---

## 4.2 Designing a Stage — What Would You Need?

Imagine you're the DAGScheduler. You've just split the word count RDD graph into two groups:

```
Group 1: HadoopRDD → FlatMappedRDD → MappedRDD    (before the shuffle)
Group 2: ShuffledRDD                                (after the shuffle)
```

You need to manage these groups. For each one, what do you need to know?

**Identity.** You need a way to refer to each group. "Stage 0" and "Stage 1." A unique ID.

**What to compute.** Which RDD chain does this group represent? You need a reference to the RDD. But which one — the first or the last in the chain?

Think about it. When a task runs, it calls `rdd.iterator(split)`. If you point to the *last* RDD (MappedRDD), calling `iterator` automatically chains backwards through FlatMappedRDD and HadoopRDD — the entire pipeline executes. If you pointed to the *first* RDD (HadoopRDD), you'd only get the raw file data, not the flatMapped and mapped version.

So the stage stores the **last** RDD. The chain takes care of itself.

**What kind of stage is it?** Group 1 exists to produce shuffle files for the next stage. Group 2 exists to produce final results for the user. These are fundamentally different:

| | Group 1 (Shuffle Map Stage) | Group 2 (Result Stage) |
|---|---|---|
| Purpose | Write shuffle files | Return results to driver |
| What tasks return | A server URI ("my files are here") | The actual computed value |
| Needs a shuffle dependency? | Yes — it needs to know the partitioner, aggregator, etc. | No |

**Who must finish first.** Group 2 can't start until Group 1 is done (because Group 2 needs the shuffle files that Group 1 produces). So Group 2 needs a list of parent stages.

**Progress tracking.** With 16,000 partitions, you need to know: which partitions have been computed? Where are their outputs? Is the stage done yet?

---

## 4.3 The Actual Stage Object

With that reasoning, the Stage class almost writes itself:

```scala
class Stage(
    val id: Int,                                      // Identity
    val rdd: RDD[_],                                  // The last RDD in the chain
    val shuffleDep: Option[ShuffleDependency[_,_,_]], // The shuffle it feeds (None for result stage)
    val parents: List[Stage])                          // Parent stages that must finish first
```

Plus tracking:

```scala
val isShuffleMap = shuffleDep != None
val numPartitions = rdd.splits.size
val outputLocs = Array.fill[List[String]](numPartitions)(Nil)
var numAvailableOutputs = 0
```

`outputLocs` is an array with one slot per partition. When ShuffleMapTask for partition 0 completes on host-A, the DAGScheduler records: `outputLocs(0) = List("host-A")`. When all slots are filled, the stage is done.

Why a *list* per slot? Because if a task is re-executed after a failure, the output might exist on multiple machines. The list tracks all known locations.

---

## 4.4 `isAvailable` — Can the Next Stage Start?

The DAGScheduler constantly asks: "Is this parent stage done yet?" The answer comes from `isAvailable`:

```scala
def isAvailable: Boolean = {
    if (parents.size == 0 && !isShuffleMap) true
    else numAvailableOutputs == numPartitions
}
```

A shuffle map stage is available when every partition has at least one output location. A result stage with no parents is always available (it reads from external storage).

Let's trace this for the word count (Stage 0 has 3 partitions):

```
t=0: numAvailableOutputs = 0. isAvailable = false.
t=1: Partition 0 completes. numAvailableOutputs = 1. Still false.
t=2: Partition 1 completes. numAvailableOutputs = 2. Still false.
t=3: Partition 2 completes. numAvailableOutputs = 3. isAvailable = TRUE!
     → Stage 1 can now start.
```

---

## 4.5 When a Machine Dies

When a machine crashes, its shuffle output is lost. The stage has `removeOutputLoc` to handle this:

```scala
def removeOutputLoc(partition: Int, host: String) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_ == host)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil)
      numAvailableOutputs -= 1
}
```

If a partition loses *all* its output locations, `numAvailableOutputs` drops, `isAvailable` becomes `false`, and the DAGScheduler knows: "This stage needs to be partially re-executed — only the lost partitions."

---

## 4.6 A Concrete Example

For the word count with 3 HDFS blocks and `HashPartitioner(2)`:

```
Stage 0 (Shuffle Map Stage):
  id = 0
  rdd = MappedRDD              ← last RDD before the shuffle
  shuffleDep = Some(...)        ← the shuffle this stage feeds
  parents = []                  ← no parents — reads from HDFS
  numPartitions = 3             ← 3 HDFS blocks = 3 tasks

         │ shuffle boundary
         ▼

Stage 1 (Result Stage):
  id = 1
  rdd = ShuffledRDD             ← the final RDD
  shuffleDep = None              ← this is the result stage
  parents = [Stage 0]            ← must wait for Stage 0
  numPartitions = 2              ← HashPartitioner(2) = 2 tasks
```

For a program with two shuffles (like `reduceByKey` followed by `sortByKey`), you'd get three stages chained: Stage 0 → Stage 1 → Stage 2.

---

## 4.7 Summary

| Question | Answer |
|----------|--------|
| What is a Stage? | A group of RDDs connected by narrow dependencies, executed as a unit. |
| What are the two kinds? | Shuffle Map Stage (writes shuffle files) and Result Stage (returns results to driver). |
| Why does `rdd` point to the last RDD? | Because calling `iterator()` on it chains through the whole pipeline automatically. |
| What is `outputLocs`? | An array tracking which hosts have the output for each partition. |
| What is `isAvailable`? | Returns true when all partitions have been computed. The DAGScheduler uses this to decide when the next stage can start. |
| How many tasks does a stage have? | One per partition (`numPartitions`). |

A Stage is the unit of scheduling. But within a stage, the actual work is done by **Tasks** — self-contained packages that can be shipped to remote machines. Let's look at those next.

---

**Next Chapter**: [Chapter 5: Task — The Unit of Work →](Chapter-05-Task.md)
