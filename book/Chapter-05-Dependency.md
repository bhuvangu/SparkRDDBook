# Chapter 5: Dependency — Who Is Your Parent?

You now know that an RDD has **splits** (partitions) and a **compute** function (a recipe for each partition). But there's a question we haven't fully answered: **where does the input data for `compute()` come from?**

For a `HadoopRDD`, it comes from a file. But for a `MappedRDD`, it comes from another RDD — its **parent**. And that parent might have its own parent. This creates a chain:

```
HadoopRDD  →  FilteredRDD  →  MappedRDD  →  ShuffledRDD  →  MappedRDD
```

This chain is recorded through **dependencies**. A Dependency says: "This RDD was created from *that* RDD."

---

## 5.1 Why Dependencies Matter

Dependencies serve two critical purposes:

### Purpose 1: Scheduling — What to compute and in what order

When Spark needs to compute `rdd3`, it looks at the dependencies to figure out: "I need `rdd2` first, and for that I need `rdd1` first." It builds an execution plan.

### Purpose 2: Fault Tolerance — Recovering lost data

If a machine crashes and a partition of `rdd3` is lost, Spark doesn't panic. It looks at the dependencies and says: "I can recompute that partition of `rdd3` by getting the corresponding partition from `rdd2` and applying the map function." If that partition of `rdd2` is also lost, it goes further back: "I can recompute `rdd2` by getting data from `rdd1` and applying the filter."

This chain of dependencies is called the **lineage**. It's the RDD's recovery plan.

---

## 5.2 The Two Kinds of Dependencies

This is the most important concept in this chapter. Not all dependencies are created equal.

### Narrow Dependency: "I know exactly which parent partition I need"

A `MappedRDD`'s partition 3 only needs data from its parent's partition 3. A `FilteredRDD`'s partition 7 only needs its parent's partition 7. There's a **direct, known mapping** between child and parent partitions.

```
Narrow Dependency (one-to-one):

Parent RDD          Child RDD (MappedRDD)
┌──────────┐        ┌──────────┐
│ Split 0  │───────→│ Split 0  │
└──────────┘        └──────────┘
┌──────────┐        ┌──────────┐
│ Split 1  │───────→│ Split 1  │
└──────────┘        └──────────┘
┌──────────┐        ┌──────────┐
│ Split 2  │───────→│ Split 2  │
└──────────┘        └──────────┘

Each child partition needs exactly one parent partition.
```

### Shuffle (Wide) Dependency: "I need data from ALL parent partitions"

A `ShuffledRDD`'s partition 0 needs data from **every** parent partition. Why? Because after a `reduceByKey`, keys that were spread across all partitions need to be gathered together. Key "apple" might be in parent partition 0, 3, and 7 — and they all need to end up in the same child partition.

```
Shuffle Dependency (all-to-all):

Parent RDD              Child RDD (ShuffledRDD)
┌──────────┐        ┌──────────┐
│ Split 0  │───┬───→│ Split 0  │
└──────────┘   │    └──────────┘
               │ ╲
┌──────────┐   │  ╲  ┌──────────┐
│ Split 1  │───┼───→│ Split 1  │
└──────────┘   │  ╱  └──────────┘
               │ ╱
┌──────────┐   │    ┌──────────┐
│ Split 2  │───┴───→│ Split 2  │
└──────────┘        └──────────┘

Each child partition needs data from EVERY parent partition.
(Lines cross — every parent sends to every child.)
```

### Why This Distinction Matters

| | Narrow Dependency | Shuffle Dependency |
|---|---|---|
| **Data movement** | None — data stays on the same machine | Data moves across the network (expensive!) |
| **Can be pipelined** | Yes — map + filter + map are all processed element-by-element | No — must wait for all parent data to arrive |
| **Recovery cost** | Cheap — recompute just the one parent partition needed | Expensive — may need to recompute many parent partitions |
| **Stage boundaries** | All narrow dependencies within a stage get pipelined together | A shuffle creates a new stage boundary |

---

## 5.3 The Actual Code: `Dependency.scala`

Let's look at the source code (`spark-0.5.0/core/src/main/scala/spark/Dependency.scala`):

```scala
abstract class Dependency[T](val rdd: RDD[T], val isShuffle: Boolean) extends Serializable
```

**In Java terms:**
```java
public abstract class Dependency<T> implements Serializable {
    private final RDD<T> rdd;        // the parent RDD
    private final boolean isShuffle;  // is this a shuffle dependency?
    
    public Dependency(RDD<T> rdd, boolean isShuffle) {
        this.rdd = rdd;
        this.isShuffle = isShuffle;
    }
    
    public RDD<T> getRdd() { return rdd; }
    public boolean isShuffle() { return isShuffle; }
}
```

Every dependency knows two things:
1. **`rdd`** — which parent RDD this dependency points to
2. **`isShuffle`** — whether data needs to be shuffled (redistributed across the network)

Now let's look at the subclasses:

---

## 5.4 NarrowDependency

```scala
abstract class NarrowDependency[T](rdd: RDD[T]) extends Dependency(rdd, false) {
  def getParents(outputPartition: Int): Seq[Int]
}
```

**In Java terms:**
```java
public abstract class NarrowDependency<T> extends Dependency<T> {
    
    public NarrowDependency(RDD<T> rdd) {
        super(rdd, false);  // isShuffle = false
    }
    
    /**
     * Given a child partition index, which parent partition(s) does it need?
     */
    public abstract List<Integer> getParents(int outputPartition);
}
```

A `NarrowDependency` says `isShuffle = false` and requires a `getParents()` method. This method answers: "If I need to compute child partition #X, which parent partition(s) do I need?"

There are two concrete implementations:

### OneToOneDependency

```scala
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int) = List(partitionId)
}
```

**In Java terms:**
```java
public class OneToOneDependency<T> extends NarrowDependency<T> {
    
    public OneToOneDependency(RDD<T> rdd) {
        super(rdd);
    }
    
    public List<Integer> getParents(int partitionId) {
        return List.of(partitionId);  // partition 3 needs parent partition 3
    }
}
```

This is the simplest dependency: child partition N depends on parent partition N. Same partition number.

**Used by**: `MappedRDD`, `FilteredRDD`, `FlatMappedRDD`, `SampledRDD`, `PipedRDD`, `GlommedRDD`, `MapPartitionsRDD`, `SortedRDD`, `MappedValuesRDD`, `FlatMappedValuesRDD` — basically every simple transformation.

### RangeDependency

```scala
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {
  
  override def getParents(partitionId: Int) = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
```

**In Java terms:**
```java
public class RangeDependency<T> extends NarrowDependency<T> {
    private int inStart;   // starting partition index in the parent
    private int outStart;  // starting partition index in the child
    private int length;    // how many partitions
    
    public RangeDependency(RDD<T> rdd, int inStart, int outStart, int length) {
        super(rdd);
        this.inStart = inStart;
        this.outStart = outStart;
        this.length = length;
    }
    
    public List<Integer> getParents(int partitionId) {
        if (partitionId >= outStart && partitionId < outStart + length) {
            return List.of(partitionId - outStart + inStart);
        } else {
            return List.of();  // this parent doesn't contribute to this partition
        }
    }
}
```

This is used by `UnionRDD`. When you union two RDDs, the child has all partitions from both parents. The RangeDependency maps a range of child partitions back to the right parent.

Let's trace through an example:

```
RDD A has 3 splits: [0, 1, 2]
RDD B has 2 splits: [0, 1]

UnionRDD = A ∪ B has 5 splits: [0, 1, 2, 3, 4]

Dependencies:
  - RangeDependency(A, inStart=0, outStart=0, length=3)
    → Union splits 0,1,2 come from A's splits 0,1,2
    
  - RangeDependency(B, inStart=0, outStart=3, length=2)
    → Union splits 3,4 come from B's splits 0,1

Querying getParents():
  - getParents(0) on A's dep → 0  (Union split 0 = A's split 0)
  - getParents(1) on A's dep → 1  (Union split 1 = A's split 1)
  - getParents(2) on A's dep → 2  (Union split 2 = A's split 2)
  - getParents(3) on B's dep → 0  (Union split 3 = B's split 0)
  - getParents(4) on B's dep → 1  (Union split 4 = B's split 1)
```

---

## 5.5 ShuffleDependency

```scala
class ShuffleDependency[K, V, C](
    val shuffleId: Int,
    rdd: RDD[(K, V)],
    val aggregator: Aggregator[K, V, C],
    val partitioner: Partitioner)
  extends Dependency(rdd, true)
```

**In Java terms:**
```java
public class ShuffleDependency<K, V, C> extends Dependency<Pair<K, V>> {
    private final int shuffleId;
    private final Aggregator<K, V, C> aggregator;
    private final Partitioner partitioner;
    
    public ShuffleDependency(int shuffleId, RDD<Pair<K, V>> rdd, 
                              Aggregator<K, V, C> aggregator, Partitioner partitioner) {
        super(rdd, true);  // isShuffle = true
        this.shuffleId = shuffleId;
        this.aggregator = aggregator;
        this.partitioner = partitioner;
    }
}
```

A `ShuffleDependency` says `isShuffle = true` and carries extra information:
- **`shuffleId`**: A unique ID for this shuffle operation (so the system can track shuffle data)
- **`aggregator`**: How to combine values for the same key (we'll explore this in Chapter 13)
- **`partitioner`**: How to decide which key goes to which output partition (Chapter 6)

Notice: There's no `getParents()` method here. A shuffle dependency doesn't map specific parent partitions to specific child partitions. Instead, **every child partition potentially needs data from every parent partition.** The data routing is determined by the `partitioner` — a key's hash code determines which output partition it goes to.

**Used by**: `ShuffledRDD`, and some dependencies in `CoGroupedRDD`.

---

## 5.6 How RDDs Declare Their Dependencies

Every RDD has a `dependencies` field. Let's see how different RDDs set theirs:

### MappedRDD — One parent, OneToOne
```scala
class MappedRDD[U, T](prev: RDD[T], f: T => U) extends RDD[U](prev.context) {
  override val dependencies = List(new OneToOneDependency(prev))
  // ...
}
```

"I depend on one parent RDD (`prev`), and my partition N comes from its partition N."

### HadoopRDD — No parents
```scala
class HadoopRDD[K, V](...) extends RDD[(K, V)](sc) {
  override val dependencies: List[Dependency[_]] = Nil
  // ...
}
```

"I have no parents. I'm a root RDD — I read data from external storage."

### UnionRDD — Multiple parents, RangeDependency
```scala
class UnionRDD[T](sc: SparkContext, rdds: Seq[RDD[T]]) extends RDD[T](sc) {
  override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for ((rdd, index) <- rdds.zipWithIndex) {
      deps += new RangeDependency(rdd, 0, pos, rdd.splits.size)
      pos += rdd.splits.size
    }
    deps.toList
  }
  // ...
}
```

"I depend on multiple parent RDDs. Each parent's partitions map to a range of my partitions."

### ShuffledRDD — One parent, ShuffleDependency
```scala
class ShuffledRDD[K, V, C](parent: RDD[(K, V)], aggregator: Aggregator[K, V, C], part: Partitioner)
  extends RDD[(K, C)](parent.context) {
  
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)
  // ...
}
```

"I depend on one parent RDD, but the dependency is a shuffle — data must be redistributed across the network."

### CartesianRDD — Two parents, custom NarrowDependencies
```scala
class CartesianRDD[T, U](sc: SparkContext, rdd1: RDD[T], rdd2: RDD[U])
  extends RDD[Pair[T, U]](sc) {
  
  val numSplitsInRdd2 = rdd2.splits.size
  
  override val dependencies = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numSplitsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numSplitsInRdd2)
    }
  )
}
```

This is clever! For a cross product, partition `id` in the CartesianRDD maps to:
- `rdd1`'s partition `id / numSplitsInRdd2` (integer division)
- `rdd2`'s partition `id % numSplitsInRdd2` (modulo)

Example with `rdd1` having 2 splits and `rdd2` having 3 splits:

```
CartesianRDD splits: 2 × 3 = 6

Split 0 → rdd1[0/3=0], rdd2[0%3=0]  → (rdd1 split 0, rdd2 split 0)
Split 1 → rdd1[1/3=0], rdd2[1%3=1]  → (rdd1 split 0, rdd2 split 1)
Split 2 → rdd1[2/3=0], rdd2[2%3=2]  → (rdd1 split 0, rdd2 split 2)
Split 3 → rdd1[3/3=1], rdd2[3%3=0]  → (rdd1 split 1, rdd2 split 0)
Split 4 → rdd1[4/3=1], rdd2[4%3=1]  → (rdd1 split 1, rdd2 split 1)
Split 5 → rdd1[5/3=1], rdd2[5%3=2]  → (rdd1 split 1, rdd2 split 2)
```

Still narrow! Each child partition knows exactly which two parent partitions it needs.

### CoGroupedRDD — Smart choice between narrow and shuffle
```scala
class CoGroupedRDD[K](rdds: Seq[RDD[(_, _)]], part: Partitioner) extends RDD[...] {
  override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    for ((rdd, index) <- rdds.zipWithIndex) {
      if (rdd.partitioner == Some(part)) {
        deps += new OneToOneDependency(rdd)       // already partitioned correctly — no shuffle needed!
      } else {
        deps += new ShuffleDependency(...)         // needs shuffle
      }
    }
    deps.toList
  }
}
```

This is an optimization! If a parent RDD is already partitioned the same way the CoGroupedRDD wants, Spark uses a narrow dependency (no shuffle). Only parents with different partitioning need to be shuffled. We'll explore this in Chapter 14.

---

## 5.7 Visualizing the Dependency Graph

Let's trace a real example:

```scala
val lines = sc.textFile("data.txt")                    // HadoopRDD
val words = lines.flatMap(_.split(" "))                // FlatMappedRDD
val pairs = words.map(word => (word, 1))               // MappedRDD
val counts = pairs.reduceByKey(_ + _)                  // ShuffledRDD
val filtered = counts.filter(_._2 > 10)               // FilteredRDD
```

The dependency graph looks like:

```
HadoopRDD ──narrow──→ FlatMappedRDD ──narrow──→ MappedRDD ──shuffle──→ ShuffledRDD ──narrow──→ FilteredRDD
  (lines)               (words)                  (pairs)               (counts)                (filtered)
                                                          ↑
                                                   STAGE BOUNDARY
                                                  (data must be
                                                   redistributed)
```

The shuffle dependency creates a **stage boundary**. Spark groups narrow dependencies into stages that can be pipelined:

- **Stage 1**: HadoopRDD → FlatMappedRDD → MappedRDD (all narrow, all pipelined)
- **Stage 2**: ShuffledRDD → FilteredRDD (narrow, pipelined)

Stage 1 must complete before Stage 2 can start, because Stage 2 needs the shuffled data.

---

## 5.8 How Dependencies Enable Fault Recovery

Imagine Machine 3 crashes, and the data for `FilteredRDD` partition 5 is lost. Here's how Spark recovers:

1. Look at `FilteredRDD`'s dependency: **OneToOneDependency** on `ShuffledRDD`
2. Need `ShuffledRDD` partition 5. Is it cached? Let's say yes → read from cache. Done!

But what if `ShuffledRDD` partition 5 is also lost?

3. `ShuffledRDD` has a **ShuffleDependency** on `MappedRDD`. To recompute, need shuffle data.
4. Check if the shuffle output files are still on disk (Spark writes shuffle data to disk). If yes → re-read them.
5. If the shuffle files are also lost, need to recompute `MappedRDD` partitions and redo the shuffle.

For narrow dependencies, only the **specific lost partition** needs recomputing. For shuffle dependencies, potentially **multiple parent partitions** need recomputing. That's why narrow dependencies are cheaper to recover from.

---

## 5.9 Summary

| Question | Answer |
|----------|--------|
| What is a Dependency? | A link from a child RDD to a parent RDD, recording "I was created from this." |
| What does it contain? | A reference to the parent RDD, and whether it's a shuffle. |
| What is a NarrowDependency? | A dependency where each child partition needs a known, small set of parent partitions. No data shuffle needed. |
| What is a OneToOneDependency? | The simplest narrow dependency: child partition N → parent partition N. Used by map, filter, flatMap. |
| What is a RangeDependency? | A narrow dependency where a range of child partitions maps to a range of parent partitions. Used by UnionRDD. |
| What is a ShuffleDependency? | A dependency where each child partition potentially needs data from ALL parent partitions. Requires network data transfer. |
| Why are there two kinds? | For scheduling (stages), performance (pipelining), and fault recovery (narrow = cheap recovery, shuffle = expensive). |
| What is lineage? | The entire chain of dependencies from the final RDD back to the original data sources. It's the RDD's "family tree" and recovery plan. |

We now have three of the five RDD properties: **splits**, **compute**, and **dependencies**. Next, we'll look at the fourth: **partitioner** — which is specifically about how key-value data gets distributed across partitions.

---

**Next Chapter**: [Chapter 6: Partitioner — How Keys Get Assigned to Partitions →](Chapter-06-Partitioner.md)
