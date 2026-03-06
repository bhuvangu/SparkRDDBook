# Chapter 13: ShuffledRDD — The Network Shuffle

This is the **most important chapter in Part 4**. The shuffle is what makes distributed computing hard. Every time you do a `reduceByKey`, `groupByKey`, `join`, or `sortByKey`, a shuffle happens underneath. Understanding `ShuffledRDD` is understanding the heart of distributed data processing.

---

## 13.1 Why Do We Need a Shuffle?

Remember the word count example from Chapter 1?

```scala
val words = sc.textFile("data.txt").flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val counts = pairs.reduceByKey(_ + _)
```

After `pairs`, we have data like this across 3 partitions:

```
Partition 0: ("apple", 1), ("banana", 1), ("cherry", 1)
Partition 1: ("apple", 1), ("banana", 1), ("dog", 1)
Partition 2: ("cherry", 1), ("apple", 1), ("dog", 1)
```

To sum up counts for "apple", we need all three `("apple", 1)` entries to be on the **same machine**. But they're on three different machines!

**The shuffle redistributes data across the network so that all values for the same key end up in the same partition.**

```
After shuffle (HashPartitioner with 2 partitions):

Partition 0: ("banana", 1), ("cherry", 1), ("cherry", 1), ("dog", 1), ("dog", 1)
Partition 1: ("apple", 1), ("apple", 1), ("apple", 1)

Now reduceByKey can work locally within each partition:
Partition 0: ("banana", 1), ("cherry", 2), ("dog", 2)
Partition 1: ("apple", 3)
```

---

## 13.2 The Aggregator — Three Functions

Before we look at ShuffledRDD itself, we need to understand the `Aggregator`. It defines **how to combine values for the same key**. It has three functions:

```scala
class Aggregator[K, V, C](
    val createCombiner: V => C,
    val mergeValue: (C, V) => C,
    val mergeCombiners: (C, C) => C)
```

**In Java terms:**
```java
public class Aggregator<K, V, C> {
    Function<V, C> createCombiner;       // Turn a single value into a combiner
    BiFunction<C, V, C> mergeValue;      // Add a value to an existing combiner
    BiFunction<C, C, C> mergeCombiners;  // Merge two combiners together
}
```

### Example: reduceByKey(_ + _) (summing integers)

```
K = String (the word)
V = Int (the count, always 1)
C = Int (the running sum)

createCombiner: v => v                    // first value seen: the combiner IS the value
                                          // e.g., first "apple" → combiner = 1

mergeValue: (c, v) => c + v              // add another value to the combiner
                                          // e.g., combiner=1 + value=1 → combiner=2

mergeCombiners: (c1, c2) => c1 + c2     // merge two combiners from different machines
                                          // e.g., combiner=2 + combiner=1 → combiner=3
```

### Example: groupByKey (collecting values into a list)

```
K = String (the word)
V = Int (the count)
C = ArrayBuffer[Int] (list of all counts)

createCombiner: v => ArrayBuffer(v)               // first value: start a list
mergeValue: (buf, v) => buf += v                   // add value to list
mergeCombiners: (b1, b2) => b1 ++= b2             // concatenate two lists
```

### Why three functions?

This is a critical optimization. Instead of sending all raw values across the network and grouping them at the destination, Spark can **combine values locally first** (on the map side), then send the smaller combined results. This is like the "Combiner" in MapReduce.

```
Without local combining:
  Machine A sends: ("apple", 1), ("apple", 1), ("apple", 1)  → 3 records over network

With local combining (createCombiner + mergeValue):
  Machine A locally combines: ("apple", 1+1+1) = ("apple", 3)
  Machine A sends: ("apple", 3)  → 1 record over network!

Then at destination (mergeCombiners):
  ("apple", 3) from Machine A + ("apple", 2) from Machine B = ("apple", 5)
```

---

## 13.3 The ShuffledRDD Class

Now let's look at the actual code (`ShuffledRDD.scala`):

```scala
class ShuffledRDD[K, V, C](
    parent: RDD[(K, V)],
    aggregator: Aggregator[K, V, C],
    part : Partitioner) 
  extends RDD[(K, C)](parent.context) {

  override val partitioner = Some(part)
  
  @transient
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil
  
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    def mergePair(k: K, c: C) {
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, c)
      } else {
        combiners.put(k, aggregator.mergeCombiners(oldC, c))
      }
    }
    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[K, C](dep.shuffleId, split.index, mergePair)
    return new Iterator[(K, C)] {
      var iter = combiners.entrySet().iterator()
      def hasNext(): Boolean = iter.hasNext()
      def next(): (K, C) = {
        val entry = iter.next()
        (entry.getKey, entry.getValue)
      }
    }
  }
}
```

Let's walk through each property.

---

## 13.4 Property ①: Splits — Brand New

```scala
val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))
```

ShuffledRDD creates **completely new** splits — it doesn't inherit from its parent. The number of splits equals the number of partitions requested by the Partitioner.

```
If part = HashPartitioner(4):
  splits = [ShuffledRDDSplit(0), ShuffledRDDSplit(1), ShuffledRDDSplit(2), ShuffledRDDSplit(3)]
```

These splits are simple — just an index, nothing else. The data for each split comes from the network, not from a file.

---

## 13.5 Property ②: Compute — Fetch and Merge

This is the core of the shuffle. Let's break it down:

```java
// Java translation of ShuffledRDD.compute()
public Iterator<Pair<K, C>> compute(Split split) {
    
    // Step 1: Create a HashMap to store merged results
    HashMap<K, C> combiners = new HashMap<>();
    
    // Step 2: Define how to merge incoming data
    // When a (key, combiner) pair arrives from the network:
    //   - If we haven't seen this key before: just store it
    //   - If we have: merge the new combiner with the existing one
    BiConsumer<K, C> mergePair = (k, c) -> {
        C existing = combiners.get(k);
        if (existing == null) {
            combiners.put(k, c);
        } else {
            combiners.put(k, aggregator.mergeCombiners(existing, c));
        }
    };
    
    // Step 3: Fetch data from the network
    // This contacts all map-side tasks and pulls the data
    // destined for this partition (split.index)
    shuffleFetcher.fetch(dep.shuffleId, split.index, mergePair);
    
    // Step 4: Return an iterator over the merged HashMap
    return combiners.entrySet().stream()
        .map(e -> new Pair<>(e.getKey(), e.getValue()))
        .iterator();
}
```

### The Full Shuffle Data Flow

Here's the complete picture of how data flows during a shuffle:

```
MAP SIDE (Stage 1)                         REDUCE SIDE (Stage 2)
─────────────                              ────────────────

Parent Partition 0:                        ShuffledRDD Partition 0:
  ("apple",1) → hash%2=1 ──────────┐
  ("banana",1) → hash%2=0 ─┐       │        ┌─→ HashMap:
  ("cherry",1) → hash%2=0 ─┤       │        │   "banana" → 2
                            │       │        │   "cherry" → 2
Parent Partition 1:         │       │        │   "dog" → 2
  ("apple",1) → hash%2=1 ──┼───────┤        │
  ("banana",1) → hash%2=0 ─┤       │        │
  ("dog",1) → hash%2=0 ────┤       │    fetch(shuffleId, partition=0, mergePair)
                            │       │        │
Parent Partition 2:         │       │        │
  ("cherry",1) → hash%2=0 ─┤       │        │
  ("apple",1) → hash%2=1 ──┼───────┤
  ("dog",1) → hash%2=0 ────┘       │
                                    │     ShuffledRDD Partition 1:
                                    │
                                    └─→ HashMap:
                                         "apple" → 3

                    ↑                              ↑
            Data written to                 Data fetched from
            shuffle files                   shuffle files
            on local disk                   over the network
```

---

## 13.6 Property ③: Dependencies — ShuffleDependency

```scala
val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
override val dependencies = List(dep)
```

A single `ShuffleDependency` pointing to the parent. This tells Spark:
- **isShuffle = true** — a new stage boundary must be created
- **shuffleId** — a unique ID to track the shuffle files
- **aggregator** — how to combine values
- **partitioner** — which key goes to which output partition

---

## 13.7 Property ④: Partitioner — Set!

```scala
override val partitioner = Some(part)
```

ShuffledRDD is one of the few RDDs that **has a partitioner**. After a shuffle, Spark knows exactly how keys are distributed. This is crucial for subsequent operations — if you do another `reduceByKey` or a `join`, Spark can check: "Is the data already partitioned correctly?"

---

## 13.8 Property ⑤: Preferred Locations — None

```scala
override def preferredLocations(split: Split) = Nil
```

ShuffledRDD has no location preference. The data comes from the network (from all map-side machines), not from a specific file on a specific machine.

---

## 13.9 The Complete Shuffle Timeline

Let's trace a `reduceByKey` end-to-end:

```
val pairs = sc.textFile("data.txt").flatMap(_.split(" ")).map(w => (w, 1))
val counts = pairs.reduceByKey(_ + _)
counts.collect()
```

**Step 1: Stage 1 runs (map side)**
- Each task reads its file block, splits into words, creates (word, 1) pairs
- For each pair, the Partitioner decides which output partition the key belongs to
- The Aggregator's `createCombiner` and `mergeValue` combine values locally
- Results are written to local shuffle files on disk

**Step 2: Shuffle data transfer**
- Each Stage 2 task contacts all Stage 1 machines
- "Give me all the data you wrote for partition X of shuffle Y"
- Data travels over the network

**Step 3: Stage 2 runs (reduce side)**
- `ShuffledRDD.compute()` calls `shuffleFetcher.fetch()`
- Incoming (key, combiner) pairs are merged using `mergeCombiners` in a HashMap
- The HashMap is converted to an Iterator and returned

**Step 4: Results collected**
- `collect()` gathers all partition results to the driver

---

## 13.10 Why Shuffles Are Expensive

Shuffles are the most expensive operation in Spark because:

1. **Disk I/O**: Map-side results are written to disk (for fault tolerance)
2. **Network I/O**: Data transfers between all mappers and all reducers
3. **Serialization**: Data must be serialized to bytes for network transfer
4. **Memory**: The reduce side builds a HashMap of all received data
5. **No pipelining**: Stage 2 can't start until Stage 1 is complete

This is why Spark optimizes to **avoid shuffles** whenever possible (e.g., using narrow dependencies, checking partitioners).

---

## 13.11 Summary

| Property | ShuffledRDD |
|----------|------------|
| **Splits** | Brand new (from Partitioner, not from parent) |
| **Compute** | Fetch data from network, merge in HashMap using Aggregator |
| **Dependencies** | ShuffleDependency (wide — creates stage boundary) |
| **Partitioner** | Yes! Set to the requested Partitioner |
| **Preferred Locations** | None (data comes from network) |
| **Used by** | `reduceByKey`, `groupByKey`, `combineByKey`, `sortByKey`, etc. |

---

**Next Chapter**: [Chapter 14: CoGroupedRDD — Smart Joins →](Chapter-14-CoGroupedRDD.md)
