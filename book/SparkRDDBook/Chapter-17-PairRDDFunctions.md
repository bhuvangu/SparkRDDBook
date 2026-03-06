# Chapter 17: PairRDDFunctions — The Key-Value Power Tools

When your RDD contains key-value pairs (tuples of `(K, V)`), Spark automatically makes extra methods available through `PairRDDFunctions`. These are the workhorses of data processing: `reduceByKey`, `groupByKey`, `join`, `sortByKey`, and more.

---

## 17.1 How It Works — Implicit Conversion

In Java, you'd need to explicitly wrap your RDD. In Scala, Spark does this automatically:

```scala
// In SparkContext (simplified):
implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)]) = new PairRDDFunctions(rdd)
```

So when you write:
```scala
val pairs: RDD[(String, Int)] = ...
pairs.reduceByKey(_ + _)    // reduceByKey is on PairRDDFunctions, not RDD
```

Scala automatically converts `pairs` to `PairRDDFunctions(pairs)` and calls `reduceByKey` on it.

---

## 17.2 The Key Operations

### `combineByKey` — The Foundation

**Every** key-value aggregation goes through `combineByKey`:

```scala
def combineByKey[C](createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    partitioner: Partitioner): RDD[(K, C)] = {
  val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
  new ShuffledRDD(self, aggregator, partitioner)
}
```

It creates an `Aggregator` (Chapter 13) and a `ShuffledRDD`. That's it — all the complexity is in the shuffle.

### `reduceByKey(func)` — Sum/combine values per key

```scala
def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, defaultPartitioner(self))
}
```

Uses `combineByKey` with:
- `createCombiner`: The value itself (`v => v`)
- `mergeValue`: Apply `func` to combine (`func`)
- `mergeCombiners`: Same function (`func`)

**Example**: `reduceByKey(_ + _)` sums all values per key.

### `groupByKey()` — Collect all values per key into a list

```scala
def groupByKey(partitioner: Partitioner): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](createCombiner _, mergeValue _, mergeCombiners _, partitioner)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
}
```

Collects values into `ArrayBuffer`s (like Java's `ArrayList`).

> **⚠️ Performance tip**: `reduceByKey` is almost always better than `groupByKey` because it combines values **locally** before the shuffle, sending less data over the network.

### `join(other)` — SQL-style inner join

```scala
def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = {
    this.cogroup(other, partitioner).flatMapValues {
        case (vs, ws) =>
            for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
}
```

Built on `cogroup` (Chapter 14) + `flatMapValues`. For each key, produces all pairs of (left value, right value).

### `sortByKey()` — Global sort

```scala
def sortByKey(ascending: Boolean = true): RDD[(K,V)] = {
    val rangePartitionedRDD = self.partitionBy(new RangePartitioner(self.splits.size, self, ascending))
    new SortedRDD(rangePartitionedRDD, ascending)
}
```

Two steps:
1. **Repartition** with a `RangePartitioner` (so keys are in sorted order across partitions)
2. **Sort locally** within each partition using `SortedRDD`

### `mapValues(f)` and `flatMapValues(f)` — Transform only values

```scala
def mapValues[U](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD(self, cleanF)
}
```

These create `MappedValuesRDD` / `FlatMappedValuesRDD` which **preserve the partitioner**. Since keys don't change, the partitioning remains valid.

### `lookup(key)` — Find values for a specific key

```scala
def lookup(key: K): Seq[V] = {
    self.partitioner match {
        case Some(p) =>
            val index = p.getPartition(key)
            // Only scan ONE partition instead of all of them
            val res = self.context.runJob(self, process _, Array(index), false)
            res(0)
        case None =>
            throw new UnsupportedOperationException("lookup() called on an RDD without a partitioner")
    }
}
```

If the RDD has a partitioner, `lookup` computes which partition the key is in and only reads that one partition. Much faster than scanning everything!

---

## 17.3 Helper RDDs

### SortedRDD — Sorts within a partition

```scala
class SortedRDD[K <% Ordered[K], V](prev: RDD[(K, V)], ascending: Boolean)
  extends RDD[(K, V)](prev.context) {
  override def compute(split: Split) = {
      prev.iterator(split).toArray
          .sortWith((x, y) => if (ascending) x._1 < y._1 else x._1 > y._1).iterator
  }
}
```

Loads the partition into an array, sorts it, returns an iterator. Preserves the parent's partitioner.

### MappedValuesRDD — Transforms values, preserves keys

```scala
class MappedValuesRDD[K, V, U](prev: RDD[(K, V)], f: V => U) extends RDD[(K, U)](prev.context) {
  override val partitioner = prev.partitioner   // PRESERVED!
  override def compute(split: Split) = prev.iterator(split).map{case (k, v) => (k, f(v))}
}
```

Key insight: `override val partitioner = prev.partitioner` — the partitioner is passed through because keys don't change.

---

## 17.4 SequenceFileRDDFunctions

For saving key-value RDDs as Hadoop SequenceFiles:

```scala
class SequenceFileRDDFunctions[K <% Writable, V <% Writable](self: RDD[(K,V)]) {
    def saveAsSequenceFile(path: String) { ... }
}
```

Converts keys and values to Hadoop's `Writable` format and saves using `SequenceFileOutputFormat`.

---

## 17.5 How All Operations Connect

```
                            combineByKey
                           ╱     │      ╲
                reduceByKey   groupByKey  partitionBy
                                              │
                    cogroup ──────────────────│
                   ╱   │   ╲                  │
               join  leftJoin  rightJoin   sortByKey
```

Everything flows through either `combineByKey` (for aggregations → `ShuffledRDD`) or `cogroup` (for joins → `CoGroupedRDD`). These are the two fundamental operations, and everything else is built on top of them.

---

## 17.6 Summary

| Operation | Builds on | Shuffle? | Notes |
|-----------|----------|----------|-------|
| `reduceByKey` | `combineByKey` → `ShuffledRDD` | Yes | Combines locally first (efficient) |
| `groupByKey` | `combineByKey` → `ShuffledRDD` | Yes | Collects values into lists (less efficient) |
| `combineByKey` | Directly creates `ShuffledRDD` | Yes | The foundation of all by-key aggregations |
| `join` | `cogroup` → `flatMapValues` | Usually | Skips shuffle if already co-partitioned |
| `leftOuterJoin` | `cogroup` → `flatMapValues` | Usually | Same |
| `rightOuterJoin` | `cogroup` → `flatMapValues` | Usually | Same |
| `sortByKey` | `partitionBy` → `SortedRDD` | Yes | Range partition + local sort |
| `mapValues` | `MappedValuesRDD` | No | Preserves partitioner |
| `flatMapValues` | `FlatMappedValuesRDD` | No | Preserves partitioner |
| `lookup` | Direct partition scan | No | O(1) partition lookups if partitioner exists |

---

**Next Chapter**: [Chapter 18: Lineage — The RDD Graph →](Chapter-18-Lineage.md)
