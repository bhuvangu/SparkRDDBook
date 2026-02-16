# Chapter 6: Partitioner — How Keys Get Assigned to Partitions

So far, we've learned three of the five RDD properties: splits, compute, and dependencies. The fourth property is the **Partitioner** — and it only matters for **key-value RDDs** (RDDs where each element is a pair like `(key, value)`).

---

## 6.1 The Problem: Where Should Each Key Go?

Imagine you have an RDD of (word, count) pairs spread across 3 partitions:

```
Partition 0: ("apple", 5), ("banana", 3), ("cherry", 7)
Partition 1: ("apple", 2), ("banana", 1), ("dog", 4)
Partition 2: ("cherry", 3), ("dog", 2), ("apple", 1)
```

You want to run `reduceByKey(_ + _)` to get total counts per word. The result should be:

```
("apple", 8), ("banana", 4), ("cherry", 10), ("dog", 6)
```

To do this, **all values for "apple" must end up on the same machine**. The values `5`, `2`, and `1` are currently spread across partitions 0, 1, and 2 — they need to be gathered together.

The question is: **which partition should "apple" go to?** Which partition gets "banana"? Which gets "cherry"?

This is exactly what a **Partitioner** decides.

---

## 6.2 The Java Analogy: HashMap's Bucket Assignment

If you've used a `HashMap` in Java, you already understand this concept.

A Java `HashMap` works like this:
1. You have an array of "buckets" (say 16 buckets)
2. When you do `map.put("apple", 5)`, Java computes: `bucket = "apple".hashCode() % 16`
3. "apple" goes into that bucket
4. All entries with the same key always end up in the same bucket

A Spark Partitioner works **exactly the same way**, except:
- Instead of buckets in memory, there are **partitions across machines**
- Instead of one HashMap on one JVM, it's a **distributed** operation across a cluster

```
Java HashMap:           Spark Partitioner:
┌─────────┐             ┌─────────────────┐
│Bucket 0 │             │ Partition 0     │  (on Machine A)
│Bucket 1 │             │ Partition 1     │  (on Machine B)
│Bucket 2 │             │ Partition 2     │  (on Machine C)
│  ...    │             │ Partition 3     │  (on Machine D)
│Bucket 15│             └─────────────────┘
└─────────┘
```

The algorithm is the same: `partition = key.hashCode() % numPartitions`

---

## 6.3 The Actual Code: `Partitioner.scala`

Let's look at the source code (`spark-0.5.0/core/src/main/scala/spark/Partitioner.scala`):

```scala
abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}
```

**In Java terms:**
```java
public abstract class Partitioner implements Serializable {
    
    /**
     * How many partitions are there?
     */
    public abstract int numPartitions();
    
    /**
     * Given a key, which partition (0 to numPartitions-1) should it go to?
     */
    public abstract int getPartition(Object key);
}
```

That's the entire contract. A Partitioner answers one question: **"Given this key, which partition number should it go to?"**

---

## 6.4 HashPartitioner

The simplest and most common partitioner:

```scala
class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any) = {
    val mod = key.hashCode % partitions
    if (mod < 0) {
      mod + partitions
    } else {
      mod // Guard against negative hash codes
    }
  }
  
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
```

**In Java terms:**
```java
public class HashPartitioner extends Partitioner {
    private int partitions;
    
    public HashPartitioner(int partitions) {
        this.partitions = partitions;
    }
    
    public int numPartitions() {
        return partitions;
    }
    
    public int getPartition(Object key) {
        int mod = key.hashCode() % partitions;
        if (mod < 0) {
            return mod + partitions;  // handle negative hash codes
        } else {
            return mod;
        }
    }
    
    public boolean equals(Object other) {
        if (other instanceof HashPartitioner) {
            return ((HashPartitioner) other).partitions == this.partitions;
        }
        return false;
    }
}
```

### How it works — traced through:

```
HashPartitioner with 3 partitions:

"apple".hashCode()  = 93029210
93029210 % 3 = 1        → Partition 1

"banana".hashCode() = -1396355602
-1396355602 % 3 = -2
-2 + 3 = 1              → Partition 1

"cherry".hashCode() = -1360544799
-1360544799 % 3 = 0     → Partition 0

"dog".hashCode()    = 99644
99644 % 3 = 2           → Partition 2
```

So the data gets redistributed as:
```
Before (random placement):           After (hash-partitioned):
Partition 0: apple, banana, cherry   Partition 0: cherry
Partition 1: apple, banana, dog      Partition 1: apple, banana
Partition 2: cherry, dog, apple      Partition 2: dog
```

Now all "apple" values are in Partition 1, all "cherry" values are in Partition 0, etc. A `reduceByKey` can now sum up values within each partition without any further data movement!

### Why `equals()` matters

Notice the `equals()` method checks if two HashPartitioners have the same number of partitions. This is used by `CoGroupedRDD` (Chapter 14) for optimization: if two RDDs already use the same HashPartitioner, Spark can join them **without** a shuffle — their keys are already in the right partitions.

---

## 6.5 RangePartitioner

The second partitioner sorts data into **ranges**, like a phone book that's divided alphabetically:

```
Partition 0: A-F  (all keys from "a" to "f")
Partition 1: G-M  (all keys from "g" to "m")
Partition 2: N-Z  (all keys from "n" to "z")
```

This is used for `sortByKey()` — you want the output to be globally sorted, so you need keys to be arranged in order across partitions.

```scala
class RangePartitioner[K <% Ordered[K]: ClassManifest, V](
    partitions: Int,
    @transient rdd: RDD[(K,V)],
    private val ascending: Boolean = true) 
  extends Partitioner {

  private val rangeBounds: Array[K] = {
    val rddSize = rdd.count()
    val maxSampleSize = partitions * 10.0
    val frac = math.min(maxSampleSize / math.max(rddSize, 1), 1.0)
    val rddSample = rdd.sample(true, frac, 1).map(_._1).collect()
      .sortWith((x, y) => if (ascending) x < y else x > y)
    if (rddSample.length == 0) {
      Array()
    } else {
      val bounds = new Array[K](partitions)
      for (i <- 0 until partitions) {
        bounds(i) = rddSample(i * rddSample.length / partitions)
      }
      bounds
    }
  }

  def numPartitions = rangeBounds.length

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    while (partition < rangeBounds.length - 1 && k > rangeBounds(partition)) {
      partition += 1
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - 1 - partition
    }
  }
}
```

**In Java terms (simplified):**
```java
public class RangePartitioner<K extends Comparable<K>, V> extends Partitioner {
    private K[] rangeBounds;
    private boolean ascending;
    
    public RangePartitioner(int partitions, RDD<Pair<K,V>> rdd, boolean ascending) {
        this.ascending = ascending;
        
        // Step 1: Sample some keys from the data
        long rddSize = rdd.count();
        double frac = Math.min(partitions * 10.0 / Math.max(rddSize, 1), 1.0);
        K[] sample = rdd.sample(true, frac, 1)
            .map(pair -> pair.getKey())
            .collect();
        
        // Step 2: Sort the sample
        Arrays.sort(sample);
        
        // Step 3: Pick evenly-spaced boundaries
        rangeBounds = new K[partitions];
        for (int i = 0; i < partitions; i++) {
            rangeBounds[i] = sample[i * sample.length / partitions];
        }
    }
    
    public int numPartitions() {
        return rangeBounds.length;
    }
    
    public int getPartition(Object key) {
        K k = (K) key;
        int partition = 0;
        while (partition < rangeBounds.length - 1 && k.compareTo(rangeBounds[partition]) > 0) {
            partition++;
        }
        return ascending ? partition : rangeBounds.length - 1 - partition;
    }
}
```

### How it works:

1. **Sample**: Take a random sample of keys from the data (don't read everything — just a sample)
2. **Sort the sample**: Arrange the sampled keys in order
3. **Pick boundaries**: Choose evenly-spaced keys as partition boundaries
4. **Assign**: For any key, walk through the boundaries to find which partition it belongs to

**Example** with numbers and 3 partitions:

```
Data: [5, 12, 3, 45, 8, 22, 1, 37, 15, 28, 9, 41]

Step 1-2: Sample and sort → [1, 3, 5, 8, 9, 12, 15, 22, 28, 37, 41, 45]

Step 3: Boundaries →
  Boundary 0: sample[0 * 12/3] = sample[0] = 1
  Boundary 1: sample[1 * 12/3] = sample[4] = 9
  Boundary 2: sample[2 * 12/3] = sample[8] = 28

Step 4: Assignment:
  Partition 0: keys ≤ 1     → [1]
  Partition 1: keys 2-9     → [3, 5, 8, 9]
  Partition 2: keys 10-28   → [12, 15, 22, 28]
  (keys > 28 also go to last partition → [37, 41, 45])

Now within each partition, if you sort locally, the data is GLOBALLY sorted:
  [1] | [3, 5, 8, 9] | [12, 15, 22, 28, 37, 41, 45]
```

---

## 6.6 Which RDDs Have a Partitioner?

Not all RDDs have a partitioner. Only key-value RDDs that have been through a shuffle (or explicitly partitioned) have one.

In the base `RDD` class:
```scala
val partitioner: Option[Partitioner] = None   // default: no partitioner
```

Which RDDs override this?

| RDD | Partitioner | Why |
|-----|------------|-----|
| `HadoopRDD` | `None` | Data comes from a file — not partitioned by key |
| `MappedRDD` | `None` | Just applies a function — doesn't repartition |
| `FilteredRDD` | `None` | Just filters — doesn't repartition |
| `ShuffledRDD` | `Some(part)` ✓ | The shuffle explicitly repartitions data by key |
| `CoGroupedRDD` | `Some(part)` ✓ | Data is grouped by key |
| `SortedRDD` | Parent's partitioner ✓ | Preserves the range partitioning from sortByKey |
| `MappedValuesRDD` | Parent's partitioner ✓ | Only changes values, not keys — partitioning preserved |
| `FlatMappedValuesRDD` | Parent's partitioner ✓ | Same — only values change |

The key insight: **if a transformation only changes values (not keys), the partitioner is preserved.** If it changes keys or reshuffles data, a new partitioner is set.

---

## 6.7 Why Partitioners Enable Optimizations

The partitioner is not just bookkeeping. It enables critical optimizations:

### Optimization 1: Avoiding unnecessary shuffles

When you join two RDDs:
```scala
val rdd1 = someData.reduceByKey(_ + _)   // uses HashPartitioner(10)
val rdd2 = otherData.reduceByKey(_ + _)  // uses HashPartitioner(10)
val joined = rdd1.join(rdd2)             // join by key
```

Spark checks: "Do `rdd1` and `rdd2` have the same partitioner?" If yes (same `HashPartitioner` with same number of partitions), keys are already co-located — **no shuffle needed!** This is checked in `CoGroupedRDD`:

```scala
if (rdd.partitioner == Some(part)) {
    deps += new OneToOneDependency(rdd)       // no shuffle!
} else {
    deps += new ShuffleDependency(...)         // needs shuffle
}
```

### Optimization 2: Efficient key lookup

If an RDD has a partitioner, you can look up a single key without scanning all partitions:

```scala
def lookup(key: K): Seq[V] = {
    self.partitioner match {
        case Some(p) =>
            val index = p.getPartition(key)    // which partition has this key?
            // Only scan that one partition, not all of them!
            val res = self.context.runJob(self, process _, Array(index), false)
            res(0)
        case None =>
            throw new UnsupportedOperationException("lookup() called on an RDD without a partitioner")
    }
}
```

Instead of checking all partitions (expensive!), Spark computes which single partition contains the key and only reads that one.

---

## 6.8 Summary

| Question | Answer |
|----------|--------|
| What is a Partitioner? | A strategy for assigning keys to partition numbers. Like HashMap's bucket assignment, but across machines. |
| When is it used? | Only for key-value RDDs (RDDs of pairs). |
| What is HashPartitioner? | The most common partitioner: `key.hashCode() % numPartitions`. Distributes keys roughly evenly. |
| What is RangePartitioner? | A partitioner that sorts keys into ordered ranges. Used for `sortByKey()`. |
| Why does it matter? | 1) Avoids unnecessary shuffles when two RDDs have the same partitioner. 2) Enables efficient key lookups. |
| Which RDDs have one? | Only RDDs that have been shuffled or explicitly partitioned. `ShuffledRDD`, `CoGroupedRDD`, and RDDs that preserve their parent's partitioner (like `MappedValuesRDD`). |

---

## 6.9 All Five Properties — Complete!

With the Partitioner covered, we now know all five properties of an RDD:

| # | Property | What It Answers |
|---|----------|----------------|
| 1 | **Splits** | "How is this data divided into chunks?" |
| 2 | **Compute** | "How do I produce the data for one chunk?" |
| 3 | **Dependencies** | "Which other RDDs did this one come from?" |
| 4 | **Partitioner** | "How are keys assigned to chunks?" (optional) |
| 5 | **Preferred Locations** | "Where should each chunk ideally be computed?" (optional) |

We haven't discussed Preferred Locations in its own chapter because it's simple: it's a hint that says "this partition's data is on machine X, so try to compute it there." Only `HadoopRDD` sets this (using HDFS block locations). Most other RDDs return an empty list.

Now it's time to see how all five properties come together in the base `RDD` class itself.

---

**Next Chapter**: [Chapter 7: The Base RDD Class — The Contract →](Chapter-07-Base-RDD-Class.md)
