# Chapter 3: Split — What Is a Partition, Really?

Before we can understand an RDD, we need to understand its most basic building block: the **Split** (also called a **partition**). This chapter explains what it is, why it exists, and exactly how it looks in code.

---

## 3.1 The Real-World Analogy

Imagine you have a book with 1,000 pages. You want 10 friends to read it simultaneously so it gets read faster. What do you do?

You **split** the book:
- Friend 1 gets pages 1–100
- Friend 2 gets pages 101–200
- Friend 3 gets pages 201–300
- ...
- Friend 10 gets pages 901–1000

Each friend has a **chunk** of the book. They can read their chunk independently, without needing to coordinate with each other.

In Spark, an RDD's data is split the same way. Each chunk is called a **Split** (or **partition**). Each Split can be processed independently, by a different machine in the cluster.

```
RDD with 4 splits:

┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Split 0  │  │ Split 1  │  │ Split 2  │  │ Split 3  │
│ "apple"  │  │ "dog"    │  │ "grape"  │  │ "ice"    │
│ "banana" │  │ "egg"    │  │ "honey"  │  │ "jam"    │
│ "cherry" │  │ "fig"    │  │          │  │ "kiwi"   │
└──────────┘  └──────────┘  └──────────┘  └──────────┘
  Machine A     Machine B     Machine C     Machine D
```

Each split can live on a different machine. Each machine processes its own split. That's how Spark achieves **parallelism** — work on all splits at the same time.

---

## 3.2 The Actual Code: `Split.scala`

Let's look at the actual source code. Open the file `spark-0.5.0/core/src/main/scala/spark/Split.scala`:

```scala
package spark

/**
 * A partition of an RDD.
 */
trait Split extends Serializable {
  /**
   * Get the split's index within its parent RDD
   */
  val index: Int
  
  // A better default implementation of HashCode
  override def hashCode(): Int = index
}
```

That's the **entire file**. Let's translate it to Java:

```java
// Java equivalent of Split.scala
package spark;

import java.io.Serializable;

public interface Split extends Serializable {
    
    /**
     * Get the split's index within its parent RDD
     */
    int getIndex();
    
    // Default hashCode implementation
    default int hashCode() {
        return getIndex();
    }
}
```

### What does this tell us?

1. **`Split` is a trait (interface)** — it's not a concrete class. It's a contract that says: "If you are a Split, you must have an `index`."

2. **`val index: Int`** — Every split has an integer index. Think of it as the split's ID number. Split 0, Split 1, Split 2, etc.

3. **`extends Serializable`** — Splits can be serialized (converted to bytes and sent over the network). This is essential because Spark needs to send split information from the driver machine to worker machines.

4. **`hashCode()` returns `index`** — Two splits are distinguished by their index number.

That's it. A Split is just an **index number**. It's the simplest possible thing — just "I am partition number X."

---

## 3.3 But Wait — Isn't That Too Simple?

You might think: "A partition is just a number? Where's the actual data? Where's the information about which file this partition reads from?"

Great question. The `Split` trait is intentionally minimal. It defines only what **every** partition must have (an index). The **specific details** are added by subclasses.

Think of it in Java terms:

```java
// The base interface — very simple
interface Split extends Serializable {
    int getIndex();
}

// A specific implementation for reading from HDFS files
class HadoopSplit implements Split {
    private int rddId;
    private int index;
    private InputSplit inputSplit;  // <-- HERE's the HDFS block info!
    
    public int getIndex() { return index; }
    // The inputSplit knows which HDFS block this partition maps to,
    // including the file path, byte offset, and length
}

// A specific implementation for a shuffled dataset
class ShuffledRDDSplit implements Split {
    private int index;
    
    public int getIndex() { return index; }
    // That's really it — shuffled partitions don't need extra info
    // because data comes from the network, not from a file
}
```

Each type of RDD creates its own type of Split that carries whatever extra information it needs.

---

## 3.4 Real Split Subclasses from the Code

Let's look at every Split subclass in Spark 0.5.0:

### HadoopSplit (from `HadoopRDD.scala`)

```scala
class HadoopSplit(rddId: Int, idx: Int, @transient s: InputSplit)
  extends Split
  with Serializable {
  
  val inputSplit = new SerializableWritable[InputSplit](s)
  override def hashCode(): Int = (41 * (41 + rddId) + idx).toInt
  override val index = idx
}
```

**In Java terms:**
```java
class HadoopSplit implements Split, Serializable {
    private int rddId;
    private int index;
    private SerializableWritable<InputSplit> inputSplit;  // wraps Hadoop's InputSplit
    
    public HadoopSplit(int rddId, int idx, InputSplit s) {
        this.rddId = rddId;
        this.index = idx;
        this.inputSplit = new SerializableWritable<>(s);
    }
    
    public int getIndex() { return index; }
    public int hashCode() { return 41 * (41 + rddId) + index; }
}
```

**What's extra here:** The `inputSplit` field. This is Hadoop's way of describing a chunk of a file. It contains:
- The file path (e.g., `/data/logs/access.log`)
- The byte offset where this chunk starts (e.g., byte 1,000,000)
- The length of this chunk (e.g., 67,108,864 bytes = 64 MB)
- Which machines have copies of this data (e.g., `["machine-3", "machine-7", "machine-12"]`)

So a `HadoopSplit` knows: "I am partition #5, and I read bytes 5,000,000 to 5,067,108,864 of file `/data/logs/access.log`."

### ShuffledRDDSplit (from `ShuffledRDD.scala`)

```scala
class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}
```

**In Java terms:**
```java
class ShuffledRDDSplit implements Split {
    private int index;
    
    public ShuffledRDDSplit(int idx) {
        this.index = idx;
    }
    
    public int getIndex() { return index; }
    public int hashCode() { return index; }
}
```

**What's extra here:** Nothing! A shuffled partition doesn't need to know about files. It just needs an index number. The data it will contain comes from the network during the shuffle — it doesn't come from a specific file location.

### CartesianSplit (from `CartesianRDD.scala`)

```scala
class CartesianSplit(idx: Int, val s1: Split, val s2: Split) extends Split with Serializable {
  override val index = idx
}
```

**In Java terms:**
```java
class CartesianSplit implements Split, Serializable {
    private int index;
    private Split s1;  // a split from the first RDD
    private Split s2;  // a split from the second RDD
    
    public CartesianSplit(int idx, Split s1, Split s2) {
        this.index = idx;
        this.s1 = s1;
        this.s2 = s2;
    }
    
    public int getIndex() { return index; }
}
```

**What's extra here:** Two split references — `s1` and `s2`. A Cartesian partition represents a combination of one partition from the first RDD and one partition from the second RDD. It needs to remember which two parent partitions it combines.

### UnionSplit (from `UnionRDD.scala`)

```scala
class UnionSplit[T: ClassManifest](
    idx: Int, 
    rdd: RDD[T],
    split: Split)
  extends Split
  with Serializable {
  
  def iterator() = rdd.iterator(split)
  def preferredLocations() = rdd.preferredLocations(split)
  override val index = idx
}
```

**In Java terms:**
```java
class UnionSplit<T> implements Split, Serializable {
    private int index;
    private RDD<T> rdd;       // which parent RDD this split came from
    private Split split;       // which split in that parent RDD
    
    public int getIndex() { return index; }
    
    public Iterator<T> iterator() {
        return rdd.iterator(split);
    }
    
    public List<String> preferredLocations() {
        return rdd.preferredLocations(split);
    }
}
```

**What's extra here:** A reference to the parent RDD and the parent split. A Union combines multiple RDDs. Each UnionSplit remembers: "I am actually split #3 of parent RDD #2."

### SampledRDDSplit (from `SampledRDD.scala`)

```scala
class SampledRDDSplit(val prev: Split, val seed: Int) extends Split with Serializable {
  override val index = prev.index
}
```

**In Java terms:**
```java
class SampledRDDSplit implements Split, Serializable {
    private Split prev;   // the parent split to sample from
    private int seed;      // random seed for reproducibility
    
    public int getIndex() { return prev.getIndex(); }
}
```

**What's extra here:** The parent split and a random seed. Each partition needs its own seed so that sampling is deterministic (you get the same sample if you re-run).

### CoGroupSplit (from `CoGroupedRDD.scala`)

```scala
class CoGroupSplit(idx: Int, val deps: Seq[CoGroupSplitDep]) extends Split with Serializable {
  override val index = idx
  override def hashCode(): Int = idx
}
```

**What's extra here:** A list of dependencies — each one describing where data for this partition comes from (either directly from a parent partition, or via a network shuffle).

---

## 3.5 The Pattern

Now you can see the pattern clearly:

| Split Type | What Extra Info It Carries | Why |
|-----------|--------------------------|-----|
| `HadoopSplit` | Hadoop `InputSplit` (file path, byte offset, length) | Needs to know which file block to read |
| `ShuffledRDDSplit` | Nothing extra (just the index) | Data comes from the network, not a file |
| `CartesianSplit` | Two parent splits (`s1`, `s2`) | Cross-product of two partitions |
| `UnionSplit` | Parent RDD + parent split | Delegates to the right parent |
| `SampledRDDSplit` | Parent split + random seed | Samples from parent with a specific seed |
| `CoGroupSplit` | List of dependency descriptions | Groups data from multiple sources |

**The base `Split` trait gives every partition an index. Each RDD subclass adds whatever extra metadata its partitions need.**

---

## 3.6 How Splits Are Created

Splits are created when the RDD object is constructed. Every RDD has a `splits` method that returns `Array[Split]` — an array of all its partitions.

Here's how different RDDs create their splits:

**HadoopRDD** — asks Hadoop how many blocks the file has:
```scala
val splits_ : Array[Split] = {
    val inputFormat = createInputFormat(conf)
    val inputSplits = inputFormat.getSplits(conf, minSplits)    // Hadoop tells us the blocks
    val array = new Array[Split](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopSplit(id, i, inputSplits(i))      // wrap each block as a Split
    }
    array
}
```

**ShuffledRDD** — creates one split per partition requested by the Partitioner:
```scala
val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))
// If numPartitions = 4, this creates: [Split(0), Split(1), Split(2), Split(3)]
```

**MappedRDD** (map transformation) — reuses the parent's splits:
```scala
override def splits = prev.splits    // same partitions as parent
```

This last one is the most common case. Most transformations (map, filter, flatMap) don't change the partitioning — they just process each partition differently. So they inherit their parent's splits directly.

---

## 3.7 Summary

| Question | Answer |
|----------|--------|
| What is a Split? | A partition — a chunk of an RDD's data that can be processed independently |
| What does it contain? | At minimum, just an index (a number). Subclasses add extra metadata. |
| Where does the actual data live? | Not in the Split. The Split is a *description*. The actual data is produced by the `compute()` function (next chapter). |
| Who creates Splits? | Each RDD subclass creates its own splits in its `splits` method. |
| How many Splits does an RDD have? | Depends on the RDD. A HadoopRDD has one per HDFS block. A ShuffledRDD has one per partition. A MappedRDD has the same number as its parent. |
| Can different machines work on different Splits? | Yes! That's the whole point. Each Split can be processed on a different machine in parallel. |

Now that you know what a Split is, the next question is: **how does Spark actually produce the data for each Split?** That's the `compute()` function, which is the topic of our next chapter.

---

**Next Chapter**: [Chapter 4: Compute — The Recipe for One Partition →](Chapter-04-Compute.md)
