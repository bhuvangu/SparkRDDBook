# Chapter 19: Putting It All Together — A Full Example

Let's trace a complete word count program through every layer of the RDD system. We'll see every object created, every method called, and every data movement.

---

## 19.1 The Program

```scala
val counts = sc.textFile("data.txt")
    .flatMap(_.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .collect()
```

This reads a text file, splits each line into words, counts each word, and brings the results to the driver. Five lines of code. Let's see what happens inside.

---

## 19.2 Phase 1: Building the DAG (No Computation)

Each line creates an RDD object:

### Line 1: `sc.textFile("data.txt")`

**Creates**: `HadoopRDD` (id=0)

```
Object created in memory:
┌─────────────────────────────────────────────┐
│ HadoopRDD (id=0)                            │
│   splits: [HadoopSplit(0), HadoopSplit(1), HadoopSplit(2)]  │
│   compute: opens RecordReader, reads lines  │
│   dependencies: Nil (no parent)             │
│   partitioner: None                         │
│   preferredLocations: HDFS block locations  │
└─────────────────────────────────────────────┘
```

The file has 3 HDFS blocks → 3 splits.

### Line 2: `.flatMap(_.split(" "))`

**Creates**: `FlatMappedRDD` (id=1)

```
┌─────────────────────────────────────────────┐
│ FlatMappedRDD (id=1)                        │
│   splits: same as HadoopRDD (3 splits)      │
│   compute: prev.iterator(split).flatMap(f)  │
│   dependencies: [OneToOneDependency(HadoopRDD)]  │
│   f: line => line.split(" ")                │
└─────────────────────────────────────────────┘
```

### Line 3: `.map(word => (word, 1))`

**Creates**: `MappedRDD` (id=2)

```
┌─────────────────────────────────────────────┐
│ MappedRDD (id=2)                            │
│   splits: same as parent (3 splits)         │
│   compute: prev.iterator(split).map(f)      │
│   dependencies: [OneToOneDependency(FlatMappedRDD)]  │
│   f: word => (word, 1)                      │
└─────────────────────────────────────────────┘
```

### Line 4: `.reduceByKey(_ + _)`

**Creates**: `ShuffledRDD` (id=3) — via PairRDDFunctions.combineByKey

```
┌──────────────────────────────────────────────────────┐
│ ShuffledRDD (id=3)                                   │
│   splits: [ShuffledRDDSplit(0), ShuffledRDDSplit(1)] │
│   compute: fetch from network, merge in HashMap      │
│   dependencies: [ShuffleDependency(MappedRDD)]       │
│   partitioner: Some(HashPartitioner(2))              │
│   aggregator: createCombiner=v=>v, merge=_+_, combine=_+_  │
└──────────────────────────────────────────────────────┘
```

The HashPartitioner determines the number of output partitions (let's say 2 for this example).

### The DAG at this point:

```
HadoopRDD(0) ──OneToOne──→ FlatMappedRDD(1) ──OneToOne──→ MappedRDD(2) ──Shuffle──→ ShuffledRDD(3)
  3 splits                    3 splits                      3 splits                   2 splits
```

**Still no data has been read. No computation has happened.** We've just created 4 Java objects linked by dependency pointers.

---

## 19.3 Phase 2: `.collect()` Triggers Execution

`collect()` calls `sc.runJob(ShuffledRDD, iter => iter.toArray)`. The scheduler:

1. Walks backwards from `ShuffledRDD`
2. Finds the ShuffleDependency → creates a stage boundary
3. Creates two stages

---

## 19.4 Phase 3: Stage 1 Execution (Map Side)

**Stage 1**: HadoopRDD → FlatMappedRDD → MappedRDD

3 tasks created (one per partition):

### Task 0 (runs on Machine A, where HDFS block 0 lives):

```
Step 1: HadoopRDD.compute(Split 0)
   → Opens RecordReader for HDFS block 0
   → Iterator yields:
        (0, "hello world hello")
        (35, "spark is great")

Step 2: FlatMappedRDD.compute(Split 0)
   → prev.iterator(Split 0).flatMap(line => line.split(" "))
   → Iterator yields:
        "hello", "world", "hello", "spark", "is", "great"

Step 3: MappedRDD.compute(Split 0)
   → prev.iterator(Split 0).map(word => (word, 1))
   → Iterator yields:
        ("hello", 1), ("world", 1), ("hello", 1), ("spark", 1), ("is", 1), ("great", 1)

Step 4: Write shuffle output
   For each (key, value), compute partition = key.hashCode() % 2:
     ("hello", 1) → hash % 2 = 0 → shuffle file for partition 0
     ("world", 1) → hash % 2 = 1 → shuffle file for partition 1
     ("hello", 1) → hash % 2 = 0 → shuffle file for partition 0
     ("spark", 1) → hash % 2 = 1 → shuffle file for partition 1
     ("is", 1)    → hash % 2 = 0 → shuffle file for partition 0
     ("great", 1) → hash % 2 = 1 → shuffle file for partition 1

   Local combining (using aggregator.mergeValue):
     Partition 0 file: ("hello", 2), ("is", 1)
     Partition 1 file: ("world", 1), ("spark", 1), ("great", 1)
```

Note the pipelining: each line flows through flatMap → map → shuffle write **without any intermediate list being created**. It's all iterators.

Tasks 1 and 2 do the same for their partitions. All 3 tasks run **in parallel** on different machines.

---

## 19.5 Phase 4: Shuffle Data Transfer

After all Stage 1 tasks complete, shuffle data is on disk across machines:

```
Machine A:  partition-0-file: ("hello",2), ("is",1)
            partition-1-file: ("world",1), ("spark",1), ("great",1)

Machine B:  partition-0-file: ("hello",1), ("spark",1) 
            partition-1-file: ("world",1), ("is",1)

Machine C:  partition-0-file: ("great",1)
            partition-1-file: ("hello",1)
```

---

## 19.6 Phase 5: Stage 2 Execution (Reduce Side)

**Stage 2**: ShuffledRDD (→ collect)

2 tasks created (one per ShuffledRDD partition):

### Task 0 (ShuffledRDD partition 0):

```
Step 1: ShuffledRDD.compute(Split 0)

   Create HashMap: combiners = {}

   Fetch from Machine A (partition 0):
     ("hello", 2) → combiners = {"hello": 2}
     ("is", 1)    → combiners = {"hello": 2, "is": 1}

   Fetch from Machine B (partition 0):
     ("hello", 1) → mergeCombiners(2, 1) = 3 → combiners = {"hello": 3, "is": 1}
     ("spark", 1) → combiners = {"hello": 3, "is": 1, "spark": 1}

   Fetch from Machine C (partition 0):
     ("great", 1) → combiners = {"hello": 3, "is": 1, "spark": 1, "great": 1}

   Return iterator over HashMap:
     ("hello", 3), ("is", 1), ("spark", 1), ("great", 1)

Step 2: collect's function: iter.toArray
   → Array(("hello", 3), ("is", 1), ("spark", 1), ("great", 1))
   
   Send this array to the driver.
```

### Task 1 (ShuffledRDD partition 1):

Similar process, fetches partition-1 files from all machines:

```
   Result: Array(("world", 2), ("is", 1), ("hello", 1))
   Send to driver.
```

---

## 19.7 Phase 6: Driver Combines Results

```
collect() receives:
  From Task 0: Array(("hello", 3), ("is", 1), ("spark", 1), ("great", 1))
  From Task 1: Array(("world", 2), ("is", 1), ("hello", 1))

Array.concat:
  Array(("hello",3), ("is",1), ("spark",1), ("great",1), ("world",2), ("is",1), ("hello",1))
```

And that's your final result!

---

## 19.8 The Timeline View

```
Time →

        Machine A              Machine B              Machine C
        ─────────              ─────────              ─────────
        
 t=0    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
        │ Stage 1      │      │ Stage 1      │      │ Stage 1      │
        │ Task 0:      │      │ Task 1:      │      │ Task 2:      │
        │ read HDFS 0  │      │ read HDFS 1  │      │ read HDFS 2  │
        │ flatMap      │      │ flatMap      │      │ flatMap      │
        │ map          │      │ map          │      │ map          │
        │ write shuffle│      │ write shuffle│      │ write shuffle│
        └──────────────┘      └──────────────┘      └──────────────┘
 
 t=1    ═══════════════════ SHUFFLE BARRIER ═══════════════════════

 t=2    ┌──────────────┐      ┌──────────────┐
        │ Stage 2      │      │ Stage 2      │
        │ Task 0:      │      │ Task 1:      │
        │ fetch data   │      │ fetch data   │
        │ merge HashMap│      │ merge HashMap│
        │ → driver     │      │ → driver     │
        └──────────────┘      └──────────────┘

 t=3    ┌──────────────┐
        │ Driver:      │
        │ concatenate  │
        │ return result│
        └──────────────┘
```

---

## 19.9 Every Object Involved — The Complete Map

```
Source Code Files:
  Split.scala         → HadoopSplit, ShuffledRDDSplit
  Dependency.scala    → OneToOneDependency, ShuffleDependency
  Partitioner.scala   → HashPartitioner
  RDD.scala           → FlatMappedRDD, MappedRDD, FilteredRDD (base class)
  HadoopRDD.scala     → HadoopRDD (reads files)
  ShuffledRDD.scala   → ShuffledRDD (shuffles data)
  PairRDDFunctions.scala → reduceByKey, combineByKey (creates ShuffledRDD)

Objects created during DAG building:
  1 × HadoopRDD
  1 × FlatMappedRDD  
  1 × MappedRDD
  1 × ShuffledRDD
  1 × Aggregator
  1 × HashPartitioner
  3 × HadoopSplit
  2 × ShuffledRDDSplit
  2 × OneToOneDependency
  1 × ShuffleDependency

Objects created during execution:
  3 × RecordReader (one per HadoopRDD partition)
  6 × shuffle output files (3 tasks × 2 partitions)
  2 × HashMap (one per ShuffledRDD partition)
  Many × Iterator objects (for pipelining)
```

---

## 19.10 What You've Learned

Congratulations! You've made it through the entire book. Let's recap:

**Part 1** (Chapters 1–2): You understood the **problem** RDDs solve and learned enough **Scala** to read Spark code.

**Part 2** (Chapters 3–6): You learned the **four building blocks** — Split (partitioning), Compute (data production), Dependency (parent tracking), and Partitioner (key distribution).

**Part 3** (Chapters 7–9): You saw how the base **RDD class** combines these into a contract, how **transformations** build the DAG lazily, and how **actions** trigger execution.

**Part 4** (Chapters 10–17): You explored every **RDD subclass** — from HadoopRDD (reading files) to ShuffledRDD (network redistribution) to CoGroupedRDD (smart joins) — and the key-value operations that power real-world data processing.

**Part 5** (Chapters 18–19): You understood **lineage** (the recovery plan), **stages** (the execution plan), and traced a complete example through every layer.

---

## 19.11 The Core Insight

An RDD is not data. It's a **description** — five properties that tell Spark:
1. How to **partition** the work
2. How to **compute** each partition
3. Where the **inputs** come from
4. How **keys** are distributed
5. Where to **prefer** computing

From these five simple methods, Spark builds a complete distributed computing engine with lazy evaluation, pipelining, fault tolerance, and data locality. That's the elegance of the RDD abstraction — **simple parts, powerful whole**.

---

*Thank you for reading. Happy Sparking!* 🚀
