# 📘 Understanding RDD — From Java to Spark's Core

**A comprehensive guide to Spark's Resilient Distributed Dataset, explained for Java developers.**

Generated using Opus4.6(Anthropic) based on the actual source code of **Apache Spark 0.5.0** — the earliest public release, where the design is beautifully simple and the core ideas are crystal clear.



---

## Who This Book Is For

- You have solid **Java experience** (OOP, collections, generics, iterators, HashMap)
- You have **zero Scala experience**
- You have **zero Spark experience**
- You want to understand RDD both **logically** (what it is, why it exists) and at the **code level** (how it actually works inside)

---

## Table of Contents

### Part 1: The Problem RDD Solves
- [Chapter 1: The Problem — Processing Big Data Across Machines](Chapter-01-The-Problem.md)
- [Chapter 2: A Gentle Scala Survival Guide for Java Developers](Chapter-02-Scala-for-Java-Developers.md)

### Part 2: The RDD Building Blocks
- [Chapter 3: Split — What Is a Partition, Really?](Chapter-03-Split.md)
- [Chapter 4: Compute — The Recipe for One Partition](Chapter-04-Compute.md)
- [Chapter 5: Dependency — Who Is Your Parent?](Chapter-05-Dependency.md)
- [Chapter 6: Partitioner — How Keys Get Assigned to Partitions](Chapter-06-Partitioner.md)

### Part 3: The RDD Itself
- [Chapter 7: The Base RDD Class — The Contract](Chapter-07-Base-RDD-Class.md)
- [Chapter 8: Transformations — Creating New RDDs from Old Ones](Chapter-08-Transformations.md)
- [Chapter 9: Actions — When Computation Actually Happens](Chapter-09-Actions.md)

### Part 4: The RDD Zoo — Every Subclass Explained
- [Chapter 10: HadoopRDD — Reading Data from the Outside World](Chapter-10-HadoopRDD.md)
- [Chapter 11: UnionRDD — Combining Datasets](Chapter-11-UnionRDD.md)
- [Chapter 12: CartesianRDD — The Cross Product](Chapter-12-CartesianRDD.md)
- [Chapter 13: ShuffledRDD — The Network Shuffle](Chapter-13-ShuffledRDD.md)
- [Chapter 14: CoGroupedRDD — Smart Joins](Chapter-14-CoGroupedRDD.md)
- [Chapter 15: PipedRDD — Shelling Out to External Programs](Chapter-15-PipedRDD.md)
- [Chapter 16: SampledRDD — Random Sampling](Chapter-16-SampledRDD.md)
- [Chapter 17: PairRDDFunctions — The Key-Value Power Tools](Chapter-17-PairRDDFunctions.md)

### Part 5: The Big Picture
- [Chapter 18: Lineage — The RDD Graph](Chapter-18-Lineage.md)
- [Chapter 19: Putting It All Together — A Full Example](Chapter-19-Full-Example.md)

---

## Source Code Reference

All code examples in this book come from:
```
spark-0.5.0/core/src/main/scala/spark/
```

The key files we'll study:
| File | What It Contains |
|------|-----------------|
| `RDD.scala` | The base RDD class + simple transformations (MappedRDD, FilteredRDD, etc.) |
| `Split.scala` | The partition interface |
| `Dependency.scala` | How RDDs relate to their parents |
| `Partitioner.scala` | How keys map to partitions |
| `HadoopRDD.scala` | Reading data from HDFS/files |
| `NewHadoopRDD.scala` | Same, with newer Hadoop API |
| `ShuffledRDD.scala` | Data redistribution across the network |
| `CoGroupedRDD.scala` | Grouping multiple RDDs by key |
| `UnionRDD.scala` | Combining multiple RDDs |
| `CartesianRDD.scala` | Cross product of two RDDs |
| `PipedRDD.scala` | Piping data through external commands |
| `SampledRDD.scala` | Random sampling |
| `PairRDDFunctions.scala` | Extra operations for key-value RDDs |
| `SequenceFileRDDFunctions.scala` | Saving to Hadoop SequenceFiles |
