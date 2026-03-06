# Chapter 8: Transformations — Creating New RDDs from Old Ones

A transformation takes an existing RDD and creates a **new** RDD. The key insight: **no computation happens during a transformation.** It just builds a new RDD object with a pointer back to its parent.

---

## 8.1 Lazy Evaluation — The Most Important Concept

When you write:
```scala
val rdd1 = sc.textFile("data.txt")
val rdd2 = rdd1.filter(_.contains("error"))
val rdd3 = rdd2.map(_.toUpperCase)
```

What actually happens in memory:

```
After line 1:  rdd1 = HadoopRDD object (just metadata, no data loaded)
After line 2:  rdd2 = FilteredRDD object (points to rdd1, holds the filter function)
After line 3:  rdd3 = MappedRDD object (points to rdd2, holds the map function)
```

At this point, **zero data has been read from disk.** You've just built three lightweight Java objects linked together. It's like writing a recipe without cooking.

Only when you call an **action** does computation begin:
```scala
val results = rdd3.collect()   // NOW computation happens
```

### Why lazy?

1. **Optimization**: Spark can analyze the entire chain before executing and make smart decisions (e.g., which partitions to compute, how to pipeline operations)
2. **Efficiency**: If you build a complex chain but only need 10 results (`take(10)`), Spark doesn't need to process all data
3. **Fault tolerance**: The chain of transformations IS the recovery plan — if data is lost, replay the chain

---

## 8.2 The Transformation Catalog

Let's walk through every transformation available in Spark 0.5.0, one by one.

### `map(f)` — Transform each element

```scala
def map[U: ClassManifest](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))
```

**What it does**: Apply function `f` to every element, producing a new element.

**Java analogy**: `stream.map(f)`

**Example**:
```
Input:  ["hello", "world", "spark"]
.map(_.length)
Output: [5, 5, 5]

Input:  [1, 2, 3, 4]
.map(x => x * x)
Output: [1, 4, 9, 16]
```

**How it works internally** (MappedRDD):
- Splits: same as parent
- Dependencies: OneToOneDependency
- Compute: `prev.iterator(split).map(f)` — wraps parent's iterator

### `filter(f)` — Keep only matching elements

```scala
def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))
```

**What it does**: Keep only elements where `f` returns true.

**Java analogy**: `stream.filter(predicate)`

**Example**:
```
Input:  [1, 2, 3, 4, 5, 6]
.filter(x => x % 2 == 0)
Output: [2, 4, 6]
```

**How it works internally** (FilteredRDD):
- Splits: same as parent (but some may now have fewer elements)
- Dependencies: OneToOneDependency
- Compute: `prev.iterator(split).filter(f)`

### `flatMap(f)` — One-to-many mapping

```scala
def flatMap[U: ClassManifest](f: T => TraversableOnce[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))
```

**What it does**: Apply `f` to each element. `f` returns a collection for each input. All collections are flattened into one stream.

**Java analogy**: `stream.flatMap(f)` 

**Example**:
```
Input:  ["hello world", "foo bar baz"]
.flatMap(line => line.split(" "))
Output: ["hello", "world", "foo", "bar", "baz"]

Input:  [3, 1, 2]
.flatMap(n => (1 to n))
Output: [1, 2, 3, 1, 1, 2]
         ↑ from 3  ↑1  ↑2
```

**How it works internally** (FlatMappedRDD):
- Splits: same as parent
- Dependencies: OneToOneDependency  
- Compute: `prev.iterator(split).flatMap(f)`

### `sample(withReplacement, fraction, seed)` — Random sample

```scala
def sample(withReplacement: Boolean, fraction: Double, seed: Int): RDD[T] =
    new SampledRDD(this, withReplacement, fraction, seed)
```

**What it does**: Return a random subset of elements.

**Java analogy**: No direct equivalent — like randomly selecting items from a list.

**Parameters**:
- `withReplacement`: Can the same element be picked more than once?
- `fraction`: What proportion to sample (0.0 to 1.0)
- `seed`: Random seed for reproducibility

**Example**:
```
Input:  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
.sample(false, 0.3, seed=42)
Output: [2, 5, 8]  (approximately 30% of elements, randomly chosen)
```

**How it works internally** (SampledRDD): Covered in Chapter 16.

### `union(other)` and `++` — Combine two RDDs

```scala
def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, Array(this, other))
def ++(other: RDD[T]): RDD[T] = this.union(other)
```

**What it does**: Concatenate two RDDs into one.

**Java analogy**: Like `List.addAll(otherList)` — combining two lists.

**Example**:
```
RDD A: [1, 2, 3]
RDD B: [4, 5, 6]
A.union(B): [1, 2, 3, 4, 5, 6]
A ++ B:     [1, 2, 3, 4, 5, 6]  (same thing)
```

**How it works internally** (UnionRDD): Covered in Chapter 11.

### `cartesian(other)` — Cross product

```scala
def cartesian[U: ClassManifest](other: RDD[U]): RDD[(T, U)] = 
    new CartesianRDD(sc, this, other)
```

**What it does**: Every element in RDD A paired with every element in RDD B.

**Java analogy**: Nested for-loop producing all pairs.

**Example**:
```
RDD A: [1, 2]
RDD B: ["a", "b", "c"]
A.cartesian(B): [(1,"a"), (1,"b"), (1,"c"), (2,"a"), (2,"b"), (2,"c")]
```

**Warning**: If A has N elements and B has M elements, the result has N×M elements. This gets big fast!

**How it works internally** (CartesianRDD): Covered in Chapter 12.

### `pipe(command)` — External process

```scala
def pipe(command: String): RDD[String] = new PipedRDD(this, command)
```

**What it does**: Pipe each partition's data through an external command's stdin/stdout.

**Java analogy**: Like `Runtime.getRuntime().exec(command)` with data piped through.

**Example**:
```
Input:  ["3", "1", "4", "1", "5"]
.pipe("sort")
Output: ["1", "1", "3", "4", "5"]  (sorted by external 'sort' command)

Input:  ["hello world", "foo bar"]
.pipe("wc -w")
Output: ["2", "2"]  (word count by external 'wc' command)
```

**How it works internally** (PipedRDD): Covered in Chapter 15.

### `glom()` — Collect each partition into an array

```scala
def glom(): RDD[Array[T]] = new GlommedRDD(this)
```

**What it does**: Instead of individual elements, each partition becomes a single array of all its elements.

**Example**:
```
Input RDD with 3 partitions:
  Partition 0: [1, 2, 3]
  Partition 1: [4, 5]
  Partition 2: [6]

.glom()
Output RDD with 3 partitions:
  Partition 0: [Array(1, 2, 3)]     — one element: the array [1,2,3]
  Partition 1: [Array(4, 5)]        — one element: the array [4,5]
  Partition 2: [Array(6)]           — one element: the array [6]
```

**How it works internally** (GlommedRDD):
```scala
class GlommedRDD[T: ClassManifest](prev: RDD[T]) extends RDD[Array[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = Array(prev.iterator(split).toArray).iterator
}
```
Collects the entire partition iterator into an array, then wraps that array in a single-element iterator.

### `mapPartitions(f)` — Process a whole partition at once

```scala
def mapPartitions[U: ClassManifest](f: Iterator[T] => Iterator[U]): RDD[U] =
    new MapPartitionsRDD(this, sc.clean(f))
```

**What it does**: Like `map`, but instead of processing one element at a time, you get the entire partition's iterator and return a new iterator.

**Java analogy**: No direct equivalent. Like being given the whole `List` instead of one element.

**Example**:
```
// Regular map: process one element
.map(x => x * 2)

// mapPartitions: process the whole partition
// Useful when you need setup/teardown per partition, e.g., opening a DB connection
.mapPartitions(iter => {
    val dbConnection = openConnection()    // once per partition, not per element!
    val results = iter.map(x => dbConnection.lookup(x))
    dbConnection.close()
    results
})
```

### `groupBy(f)` — Group elements by a key function

```scala
def groupBy[K: ClassManifest](f: T => K, numSplits: Int): RDD[(K, Seq[T])] = {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(numSplits)
}
```

**What it does**: Groups elements by a key extracted by function `f`.

**Example**:
```
Input:  ["apple", "banana", "avocado", "blueberry", "cherry"]
.groupBy(word => word.charAt(0))
Output: [('a', ["apple", "avocado"]), ('b', ["banana", "blueberry"]), ('c', ["cherry"])]
```

**Note**: This internally uses `map` + `groupByKey`, which involves a shuffle (Chapter 13).

---

## 8.3 Transformation Summary Table

| Transformation | What It Creates | Splits | Dependency | Shuffle? |
|---------------|----------------|--------|------------|----------|
| `map(f)` | MappedRDD | Same as parent | OneToOne | No |
| `flatMap(f)` | FlatMappedRDD | Same as parent | OneToOne | No |
| `filter(f)` | FilteredRDD | Same as parent | OneToOne | No |
| `sample(...)` | SampledRDD | Same as parent | OneToOne | No |
| `union(other)` | UnionRDD | Concatenation | Range | No |
| `cartesian(other)` | CartesianRDD | Product (N×M) | Narrow (custom) | No |
| `pipe(cmd)` | PipedRDD | Same as parent | OneToOne | No |
| `glom()` | GlommedRDD | Same as parent | OneToOne | No |
| `mapPartitions(f)` | MapPartitionsRDD | Same as parent | OneToOne | No |
| `groupBy(f)` | ShuffledRDD (via map + groupByKey) | New | Shuffle | **Yes** |

Notice: **Almost all transformations are narrow (no shuffle)**. Only operations that need to rearrange data by key (like `groupBy`) involve a shuffle.

---

## 8.4 The Golden Rule

> **A transformation creates a new RDD object. It does NOT compute any data.**

Every time you call `.map(...)`, `.filter(...)`, `.flatMap(...)`, etc., you are:
1. Creating a new Scala/Java object in memory (lightweight — a few bytes)
2. Setting its `splits`, `dependencies`, and `compute` function
3. Returning it to you

That's all. The data doesn't move, doesn't get read, doesn't get computed. The computation happens later, when you call an action.

---

**Next Chapter**: [Chapter 9: Actions — When Computation Actually Happens →](Chapter-09-Actions.md)
