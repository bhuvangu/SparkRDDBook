# Chapter 7: The Base RDD Class — The Contract

Now that you understand all five building blocks (splits, compute, dependencies, partitioner, preferred locations), it's time to see how they come together in the actual base `RDD` class. This chapter walks through the entire `RDD.scala` file — the heart of Spark.

---

## 7.1 The Class Declaration

```scala
abstract class RDD[T: ClassManifest](@transient sc: SparkContext) extends Serializable {
```

**In Java terms:**
```java
public abstract class RDD<T> implements Serializable {
    private transient SparkContext sc;
    
    public RDD(SparkContext sc) {
        this.sc = sc;
    }
}
```

Let's break this down:

| Part | Meaning |
|------|---------|
| `abstract` | You can't create an RDD directly — you must use a subclass (MappedRDD, HadoopRDD, etc.) |
| `RDD[T]` | Generic type T — the RDD holds elements of type T (could be String, Integer, Pair, etc.) |
| `ClassManifest` | Keeps runtime type info for T (ignore this — just means "we know what T is at runtime") |
| `@transient sc: SparkContext` | The SparkContext — the "entry point" to Spark. `@transient` means don't serialize it. |
| `Serializable` | RDD objects can be sent over the network to worker machines |

---

## 7.2 The Five Properties — The Abstract Contract

```scala
// Methods that MUST be implemented by subclasses
def splits: Array[Split]
def compute(split: Split): Iterator[T]
@transient val dependencies: List[Dependency[_]]

// OPTIONALLY overridden by subclasses
val partitioner: Option[Partitioner] = None
def preferredLocations(split: Split): Seq[String] = Nil
```

**In Java terms:**
```java
public abstract class RDD<T> implements Serializable {
    
    // === MUST IMPLEMENT ===
    
    /** All the partitions of this RDD */
    public abstract Split[] splits();
    
    /** How to compute the data for one partition */
    public abstract Iterator<T> compute(Split split);
    
    /** Which parent RDDs this was created from */
    public abstract List<Dependency<?>> dependencies();
    
    // === OPTIONAL (have defaults) ===
    
    /** How keys are distributed across partitions (default: none) */
    public Optional<Partitioner> partitioner() {
        return Optional.empty();
    }
    
    /** Where each partition should preferably be computed (default: no preference) */
    public List<String> preferredLocations(Split split) {
        return Collections.emptyList();
    }
}
```

This is the **contract** that every RDD subclass must fulfill. If you implement these five things, you have a valid RDD that Spark can schedule, execute, and recover from failures.

---

## 7.3 The RDD's Identity

```scala
def context = sc

// Get a unique ID for this RDD
val id = sc.newRddId()
```

Every RDD gets:
- A reference to the `SparkContext` (the master coordinator)
- A unique `id` (a counter that increments for each new RDD)

The `id` is used internally for tracking — for example, `HadoopSplit` uses `rddId` in its `hashCode()` to distinguish splits from different RDDs.

---

## 7.4 Caching

```scala
// Variables relating to caching
private var shouldCache = false

// Change this RDD's caching
def cache(): RDD[T] = {
    shouldCache = true
    this
}
```

**In Java terms:**
```java
private boolean shouldCache = false;

public RDD<T> cache() {
    this.shouldCache = true;
    return this;   // returns itself for method chaining
}
```

`cache()` is deceptively simple. It just flips a boolean flag. No data is loaded into memory yet! The caching happens later, when data is actually computed.

Notice it returns `this` — this enables method chaining like:
```scala
val cachedRDD = rdd.filter(...).cache()
```

---

## 7.5 The `iterator()` Method — The Gateway

```scala
final def iterator(split: Split): Iterator[T] = {
    if (shouldCache) {
        SparkEnv.get.cacheTracker.getOrCompute[T](this, split)
    } else {
        compute(split)
    }
}
```

**In Java terms:**
```java
public final Iterator<T> iterator(Split split) {
    if (shouldCache) {
        return SparkEnv.get().getCacheTracker().getOrCompute(this, split);
    } else {
        return compute(split);
    }
}
```

This is the **single entry point** for getting data from an RDD. Every piece of code that wants data calls `iterator()`, never `compute()` directly.

The logic:
1. If caching is enabled → check the cache. If the data is already cached, return it. If not, compute it, cache it, then return it.
2. If caching is not enabled → just call `compute()` directly.

Notice it's `final` — subclasses can't override it. This ensures the cache check always happens.

---

## 7.6 Transformation Methods

The base RDD class provides **transformation methods** that create new RDDs. Let's look at each one:

```scala
def map[U: ClassManifest](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))

def flatMap[U: ClassManifest](f: T => TraversableOnce[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))

def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

def sample(withReplacement: Boolean, fraction: Double, seed: Int): RDD[T] =
    new SampledRDD(this, withReplacement, fraction, seed)

def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, Array(this, other))

def ++(other: RDD[T]): RDD[T] = this.union(other)

def cartesian[U: ClassManifest](other: RDD[U]): RDD[(T, U)] = 
    new CartesianRDD(sc, this, other)

def pipe(command: String): RDD[String] = new PipedRDD(this, command)

def glom(): RDD[Array[T]] = new GlommedRDD(this)

def mapPartitions[U: ClassManifest](f: Iterator[T] => Iterator[U]): RDD[U] =
    new MapPartitionsRDD(this, sc.clean(f))
```

**In Java terms (simplified):**
```java
public <U> RDD<U> map(Function<T, U> f) {
    return new MappedRDD<>(this, sparkContext.clean(f));
}

public <U> RDD<U> flatMap(Function<T, Iterable<U>> f) {
    return new FlatMappedRDD<>(this, sparkContext.clean(f));
}

public RDD<T> filter(Predicate<T> f) {
    return new FilteredRDD<>(this, sparkContext.clean(f));
}

public RDD<T> union(RDD<T> other) {
    return new UnionRDD<>(sparkContext, Arrays.asList(this, other));
}

public <U> RDD<Pair<T, U>> cartesian(RDD<U> other) {
    return new CartesianRDD<>(sparkContext, this, other);
}

public RDD<String> pipe(String command) {
    return new PipedRDD<>(this, command);
}
```

### Key observations:

1. **Every transformation returns a new RDD object**. The original RDD is unchanged (immutable).

2. **No computation happens**. Calling `rdd.map(f)` just creates a `MappedRDD` object with a pointer to the parent and the function `f`. It doesn't apply `f` to any data.

3. **`sc.clean(f)`** — This is a utility that makes the function `f` serializable by removing unnecessary references to outer classes. Without this, the function might accidentally capture and try to serialize things like the entire SparkContext.

4. **Each transformation creates a specific RDD subclass** — `map` creates `MappedRDD`, `filter` creates `FilteredRDD`, etc. Each subclass knows how to compute itself (Chapter 4) and what its dependencies are (Chapter 5).

We'll explore each transformation in detail in Chapter 8.

---

## 7.7 Action Methods

Actions are different from transformations — they **actually trigger computation** and return results:

```scala
def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
}

def count(): Long = {
    sc.runJob(this, (iter: Iterator[T]) => {
        var result = 0L
        while (iter.hasNext) {
            result += 1L
            iter.next
        }
        result
    }).sum
}

def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
        if (iter.hasNext) Some(iter.reduceLeft(cleanF)) else None
    }
    val options = sc.runJob(this, reducePartition)
    val results = new ArrayBuffer[T]
    for (opt <- options; elem <- opt) results += elem
    if (results.size == 0) throw new UnsupportedOperationException("empty collection")
    else return results.reduceLeft(cleanF)
}

def take(num: Int): Array[T] = {
    if (num == 0) return new Array[T](0)
    val buf = new ArrayBuffer[T]
    var p = 0
    while (buf.size < num && p < splits.size) {
        val left = num - buf.size
        val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, Array(p), true)
        buf ++= res(0)
        if (buf.size == num) return buf.toArray
        p += 1
    }
    return buf.toArray
}

def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
}
```

The key pattern: every action calls **`sc.runJob(this, function)`**. This is the magic call that:
1. Analyzes the RDD's dependency graph
2. Splits it into stages
3. Dispatches tasks to worker machines
4. Collects results back

We'll explore actions in detail in Chapter 9.

---

## 7.8 Save Methods

```scala
def saveAsTextFile(path: String) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
        .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
}

def saveAsObjectFile(path: String) {
    this.glom
        .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
        .saveAsSequenceFile(path)
}
```

`saveAsTextFile` converts each element to a string and writes it to HDFS (or local filesystem) as a text file. `saveAsObjectFile` serializes elements as bytes and saves them in a Hadoop SequenceFile format.

Notice that `saveAsTextFile` actually works by:
1. `map` each element `x` to a `(NullWritable, Text)` pair (Hadoop's way of writing text files)
2. Call `saveAsHadoopFile` (defined in `PairRDDFunctions` — Chapter 17)

It reuses existing transformations internally!

---

## 7.9 The Complete Picture — All in One Diagram

Here's the full base `RDD` class structure:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    abstract class RDD[T]                                │
├─────────────────────────────────────────────────────────────────────────┤
│ IDENTITY                                                                │
│   val id: Int                    // unique RDD identifier               │
│   def context: SparkContext      // link to the Spark master            │
├─────────────────────────────────────────────────────────────────────────┤
│ THE 5 PROPERTIES (Abstract Contract)                                    │
│   def splits: Array[Split]                    // ① partitions           │
│   def compute(split: Split): Iterator[T]      // ② recipe per split    │
│   val dependencies: List[Dependency[_]]       // ③ parent links         │
│   val partitioner: Option[Partitioner] = None // ④ key distribution     │
│   def preferredLocations(split: Split) = Nil  // ⑤ locality hints       │
├─────────────────────────────────────────────────────────────────────────┤
│ CACHING                                                                 │
│   def cache(): RDD[T]                         // mark for caching       │
│   final def iterator(split): Iterator[T]      // compute or read cache  │
├─────────────────────────────────────────────────────────────────────────┤
│ TRANSFORMATIONS (return new RDDs — lazy)                                │
│   def map(f): RDD[U]                                                    │
│   def flatMap(f): RDD[U]                                                │
│   def filter(f): RDD[T]                                                 │
│   def sample(...): RDD[T]                                               │
│   def union(other): RDD[T]                                              │
│   def cartesian(other): RDD[(T, U)]                                     │
│   def pipe(command): RDD[String]                                        │
│   def glom(): RDD[Array[T]]                                             │
│   def mapPartitions(f): RDD[U]                                          │
│   def groupBy(f): RDD[(K, Seq[T])]                                      │
├─────────────────────────────────────────────────────────────────────────┤
│ ACTIONS (trigger computation — eager)                                   │
│   def collect(): Array[T]                                               │
│   def count(): Long                                                     │
│   def reduce(f): T                                                      │
│   def fold(zero)(f): T                                                  │
│   def aggregate(zero)(seqOp, combOp): U                                 │
│   def take(num): Array[T]                                               │
│   def first(): T                                                        │
│   def foreach(f): Unit                                                  │
│   def saveAsTextFile(path): Unit                                        │
│   def saveAsObjectFile(path): Unit                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 7.10 The RDD Subclass Pattern

Every RDD subclass follows the same pattern. Let's use `FilteredRDD` as the template:

```scala
class FilteredRDD[T: ClassManifest](prev: RDD[T], f: T => Boolean) 
  extends RDD[T](prev.context) {
  
  override def splits = prev.splits                               // ① same splits as parent
  override val dependencies = List(new OneToOneDependency(prev))  // ③ one-to-one with parent
  override def compute(split: Split) = prev.iterator(split).filter(f) // ② apply filter
  // ④ partitioner: uses default (None)
  // ⑤ preferredLocations: uses default (Nil)
}
```

**In Java terms:**
```java
public class FilteredRDD<T> extends RDD<T> {
    private RDD<T> prev;
    private Predicate<T> f;
    
    public FilteredRDD(RDD<T> prev, Predicate<T> f) {
        super(prev.context());
        this.prev = prev;
        this.f = f;
    }
    
    public Split[] splits() {
        return prev.splits();           // same partitions as parent
    }
    
    public List<Dependency<?>> dependencies() {
        return List.of(new OneToOneDependency<>(prev));  // one-to-one
    }
    
    public Iterator<T> compute(Split split) {
        return new FilteringIterator<>(prev.iterator(split), f);  // filter parent's data
    }
    
    // partitioner() → Optional.empty()       (inherited default)
    // preferredLocations() → emptyList()     (inherited default)
}
```

Every subclass follows this same recipe:
1. Take parent RDD(s) as constructor parameters
2. Implement `splits`, `dependencies`, and `compute`
3. Optionally override `partitioner` and `preferredLocations`

---

## 7.11 Summary

| Aspect | What It Does |
|--------|-------------|
| **The contract** | Five abstract/overridable properties that every RDD must define |
| **Immutability** | An RDD object is never modified. Transformations always create new RDD objects. |
| **Laziness** | Transformations just build RDD objects. No computation until an action is called. |
| **Caching** | A boolean flag + the `iterator()` method that checks the cache before computing |
| **The `sc.runJob()` pattern** | Every action calls `sc.runJob(this, function)` to trigger actual distributed execution |
| **Serializable** | RDD objects are serialized and sent to workers. Functions are cleaned with `sc.clean()`. |

The base `RDD` class is like an **interface with convenience methods**. It defines:
- What every RDD must provide (the 5 properties)
- What you can do with any RDD (transformations + actions)

The actual intelligence is in the subclasses — each one defines its own splits, compute, and dependencies.

---

**Next Chapter**: [Chapter 8: Transformations — Creating New RDDs from Old Ones →](Chapter-08-Transformations.md)
