# Chapter 4: Compute — The Recipe for One Partition

In the previous chapter, you learned that a **Split** is a partition identifier — it says "I am partition #X" and may carry some metadata. But a Split doesn't contain any actual data.

So where does the data come from? That's the job of the **`compute()`** function.

---

## 4.1 The Real-World Analogy

Remember our book analogy? Each friend got a chunk of pages. But giving someone "pages 101–200" is not the same as giving them the actual content. They still need to:

1. **Open the book** to page 101
2. **Read** pages one by one
3. **Return** what they found

The Split is like saying "your job is pages 101–200." The `compute()` function is the **actual reading** — it opens the book, reads the pages, and returns the content.

In code terms:
- **Split** = "Process partition number 5"
- **compute()** = "Here's how to produce the actual data for partition 5"

---

## 4.2 The Method Signature

In the base `RDD` class (`RDD.scala`), `compute` is declared as an abstract method:

```scala
abstract class RDD[T: ClassManifest](@transient sc: SparkContext) extends Serializable {
    // ...
    def compute(split: Split): Iterator[T]
    // ...
}
```

**In Java terms:**
```java
public abstract class RDD<T> implements Serializable {
    
    public abstract Iterator<T> compute(Split split);
}
```

Let's understand each part:

| Part | Meaning |
|------|---------|
| `split: Split` | **Input**: The partition to compute. "Which chunk do you want?" |
| `Iterator[T]` | **Output**: A lazy stream of elements. Not an array — an iterator that produces elements one at a time. |
| Abstract | Every RDD subclass must provide its own implementation. There's no default way to compute — it depends on *what kind* of RDD this is. |

### Why an Iterator and not a List?

This is a crucial design choice. If `compute()` returned a `List[T]` (or `Array[T]`), it would mean: "Load all the data for this partition into memory at once."

For a partition with millions of elements, that would use enormous amounts of memory.

By returning an `Iterator[T]`, Spark says: "Give me elements **one at a time**, on demand." The data streams through memory without being fully loaded.

**Java equivalent:**
```java
// BAD — loads everything into memory
public List<String> compute(Split split) {
    List<String> allData = new ArrayList<>();
    // read millions of lines into allData...
    return allData;  // HUGE memory usage!
}

// GOOD — streams one element at a time
public Iterator<String> compute(Split split) {
    return new Iterator<String>() {
        // reads one line at a time from the file
        public boolean hasNext() { /* check if more lines */ }
        public String next() { /* read and return one line */ }
    };
}
```

---

## 4.3 Compute in Action: HadoopRDD

Let's start with the most fundamental RDD — one that reads data from a file. Here's `HadoopRDD.compute()` (from `HadoopRDD.scala`):

```scala
override def compute(theSplit: Split) = new Iterator[(K, V)] {
    val split = theSplit.asInstanceOf[HadoopSplit]
    var reader: RecordReader[K, V] = null

    val conf = serializableConf.value
    val fmt = createInputFormat(conf)
    reader = fmt.getRecordReader(split.inputSplit.value, conf, Reporter.NULL)

    val key: K = reader.createKey()
    val value: V = reader.createValue()
    var gotNext = false
    var finished = false

    override def hasNext: Boolean = {
        if (!gotNext) {
            try {
                finished = !reader.next(key, value)
            } catch {
                case eof: EOFException =>
                    finished = true
            }
            gotNext = true
        }
        if (finished) {
            reader.close()
        }
        !finished
    }

    override def next: (K, V) = {
        if (!gotNext) {
            finished = !reader.next(key, value)
        }
        if (finished) {
            throw new NoSuchElementException("End of stream")
        }
        gotNext = false
        (key, value)
    }
}
```

This looks complex, but let's translate it to Java step by step:

```java
// Java translation of HadoopRDD.compute()
public Iterator<Pair<K, V>> compute(Split theSplit) {
    
    HadoopSplit split = (HadoopSplit) theSplit;
    
    // Step 1: Create a Hadoop RecordReader for this file block
    JobConf conf = serializableConf.getValue();
    InputFormat<K, V> fmt = createInputFormat(conf);
    RecordReader<K, V> reader = fmt.getRecordReader(
        split.getInputSplit(), conf, Reporter.NULL);
    
    // Step 2: Return an iterator that reads records one by one
    return new Iterator<Pair<K, V>>() {
        K key = reader.createKey();
        V value = reader.createValue();
        boolean gotNext = false;
        boolean finished = false;
        
        public boolean hasNext() {
            if (!gotNext) {
                try {
                    finished = !reader.next(key, value);  // read next record
                } catch (EOFException e) {
                    finished = true;
                }
                gotNext = true;
            }
            if (finished) {
                reader.close();
            }
            return !finished;
        }
        
        public Pair<K, V> next() {
            if (!gotNext) {
                finished = !reader.next(key, value);
            }
            if (finished) {
                throw new NoSuchElementException("End of stream");
            }
            gotNext = false;
            return new Pair<>(key, value);
        }
    };
}
```

### What's happening:

1. **Cast the Split** to `HadoopSplit` — because we know it contains the HDFS block information
2. **Create a RecordReader** — Hadoop's way of reading key-value pairs from a file block. The reader knows the file path, byte offset, and length from the split.
3. **Return an Iterator** — Each call to `next()` reads one record (key-value pair) from the file. No data is loaded until you call `next()`.

**Think of it like**: Opening a `BufferedReader` on a file and reading lines one by one, except it's a distributed file system and the "file" is one block of a large file.

---

## 4.4 Compute in Action: MappedRDD

Now let's see something simpler. When you call `rdd.map(f)`, a `MappedRDD` is created. Here's its `compute()`:

```scala
class MappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).map(f)
}
```

Focus on the `compute` line:

```scala
override def compute(split: Split) = prev.iterator(split).map(f)
```

**In Java terms:**
```java
public Iterator<U> compute(Split split) {
    Iterator<T> parentData = prev.iterator(split);  // get parent's data for this split
    return new MappingIterator<>(parentData, f);     // wrap it to apply function f
}
```

What's happening:

1. **Get the parent's data** for this split: `prev.iterator(split)` — this calls the parent RDD's `iterator()` method, which in turn calls the parent's `compute()` (or reads from cache)
2. **Apply the function `f`** to each element as it streams through

Let's trace a concrete example:

```
Suppose:
- Parent RDD has data: ["apple", "banana", "cherry"] in Split 0
- You call: rdd.map(s => s.length)   // map each string to its length

When compute(Split 0) is called on MappedRDD:
1. prev.iterator(Split 0)  →  Iterator yielding "apple", "banana", "cherry"
2. .map(s => s.length)     →  Iterator yielding 5, 6, 6

No intermediate list is created. Each string flows through the function one at a time.
```

---

## 4.5 Compute in Action: FilteredRDD

Similarly, `rdd.filter(f)` creates a `FilteredRDD`:

```scala
class FilteredRDD[T: ClassManifest](prev: RDD[T], f: T => Boolean) 
  extends RDD[T](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).filter(f)
}
```

**In Java terms:**
```java
public Iterator<T> compute(Split split) {
    Iterator<T> parentData = prev.iterator(split);
    return new FilteringIterator<>(parentData, f);  // only pass through elements where f returns true
}
```

Example:
```
Parent data in Split 0: ["apple", "banana", "cherry", "avocado"]
Filter: s => s.startsWith("a")   // keep only strings starting with "a"

FilteredRDD.compute(Split 0):
1. prev.iterator(Split 0)         →  "apple", "banana", "cherry", "avocado"
2. .filter(s => s.startsWith("a"))  →  "apple", "avocado"
```

---

## 4.6 Compute in Action: FlatMappedRDD

`rdd.flatMap(f)` is like `map`, but the function returns multiple elements for each input:

```scala
class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => TraversableOnce[U])
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).flatMap(f)
}
```

**In Java terms:**
```java
public Iterator<U> compute(Split split) {
    Iterator<T> parentData = prev.iterator(split);
    return new FlatMappingIterator<>(parentData, f);
    // For each input element, f returns a collection.
    // The iterator "flattens" all these collections into a single stream.
}
```

Example:
```
Parent data in Split 0: ["hello world", "foo bar baz"]
FlatMap: s => s.split(" ")   // split each string into words

FlatMappedRDD.compute(Split 0):
1. prev.iterator(Split 0)     →  "hello world", "foo bar baz"
2. .flatMap(s => s.split(" "))  →  "hello", "world", "foo", "bar", "baz"

"hello world" produces 2 elements, "foo bar baz" produces 3 — all flattened into one stream.
```

---

## 4.7 Compute in Action: ShuffledRDD

Now let's look at a very different kind of `compute()` — one that reads data from the **network** instead of from a parent RDD. This is `ShuffledRDD.compute()` (from `ShuffledRDD.scala`):

```scala
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
```

**In Java terms:**
```java
public Iterator<Pair<K, C>> compute(Split split) {
    // Step 1: Create a HashMap to collect results
    HashMap<K, C> combiners = new HashMap<>();
    
    // Step 2: Define how to merge incoming key-value pairs
    BiConsumer<K, C> mergePair = (k, c) -> {
        C oldC = combiners.get(k);
        if (oldC == null) {
            combiners.put(k, c);
        } else {
            combiners.put(k, aggregator.mergeCombiners(oldC, c));
        }
    };
    
    // Step 3: Fetch shuffled data from the network
    ShuffleFetcher fetcher = SparkEnv.get().getShuffleFetcher();
    fetcher.fetch(dep.shuffleId, split.getIndex(), mergePair);
    
    // Step 4: Return an iterator over the merged results
    return combiners.entrySet().stream()
        .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
        .iterator();
}
```

This is fundamentally different from the previous examples:
- **MappedRDD/FilteredRDD**: Get data from the parent RDD's iterator (data flows through a pipeline)
- **ShuffledRDD**: Get data from the **network** via a `ShuffleFetcher` (data was redistributed across machines)

We'll explore ShuffledRDD in detail in Chapter 13. For now, the key point is: **`compute()` can get its data from anywhere — a file, a parent RDD, or the network.**

---

## 4.8 The Chaining Effect

Here's where it gets beautiful. When you chain transformations:

```scala
val rdd1 = sc.textFile("data.txt")          // HadoopRDD
val rdd2 = rdd1.filter(_.contains("error")) // FilteredRDD
val rdd3 = rdd2.map(_.toUpperCase)          // MappedRDD
```

And then call an action like `rdd3.collect()`, Spark calls `rdd3.compute(split)` for each split. Here's what happens:

```
rdd3.compute(Split 0):
  → rdd2.iterator(Split 0):
    → rdd2.compute(Split 0):
      → rdd1.iterator(Split 0):
        → rdd1.compute(Split 0):
          → Opens file, reads lines one by one
        ← yields: "INFO: started", "ERROR: disk full", "INFO: running", "ERROR: timeout"
      ← .filter(_.contains("error")):
        ← yields: "ERROR: disk full", "ERROR: timeout"
    ← .map(_.toUpperCase):
      ← yields: "ERROR: DISK FULL", "ERROR: TIMEOUT"
```

The data flows **backwards through the chain** — each RDD asks its parent for data, applies its function, and passes the result upstream. All happening element by element, through iterators.

**No intermediate collections are created.** A line is read from the file, tested by the filter, converted to uppercase, and delivered — before the next line is even read from the file.

This is called **pipelining**, and it's extremely memory-efficient.

---

## 4.9 `iterator()` vs `compute()` — The Cache Check

You might have noticed that in the chaining example, we sometimes call `iterator()` instead of `compute()` directly. Here's why:

```scala
// From the base RDD class
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
        // Check if this partition's data is already cached in memory.
        // If yes, return the cached data.
        // If no, call compute(), cache the result, then return it.
        return cacheTracker.getOrCompute(this, split);
    } else {
        // No caching — just compute fresh
        return compute(split);
    }
}
```

`iterator()` is a **wrapper** around `compute()` that adds caching:
- If the RDD has been `.cache()`'d and this partition's data is already in memory → return the cached data
- Otherwise → call `compute()` to produce the data fresh

This is why code always calls `prev.iterator(split)` rather than `prev.compute(split)` — it goes through the cache check.

---

## 4.10 Summary

| Question | Answer |
|----------|--------|
| What is `compute()`? | A method that produces the actual data for one partition. It's the "recipe" that turns a Split into data. |
| What does it return? | An `Iterator[T]` — a lazy stream of elements, produced one at a time. |
| Why an Iterator, not a List? | Memory efficiency. Data streams through without being fully loaded. |
| Does every RDD implement it? | Yes. It's abstract in the base class. Every subclass must define how to compute its data. |
| Where does the data come from? | Depends on the RDD type: from a file (HadoopRDD), from a parent RDD's iterator (MappedRDD, FilteredRDD), or from the network (ShuffledRDD). |
| What is `iterator()`? | A wrapper around `compute()` that checks the cache first. Always use `iterator()` when accessing a parent RDD's data. |
| What is pipelining? | When chained transformations are computed element-by-element through iterators, without creating intermediate collections. |

You now understand two of the five RDD properties: **splits** (Chapter 3) and **compute** (this chapter). Next, we'll tackle the third property: **dependencies** — how an RDD knows which other RDDs it came from.

---

**Next Chapter**: [Chapter 5: Dependency — Who Is Your Parent? →](Chapter-05-Dependency.md)
