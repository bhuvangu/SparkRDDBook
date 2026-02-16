# Chapter 9: Actions — When Computation Actually Happens

Transformations build a chain of RDD objects. Actions **pull the trigger** — they cause Spark to actually execute the computation and return results. This chapter explains how each action works and what happens under the hood.

---

## 9.1 The Key Difference: Transformation vs Action

| | Transformation | Action |
|---|---|---|
| **Returns** | A new RDD | A value (array, number, etc.) |
| **When it runs** | Never (lazy) | Immediately (eager) |
| **What it does** | Builds an RDD object | Calls `sc.runJob()` to execute on the cluster |
| **Examples** | map, filter, flatMap | collect, count, reduce, take |

The critical method is `sc.runJob(rdd, function)`. When any action is called, it ultimately calls `runJob`, which:

1. **Walks** the RDD dependency graph
2. **Divides** it into stages (splitting at shuffle boundaries)
3. **Creates tasks** — one per partition per stage
4. **Dispatches** tasks to worker machines
5. **Collects** results back to the driver program

---

## 9.2 `collect()` — Get All the Data

```scala
def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
}
```

**In Java terms:**
```java
public T[] collect() {
    // Step 1: Run on every partition: convert the iterator to an array
    T[][] results = sparkContext.runJob(this, iter -> toArray(iter));
    // results[0] = data from partition 0
    // results[1] = data from partition 1
    // results[2] = data from partition 2
    // ...
    
    // Step 2: Concatenate all partition results into one big array
    return concatenateArrays(results);
}
```

**What happens**:
1. For each partition, call `iter.toArray` — collect all elements into an array
2. Each partition returns its array to the driver machine
3. The driver concatenates all arrays into one

```
Partition 0: [1, 2, 3]  →  Array(1, 2, 3)  ─┐
Partition 1: [4, 5]     →  Array(4, 5)      ─┼─→ Array(1, 2, 3, 4, 5, 6)
Partition 2: [6]        →  Array(6)          ─┘
```

**⚠️ Warning**: This brings ALL data to the driver machine. If the RDD has millions of elements, this will run out of memory. Use `collect()` only on small result sets.

---

## 9.3 `count()` — How Many Elements?

```scala
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
```

**In Java terms:**
```java
public long count() {
    // Step 1: On each partition, count the elements
    long[] partitionCounts = sparkContext.runJob(this, iter -> {
        long count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        return count;
    });
    // partitionCounts[0] = count from partition 0
    // partitionCounts[1] = count from partition 1
    // ...
    
    // Step 2: Sum up all partition counts
    long total = 0;
    for (long c : partitionCounts) total += c;
    return total;
}
```

**What happens**:
1. Each partition counts its elements locally (fast — just incrementing a counter)
2. Each partition sends a single number to the driver
3. The driver sums the numbers

```
Partition 0: [1, 2, 3]  →  count = 3  ─┐
Partition 1: [4, 5]     →  count = 2  ─┼─→ sum = 3 + 2 + 1 = 6
Partition 2: [6]        →  count = 1  ─┘
```

Notice how efficient this is: each partition sends **one number** over the network, not the actual data.

---

## 9.4 `reduce(f)` — Combine All Elements

```scala
def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
        if (iter.hasNext) {
            Some(iter.reduceLeft(cleanF))
        } else {
            None
        }
    }
    val options = sc.runJob(this, reducePartition)
    val results = new ArrayBuffer[T]
    for (opt <- options; elem <- opt) {
        results += elem
    }
    if (results.size == 0) {
        throw new UnsupportedOperationException("empty collection")
    } else {
        return results.reduceLeft(cleanF)
    }
}
```

**In Java terms:**
```java
public T reduce(BinaryOperator<T> f) {
    // Step 1: Reduce within each partition
    Optional<T>[] partialResults = sparkContext.runJob(this, iter -> {
        if (!iter.hasNext()) return Optional.empty();
        T result = iter.next();
        while (iter.hasNext()) {
            result = f.apply(result, iter.next());
        }
        return Optional.of(result);
    });
    
    // Step 2: Collect non-empty results
    List<T> results = new ArrayList<>();
    for (Optional<T> opt : partialResults) {
        opt.ifPresent(results::add);
    }
    
    // Step 3: Reduce the partial results on the driver
    if (results.isEmpty()) throw new UnsupportedOperationException("empty collection");
    T finalResult = results.get(0);
    for (int i = 1; i < results.size(); i++) {
        finalResult = f.apply(finalResult, results.get(i));
    }
    return finalResult;
}
```

**What happens — two-phase reduction**:

**Phase 1**: Each partition reduces its own elements. Example with `reduce(_ + _)` (sum):
```
Partition 0: [1, 2, 3]  →  1 + 2 + 3 = 6
Partition 1: [4, 5]     →  4 + 5 = 9
Partition 2: [6]        →  6
```

**Phase 2**: The driver reduces the partial results:
```
Driver: 6 + 9 + 6 = 21
```

This is the classic **map-reduce pattern**: compute partial results locally, then combine them. Each partition sends only **one value** to the driver.

---

## 9.5 `fold(zeroValue)(f)` — Reduce with a Starting Value

```scala
def fold(zeroValue: T)(op: (T, T) => T): T = {
    val cleanOp = sc.clean(op)
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp))
    return results.fold(zeroValue)(cleanOp)
}
```

**In Java terms:**
```java
public T fold(T zeroValue, BinaryOperator<T> op) {
    // Step 1: Fold within each partition (starting from zeroValue)
    T[] partialResults = sparkContext.runJob(this, iter -> {
        T result = zeroValue;
        while (iter.hasNext()) {
            result = op.apply(result, iter.next());
        }
        return result;
    });
    
    // Step 2: Fold the partial results on the driver
    T finalResult = zeroValue;
    for (T partial : partialResults) {
        finalResult = op.apply(finalResult, partial);
    }
    return finalResult;
}
```

Like `reduce`, but starts with a `zeroValue`. This means empty partitions produce `zeroValue` instead of crashing.

**Example** — Sum with `fold(0)(_ + _)`:
```
Partition 0: [1, 2, 3]  →  0 + 1 + 2 + 3 = 6
Partition 1: []          →  0 (empty partition, returns zeroValue)
Partition 2: [4, 5]     →  0 + 4 + 5 = 9

Driver: 0 + 6 + 0 + 9 = 15
```

---

## 9.6 `aggregate(zeroValue)(seqOp, combOp)` — The Most Flexible Reduction

```scala
def aggregate[U: ClassManifest](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val results = sc.runJob(this,
        (iter: Iterator[T]) => iter.aggregate(zeroValue)(cleanSeqOp, cleanCombOp))
    return results.fold(zeroValue)(cleanCombOp)
}
```

Unlike `reduce` and `fold`, `aggregate` can return a **different type** than the elements. It uses two functions:
- `seqOp`: How to merge an element `T` into an accumulator `U` (within a partition)
- `combOp`: How to merge two accumulators `U` (across partitions)

**Example** — Computing average (needs both sum and count):
```java
// Java-style pseudocode
Pair<Integer, Integer> result = rdd.aggregate(
    new Pair<>(0, 0),                            // zeroValue: (sum=0, count=0)
    (acc, x) -> new Pair<>(acc.sum + x, acc.count + 1),  // seqOp: add element
    (a, b) -> new Pair<>(a.sum + b.sum, a.count + b.count) // combOp: merge
);
double average = result.sum / (double) result.count;
```

```
Partition 0: [10, 20, 30]  →  seqOp: (0,0) → (10,1) → (30,2) → (60,3)
Partition 1: [40, 50]      →  seqOp: (0,0) → (40,1) → (90,2)

Driver combOp: (60,3) + (90,2) = (150, 5)
Average = 150 / 5 = 30.0
```

---

## 9.7 `take(num)` — Get First N Elements

```scala
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
```

**In Java terms:**
```java
public T[] take(int num) {
    if (num == 0) return new T[0];
    
    List<T> buffer = new ArrayList<>();
    int partitionIndex = 0;
    
    while (buffer.size() < num && partitionIndex < splits().length) {
        int left = num - buffer.size();  // how many more do we need?
        
        // Run job on ONLY this one partition
        T[] result = sparkContext.runJob(this, 
            iter -> takeFirst(iter, left),
            new int[]{partitionIndex},   // only partition 'partitionIndex'
            true);
        
        buffer.addAll(Arrays.asList(result));
        partitionIndex++;
    }
    
    return buffer.toArray();
}
```

**What happens**:
1. Start with partition 0. Take up to `num` elements from it.
2. If we got enough → done!
3. If not → move to partition 1, take remaining needed.
4. Continue until we have `num` elements or run out of partitions.

**Key insight**: `take()` is smart — it **only computes the partitions it needs**. If you have 100 partitions but `take(5)` finds all 5 elements in partition 0, partitions 1–99 are never touched.

```
take(5) from an RDD with 3 partitions:

Partition 0: [a, b, c]  →  take 5, got 3  →  need 2 more
Partition 1: [d, e, f]  →  take 2, got 2  →  done!
Partition 2: [g, h]     →  NEVER COMPUTED (we already have 5)

Result: [a, b, c, d, e]
```

---

## 9.8 `first()` — Get the First Element

```scala
def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
}
```

Simply calls `take(1)` and extracts the single element. Very efficient — only computes partition 0 (usually).

---

## 9.9 `foreach(f)` — Execute a Function on Every Element

```scala
def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
}
```

**In Java terms:**
```java
public void foreach(Consumer<T> f) {
    sparkContext.runJob(this, iter -> {
        while (iter.hasNext()) {
            f.accept(iter.next());
        }
    });
}
```

Runs function `f` on every element across the cluster. **Returns nothing.** Used for side effects (e.g., writing to an external database, printing).

**Important**: `f` runs on **worker machines**, not on the driver. If `f` modifies a local variable on the driver, it won't work as expected — the workers have copies, not the original.

---

## 9.10 `saveAsTextFile(path)` — Write to Disk

```scala
def saveAsTextFile(path: String) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
        .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
}
```

This is technically an action because it triggers computation. It:
1. Converts each element to a string
2. Wraps it as a Hadoop key-value pair
3. Writes to HDFS (or local filesystem) using Hadoop's `TextOutputFormat`

Each partition writes its own file:
```
output/
  part-00000    (partition 0's data)
  part-00001    (partition 1's data)
  part-00002    (partition 2's data)
  _SUCCESS      (marker file)
```

---

## 9.11 `takeSample(withReplacement, num, seed)` — Get Exactly N Random Elements

```scala
def takeSample(withReplacement: Boolean, num: Int, seed: Int): Array[T] = {
    var fraction = 0.0
    var total = 0
    var multiplier = 3.0
    var initialCount = count()     // ACTION 1: count all elements
    // ...compute the right fraction to sample...
    var samples = this.sample(withReplacement, fraction, seed).collect()  // ACTION 2: collect sample
    while (samples.length < total) {
        samples = this.sample(withReplacement, fraction, seed).collect()
    }
    val arr = samples.take(total)
    return arr
}
```

This is interesting — it calls **multiple actions**: first `count()` to know how many elements exist, then repeatedly `sample().collect()` until it gets enough. This shows that actions can be composed.

---

## 9.12 The Pattern: Two-Phase Execution

Almost every action follows this pattern:

```
┌─────────────────────────────────────────────────────┐
│                    DRIVER MACHINE                     │
│                                                       │
│  1. Call sc.runJob(rdd, partitionFunction)            │
│                                                       │
│         ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│         │ Task 0  │  │ Task 1  │  │ Task 2  │       │
│         └────┬────┘  └────┬────┘  └────┬────┘       │
│              │            │            │              │
├──────────────┼────────────┼────────────┼──────────────┤
│              ↓            ↓            ↓              │
│         ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│ WORKERS │Worker A │  │Worker B │  │Worker C │       │
│         │         │  │         │  │         │       │
│         │compute  │  │compute  │  │compute  │       │
│         │partition│  │partition│  │partition│       │
│         │   0     │  │   1     │  │   2     │       │
│         │         │  │         │  │         │       │
│         │result₀  │  │result₁  │  │result₂  │       │
│         └────┬────┘  └────┬────┘  └────┬────┘       │
│              │            │            │              │
├──────────────┼────────────┼────────────┼──────────────┤
│              ↓            ↓            ↓              │
│  2. Combine partial results on driver                 │
│     collect: concatenate arrays                       │
│     count:   sum numbers                              │
│     reduce:  reduce partial values                    │
│                                                       │
│  3. Return final result                               │
└─────────────────────────────────────────────────────┘
```

---

## 9.13 Summary

| Action | Returns | What Each Partition Sends to Driver | Driver Combines By |
|--------|---------|------------------------------------|--------------------|
| `collect()` | `Array[T]` | All elements as an array | Concatenation |
| `count()` | `Long` | One number (count) | Sum |
| `reduce(f)` | `T` | One value (partial reduction) | Apply `f` again |
| `fold(z)(f)` | `T` | One value (fold with zero) | Apply `f` again |
| `aggregate(z)(s,c)` | `U` | One accumulator value | Apply combiner |
| `take(n)` | `Array[T]` | First N elements (sequential) | Concatenation |
| `first()` | `T` | First element | Extract single |
| `foreach(f)` | `Unit` (nothing) | Nothing | Nothing |
| `saveAsTextFile(p)` | `Unit` | Nothing (writes to HDFS) | Nothing |

The pattern is always the same:
1. **Distribute**: Send a function to run on each partition
2. **Execute**: Each partition processes its data locally
3. **Collect**: Gather partial results back to the driver
4. **Combine**: Merge partial results into the final answer

---

**Next Chapter**: [Chapter 10: HadoopRDD — Reading Data from the Outside World →](Chapter-10-HadoopRDD.md)
