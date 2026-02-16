# Chapter 10: HadoopRDD — Reading Data from the Outside World

Every data pipeline starts somewhere. In Spark, data usually enters through a **HadoopRDD** — the RDD that reads from external storage like HDFS, the local filesystem, or Amazon S3.

---

## 10.1 What Problem Does It Solve?

When you write:
```scala
val lines = sc.textFile("hdfs://cluster/data/logs.txt")
```

Spark needs to:
1. Figure out where the file's data blocks live across the cluster
2. Create one partition per block
3. Read each block on the machine where it's stored (data locality)

`HadoopRDD` handles all of this by leveraging Hadoop's existing `InputFormat` system.

---

## 10.2 The Class Declaration

```scala
class HadoopRDD[K, V](
    sc: SparkContext,
    @transient conf: JobConf,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends RDD[(K, V)](sc) {
```

**In Java terms:**
```java
public class HadoopRDD<K, V> extends RDD<Pair<K, V>> {
    private SerializableWritable<JobConf> serializableConf;
    private Class<? extends InputFormat<K, V>> inputFormatClass;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private int minSplits;
    
    // conf is transient — the serializable version is used instead
}
```

Key parameters:
| Parameter | What It Is | Example |
|-----------|-----------|---------|
| `conf` | Hadoop configuration (file paths, settings) | Contains `"hdfs://cluster/data/logs.txt"` |
| `inputFormatClass` | How to read the file | `TextInputFormat` for text files |
| `keyClass` | Type of keys | `LongWritable` (line number) for text files |
| `valueClass` | Type of values | `Text` (line content) for text files |
| `minSplits` | Minimum number of partitions desired | `2` |

---

## 10.3 Property ①: Splits — One Per HDFS Block

```scala
@transient
val splits_ : Array[Split] = {
    val inputFormat = createInputFormat(conf)
    val inputSplits = inputFormat.getSplits(conf, minSplits)
    val array = new Array[Split](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopSplit(id, i, inputSplits(i))
    }
    array
}

override def splits = splits_
```

**What happens step by step:**

1. **Create an InputFormat** — Hadoop's `TextInputFormat` knows how to read text files
2. **Ask Hadoop for splits** — `getSplits()` queries HDFS: "Where are the blocks of this file?"
3. **Wrap each Hadoop split** in a `HadoopSplit` object

For a 640 MB file with 128 MB HDFS blocks:
```
HDFS file: /data/logs.txt (640 MB)

Block 0: bytes 0–128MB       → on machines [A, C, E]
Block 1: bytes 128–256MB     → on machines [B, D, F]
Block 2: bytes 256–384MB     → on machines [A, B, C]
Block 3: bytes 384–512MB     → on machines [D, E, F]
Block 4: bytes 512–640MB     → on machines [A, D, E]

→ 5 HadoopSplits created: [Split(0), Split(1), Split(2), Split(3), Split(4)]
```

---

## 10.4 Property ②: Compute — Read Records from a File Block

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
                case eof: EOFException => finished = true
            }
            gotNext = true
        }
        if (finished) reader.close()
        !finished
    }

    override def next: (K, V) = {
        if (!gotNext) finished = !reader.next(key, value)
        if (finished) throw new NoSuchElementException("End of stream")
        gotNext = false
        (key, value)
    }
}
```

**In Java terms (simplified):**
```java
public Iterator<Pair<K, V>> compute(Split theSplit) {
    HadoopSplit split = (HadoopSplit) theSplit;
    
    // Open a reader for this specific file block
    InputFormat<K, V> fmt = createInputFormat(conf);
    RecordReader<K, V> reader = fmt.getRecordReader(split.inputSplit, conf, NULL);
    
    // Return an iterator that reads one record at a time
    return new Iterator<Pair<K, V>>() {
        K key = reader.createKey();       // reusable key object
        V value = reader.createValue();   // reusable value object
        
        public boolean hasNext() {
            return reader.next(key, value);  // reads next record into key/value
        }
        
        public Pair<K, V> next() {
            return new Pair<>(key, value);
        }
    };
}
```

For `TextInputFormat`, each call to `reader.next()` reads one line:
- `key` = byte offset of the line (like a line number)
- `value` = the text content of the line

---

## 10.5 Property ③: Dependencies — None!

```scala
override val dependencies: List[Dependency[_]] = Nil   // empty list
```

A HadoopRDD has **no parent RDDs**. It reads from external storage, not from another RDD. It's a **root node** in the dependency graph — the starting point of every pipeline.

---

## 10.6 Property ⑤: Preferred Locations — Data Locality

```scala
override def preferredLocations(split: Split) = {
    val hadoopSplit = split.asInstanceOf[HadoopSplit]
    hadoopSplit.inputSplit.value.getLocations.filter(_ != "localhost")
}
```

This is where **data locality** happens. Each HDFS block is stored on specific machines (typically 3 replicas). `getLocations` returns those machine names.

Spark's scheduler uses this hint: "If possible, run the task for Split 0 on machine A, C, or E — because that's where the data is."

```
Split 0 → preferredLocations: ["machineA", "machineC", "machineE"]
Split 1 → preferredLocations: ["machineB", "machineD", "machineF"]
```

This avoids reading data over the network — the computation goes **to the data**, not the other way around.

---

## 10.7 NewHadoopRDD — The Newer API

`NewHadoopRDD` does the same thing but uses Hadoop's newer `mapreduce` API (instead of the older `mapred` API). The logic is identical:

| | HadoopRDD | NewHadoopRDD |
|---|---|---|
| **API** | `org.apache.hadoop.mapred` | `org.apache.hadoop.mapreduce` |
| **Config** | `JobConf` | `Configuration` |
| **Reader** | `RecordReader` (old) | `RecordReader` (new) |
| **Logic** | Same | Same |

---

## 10.8 Summary

HadoopRDD is the **source** RDD. It's where data enters the Spark world.

| Property | HadoopRDD's Implementation |
|----------|---------------------------|
| **Splits** | One per HDFS block (from `InputFormat.getSplits()`) |
| **Compute** | Open a `RecordReader`, read records one by one |
| **Dependencies** | None (root node) |
| **Partitioner** | None |
| **Preferred Locations** | HDFS block locations (data locality!) |

---

**Next Chapter**: [Chapter 11: UnionRDD — Combining Datasets →](Chapter-11-UnionRDD.md)
