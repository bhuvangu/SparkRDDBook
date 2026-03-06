# Chapter 6: ShuffleMapTask — Writing Shuffle Output

## 6.1 The Problem This Task Solves

In Book 1 (Chapter 13), we saw the shuffle from the consumer's perspective — ShuffledRDD fetches data from the network and merges it. But someone has to *produce* that data in the first place. That's the ShuffleMapTask.

Think about what `reduceByKey` needs. Before it can group all the "apple" counts together, the "apple" records — which are scattered across different partitions on different machines — need to be reorganized so that all "apple" records end up in the same place.

The ShuffleMapTask is the first half of that reorganization. Its job: **read your partition, sort every record into the right bucket based on its key, and write each bucket to a file on disk.**

It's a mail sorting machine. Letters (key-value pairs) come in. The machine reads the zip code (applies the partitioner to the key) and drops each letter into the right bin (output file). When all letters are sorted, the machine reports: "My bins are at this address."

---

## 6.2 The Four Steps

Every ShuffleMapTask does the same four things:

**Step 1: Read the data.** Call `rdd.iterator(split)` to get the elements for this partition. This triggers the entire compute pipeline from Book 1 — chaining back through flatMap, map, filter, all the way to the source.

**Step 2: Sort into buckets.** For each `(key, value)` pair, ask the partitioner: "Which output partition does this key belong to?" Then put it in the corresponding bucket. There's one bucket (a HashMap) per output partition.

**Step 3: Combine on the map side.** This is a crucial optimization. If the same key appears multiple times in this partition, don't write separate records — combine them first. Instead of writing `("apple", 1)` three times, write `("apple", 3)` once. This reduces the amount of data that needs to move across the network.

**Step 4: Write to disk and report.** Serialize each bucket to a file. Return the server URI so the reduce side knows where to fetch the data.

---

## 6.3 A Concrete Trace

Let's trace through the word count. This ShuffleMapTask processes partition 0, which contains:

```
("apple", 1), ("banana", 1), ("apple", 1), ("cherry", 1)
```

The `reduceByKey` uses `HashPartitioner(2)` — two output partitions.

**Step 1 — Read:** `rdd.iterator(split 0)` produces the four pairs above.

**Step 2+3 — Sort and combine:**

```
Processing ("apple", 1):
  "apple".hashCode() % 2 = 1  →  bucket[1]
  bucket[1] is empty for "apple" → store ("apple", 1)

Processing ("banana", 1):
  "banana".hashCode() % 2 = 0  →  bucket[0]
  bucket[0] is empty for "banana" → store ("banana", 1)

Processing ("apple", 1):
  "apple".hashCode() % 2 = 1  →  bucket[1]
  bucket[1] already has "apple" = 1 → combine: 1 + 1 = 2
  bucket[1] now has ("apple", 2)

Processing ("cherry", 1):
  "cherry".hashCode() % 2 = 0  →  bucket[0]
  bucket[0] is empty for "cherry" → store ("cherry", 1)

Final buckets:
  bucket[0] = {"banana": 1, "cherry": 1}
  bucket[1] = {"apple": 2}              ← combined! Not two separate ("apple",1) records
```

**Step 4 — Write:**

Each bucket gets written to a separate file. The file path has three parts:
 
```
shuffle / {shuffleId} / {mapPartition} / {reducePartition}
           │               │                │
           │               │                └── which bucket (= which reduce partition this data is for)
           │               └── which map task wrote this file (= which input partition was processed)
           └── which shuffle operation (a job can have multiple shuffles)
```

For our task (shuffleId=0, processing map partition 0, writing 2 buckets):

```
shuffle/0/0/0  →  ("banana", 1), ("cherry", 1)    ← bucket 0: data destined for reduce partition 0
shuffle/0/0/1  →  ("apple", 2)                     ← bucket 1: data destined for reduce partition 1
```

The task returns `"http://localhost:45678"` — the address of the HTTP server on this machine that serves these files. Later, when a reduce task needs the data for reduce partition 0, it will download `shuffle/0/0/0` from this address.

---

## 6.4 The Cluster-Wide Picture

We've traced one task on one machine. But remember from Chapter 1 — there are 16,000 splits, which means 16,000 ShuffleMapTasks. The scheduler distributes them across the cluster. Let's say we have 50 machines:

```
Machine A ran tasks for partitions 0–319      (320 tasks)
Machine B ran tasks for partitions 320–639    (320 tasks)
Machine C ran tasks for partitions 640–959    (320 tasks)
...
Machine AX ran tasks for partitions 15680–15999 (320 tasks)
```

Each task wrote its bucket files to the **local disk of the machine it ran on**. So after all 16,000 tasks complete:

```
Machine A's disk:
  shuffle/0/0/0, shuffle/0/0/1      ← partition 0's buckets
  shuffle/0/1/0, shuffle/0/1/1      ← partition 1's buckets
  ...
  shuffle/0/319/0, shuffle/0/319/1  ← partition 319's buckets

Machine B's disk:
  shuffle/0/320/0, shuffle/0/320/1
  shuffle/0/321/0, shuffle/0/321/1
  ...
  shuffle/0/639/0, shuffle/0/639/1

Machine C's disk:
  shuffle/0/640/0, shuffle/0/640/1
  ...
```

The file naming convention is `shuffle/{shuffleId}/{mapPartition}/{reducePartition}`. Each machine has the files for the tasks it ran — and *only* those files.

Now here's the key insight. The `reduceByKey` uses `HashPartitioner(2)`, so there are 2 reduce partitions. Every one of those 16,000 tasks wrote a file for reduce partition 0 and a file for reduce partition 1. The files for reduce partition 0 are **scattered across all 50 machines**.

When Stage 1 starts, the reduce task for partition 0 needs to reach out to *every machine in the cluster* and grab the "bucket 0" file from each one:

```
Reduce task for partition 0:
  Fetch shuffle/0/0/0 from Machine A       ← partition 0's bucket 0
  Fetch shuffle/0/1/0 from Machine A       ← partition 1's bucket 0
  ...
  Fetch shuffle/0/319/0 from Machine A     ← partition 319's bucket 0
  Fetch shuffle/0/320/0 from Machine B     ← partition 320's bucket 0
  ...
  Fetch shuffle/0/15999/0 from Machine AX  ← partition 15999's bucket 0

  That's 16,000 files from 50 machines!
  Merge all of them into one HashMap → the complete reduced data for partition 0.
```

This is why the shuffle is the most expensive operation in Spark. It's not just disk I/O — it's network I/O across the entire cluster. And it's why map-side combining (section 6.5) matters so much: every record you combine locally is one fewer record that needs to cross the network.

---

## 6.5 Why Map-Side Combining Matters

An important clarification: each task writes its **own** set of bucket files. If Machine A runs 100 tasks, there are 100 separate files for bucket 0 on Machine A's disk — `shuffle/0/0/0`, `shuffle/0/1/0`, `shuffle/0/2/0`, ..., `shuffle/0/99/0`. They don't get merged into one file. Each task is independent.

Map-side combining is about what happens **within a single task**, before writing. Consider task 0 processing its partition. Suppose the word "apple" appears 1,000 times in that one partition. Without combining, the task would write 1,000 separate `("apple", 1)` records into its bucket file `shuffle/0/0/1`. The reduce side would later download all 1,000 records and add them up.

With combining, the task uses a HashMap internally. The first time it sees "apple", it stores `("apple", 1)`. The second time, it updates to `("apple", 2)`. After seeing all 1,000 occurrences, the HashMap has `("apple", 1000)`. The task writes **one** record to the file instead of 1,000.

The savings multiply across the cluster. If "apple" appears 1,000 times in each of 16,000 partitions, that's 16 million records without combining vs. 16,000 records with combining — a 1,000x reduction in data that needs to cross the network.

The combining uses the same `Aggregator` from Book 1 (Chapter 13):
- `createCombiner(v)` — first time seeing a key: create the initial combined value
- `mergeValue(existing, v)` — same key again: merge the new value into the existing combiner

---

## 6.6 Connecting to Book 1

Now you can see the complete shuffle picture:

```
WRITE SIDE (this chapter)              READ SIDE (Book 1, Chapter 13)
─────────────────────────              ────────────────────────────────

ShuffleMapTask.run():                  ShuffledRDD.compute():
  1. Read RDD partition                  1. Ask MapOutputTracker for server URIs
  2. Sort into buckets by key            2. Download files via HTTP
  3. Map-side combine                    3. Merge into local HashMap
  4. Write buckets to files              4. Return iterator over HashMap
  5. Return server URI

        ──── HTTP fetch ────→
```

The ShuffleMapTask is the producer. ShuffledRDD.compute() is the consumer. The shuffle files on disk are the handoff point.

---

## 6.7 The Actual Code

For reference, here's the complete `run()` method. After the conceptual walkthrough above, it should read naturally:

```scala
override def run(attemptId: Int): String = {
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]
    val buckets = Array.tabulate(numOutputSplits)(_ => new JHashMap[Any, Any])
    for (elem <- rdd.iterator(split)) {
      val (k, v) = elem.asInstanceOf[(Any, Any)]
      var bucketId = partitioner.getPartition(k)
      val bucket = buckets(bucketId)
      var existing = bucket.get(k)
      if (existing == null) {
        bucket.put(k, aggregator.createCombiner(v))
      } else {
        bucket.put(k, aggregator.mergeValue(existing, v))
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    for (i <- 0 until numOutputSplits) {
      val file = SparkEnv.get.shuffleManager.getOutputFile(dep.shuffleId, partition, i)
      val out = ser.outputStream(new FastBufferedOutputStream(new FileOutputStream(file)))
      val iter = buckets(i).entrySet().iterator()
      while (iter.hasNext()) {
        val entry = iter.next()
        out.writeObject((entry.getKey, entry.getValue))
      }
      out.close()
    }
    return SparkEnv.get.shuffleManager.getServerUri
}
```

---

## 6.8 Summary

| Question | Answer |
|----------|--------|
| What does ShuffleMapTask do? | Reads a partition, sorts data into buckets by key, combines duplicate keys, writes each bucket to a file. |
| What does it return? | The server URI where the shuffle files can be fetched via HTTP. |
| What is map-side combining? | Merging values for the same key locally before writing to disk. Reduces network transfer. |
| How are files organized? | `shuffle/{shuffleId}/{mapPartition}/{reducePartition}` |
| How does the reduce side find the files? | The DAGScheduler records the server URI. The MapOutputTracker serves it to reduce tasks. The ShuffleFetcher downloads via HTTP. |

ShuffleMapTask is the producer. Now let's look at the other task type — ResultTask — which produces the final results that go back to the driver.

---

**Next Chapter**: [Chapter 7: ResultTask — Returning Results to the Driver →](Chapter-07-ResultTask.md)
