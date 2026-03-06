# Chapter 17: Putting It All Together — A Complete Trace

In Book 1 (Chapter 19), we traced a word count through the RDD layer — how RDD objects are created, how the DAG is built, and how data flows through partitions. Now let's trace the same program through the scheduler layer — every phase from `collect()` to the final result.

We'll use `master = "local[2]"` (LocalScheduler with 2 threads) so we can trace everything without network complexity.

---

## 17.1 The Program

```scala
val sc = new SparkContext("local[2]", "WordCount")
val counts = sc.textFile("data.txt")       // HadoopRDD(id=0) → MappedRDD(id=1)
    .flatMap(_.split(" "))                  // FlatMappedRDD(id=2)
    .map(word => (word, 1))                // MappedRDD(id=3)
    .reduceByKey(_ + _)                    // ShuffledRDD(id=4)
counts.collect()
```

The file has 2 HDFS blocks, so HadoopRDD has 2 partitions. `reduceByKey` uses `HashPartitioner(2)`.

---

## 17.2 Phase 1: Setting Up the Workshop

`new SparkContext("local[2]", "WordCount")` creates:

- SparkEnv with all 7 tools (cache, serializers, trackers, shuffle I/O)
- LocalScheduler with a 2-thread pool
- ID counters starting at 0

The workshop is open. No computation yet.

---

## 17.3 Phase 2: Building the Recipe (No Computation)

Each transformation creates an RDD object in memory:

```
sc.textFile("data.txt")    → HadoopRDD(id=0, 2 splits) → MappedRDD(id=1)
.flatMap(_.split(" "))     → FlatMappedRDD(id=2)
.map(word => (word, 1))    → MappedRDD(id=3)
.reduceByKey(_ + _)        → ShuffledRDD(id=4, ShuffleDependency(shuffleId=0))
```

Five RDD objects. Zero computation. Just a graph of descriptions.

---

## 17.4 Phase 3: `collect()` Knocks on the Door

```
counts.collect()
  → sc.runJob(ShuffledRDD, iter => iter.toArray, [0, 1], allowLocal=false)
    → DAGScheduler.runJob() begins
```

The action translates to: "Run `iter.toArray` on partitions 0 and 1 of ShuffledRDD."

---

## 17.5 Phase 4: Building Stages

The DAGScheduler walks the RDD graph backwards from ShuffledRDD:

```
ShuffledRDD → ShuffleDependency → STOP! Stage boundary.
  Create Stage 0 for MappedRDD(id=3)
    Walk backwards from MappedRDD(3):
      MappedRDD(3) → narrow → FlatMappedRDD → narrow → MappedRDD(1) → narrow → HadoopRDD
      No more shuffles → Stage 0 has no parents

Create Stage 1 for ShuffledRDD (the result stage)
  parents = [Stage 0]
```

Result:
```
Stage 0: rdd=MappedRDD(3), shuffleDep=Some(0), parents=[], numPartitions=2
Stage 1: rdd=ShuffledRDD,  shuffleDep=None,     parents=[Stage 0], numPartitions=2
```

---

## 17.6 Phase 5: Submitting Stages

```
submitStage(Stage 1):
  Missing parents = [Stage 0]
  → submitStage(Stage 0):
      Missing parents = []
      → Create 2 ShuffleMapTasks:
          ShuffleMapTask(stageId=0, partition=0)
          ShuffleMapTask(stageId=0, partition=1)
      → submitTasks to LocalScheduler
      → running = {Stage 0}
  → waiting = {Stage 1}
```

---

## 17.7 Phase 6: ShuffleMapTasks Execute

The LocalScheduler submits both tasks to the 2-thread pool. They run in parallel:

```
Thread 1 — ShuffleMapTask(partition=0):          Thread 2 — ShuffleMapTask(partition=1):

  serialize → deserialize task                     serialize → deserialize task

  rdd.iterator(split 0):                           rdd.iterator(split 1):
    HadoopRDD reads HDFS block 0                     HadoopRDD reads HDFS block 1
    → MappedRDD(1): extract text                     → MappedRDD(1): extract text
    → FlatMappedRDD: split into words                → FlatMappedRDD: split into words
    → MappedRDD(3): word → (word, 1)                → MappedRDD(3): word → (word, 1)

  Produces:                                        Produces:
    ("hello",1),("world",1),("hello",1)              ("spark",1),("hello",1),("world",1)

  Bucket by HashPartitioner(2):                    Bucket by HashPartitioner(2):
    bucket[0]: {"world": 1}                          bucket[0]: {"world": 1}
    bucket[1]: {"hello": 2}  ← combined!             bucket[1]: {"spark": 1, "hello": 1}

  Write files:                                     Write files:
    shuffle/0/0/0: ("world",1)                       shuffle/0/1/0: ("world",1)
    shuffle/0/0/1: ("hello",2)                       shuffle/0/1/1: ("spark",1),("hello",1)

  Return "http://localhost:45678"                  Return "http://localhost:45678"

  taskEnded(Success)                               taskEnded(Success)
```

---

## 17.8 Phase 7: Stage 0 Completes, Stage 1 Starts

The event loop processes the two completion events:

```
Event: ShuffleMapTask(partition=0) succeeded
  → stage.addOutputLoc(0, "http://localhost:45678")
  → pendingTasks still has partition 1 → not done yet

Event: ShuffleMapTask(partition=1) succeeded
  → stage.addOutputLoc(1, "http://localhost:45678")
  → pendingTasks is EMPTY → Stage 0 is done!

  → Register shuffle outputs with MapOutputTracker:
    shuffleId=0 → ["http://localhost:45678", "http://localhost:45678"]

  → Check waiting stages: Stage 1 → missing parents = [] → runnable!
  → Create 2 ResultTasks:
      ResultTask(stageId=1, partition=0, outputId=0)
      ResultTask(stageId=1, partition=1, outputId=1)
  → Submit to LocalScheduler
  → running = {Stage 1}, waiting = {}
```

---

## 17.9 Phase 8: ResultTasks Execute

```
Thread 1 — ResultTask(partition=0):               Thread 2 — ResultTask(partition=1):

  ShuffledRDD.compute(split 0):                     ShuffledRDD.compute(split 1):
    combiners = {}                                    combiners = {}

    shuffleFetcher.fetch(shuffleId=0, reduceId=0):   shuffleFetcher.fetch(shuffleId=0, reduceId=1):
      mapOutputTracker.getServerUris(0)                 mapOutputTracker.getServerUris(0)
      → ["http://localhost:45678",                      → ["http://localhost:45678",
         "http://localhost:45678"]                          "http://localhost:45678"]

      GET shuffle/0/0/0 → ("world",1)                  GET shuffle/0/0/1 → ("hello",2)
        merge: {"world": 1}                               merge: {"hello": 2}
      GET shuffle/0/1/0 → ("world",1)                  GET shuffle/0/1/1 → ("spark",1),("hello",1)
        merge: {"world": 2}                               merge: {"hello": 3, "spark": 1}

    Return iterator over {"world": 2}                 Return iterator over {"hello": 3, "spark": 1}

  func(context, iterator):                          func(context, iterator):
    iter.toArray                                      iter.toArray
    → [("world", 2)]                                  → [("hello", 3), ("spark", 1)]

  taskEnded(Success)                                taskEnded(Success)
```

---

## 17.10 Phase 9: Results Returned

```
Event: ResultTask(outputId=0) succeeded
  → results[0] = [("world", 2)]
  → numFinished = 1

Event: ResultTask(outputId=1) succeeded
  → results[1] = [("hello", 3), ("spark", 1)]
  → numFinished = 2

numFinished == numOutputParts (2) → EXIT LOOP!

Return results = [[("world",2)], [("hello",3),("spark",1)]]

Back in collect():
  Array.concat(results) = [("world",2), ("hello",3), ("spark",1)]
```

Done. The user gets their word counts.

---

## 17.11 The Complete Call Chain

```
rdd.collect()
  → sc.runJob()
    → DAGScheduler.runJob()
      → Build stages (walk RDD graph backwards)
      → submitStage(finalStage)
        → submitStage(parentStage) [recursive, parents first]
          → submitMissingTasks() → ShuffleMapTasks
            → LocalScheduler → thread pool
              → serialize → deserialize → task.run()
                → rdd.iterator() [Book 1 compute pipeline]
                → bucket + combine + write shuffle files
              → taskEnded(Success)
      → Event loop: stage complete → register outputs → wake up waiting stages
        → submitMissingTasks() → ResultTasks
          → task.run()
            → ShuffledRDD.compute() → fetch shuffle data → merge
            → func(iter) = iter.toArray
          → taskEnded(Success)
      → Event loop: all results collected → EXIT
    → return results
  → Array.concat(results)
→ final array returned to user
```

---

## 17.12 What If Something Fails?

Suppose host-B crashes after writing its shuffle files but before the reduce tasks fetch them. Here's what happens:

```
ResultTask(partition=0) tries to fetch from host-B → FetchFailed!

Event loop:
  → Mark Stage 1 as failed
  → Mark Stage 0 as failed (its output on host-B is lost)
  → Remove host-B's output location for partition 1
  → Wait 2 seconds (let other failures arrive)

After 2 seconds:
  → Resubmit Stage 0: only partition 1 needs recomputing
    (partition 0's output on host-A is still valid)
  → ShuffleMapTask(partition=1) runs on a different machine
  → Stage 0 completes again
  → Stage 1 resubmitted → ResultTasks fetch successfully
  → Results returned
```

The lineage from Book 1 makes this possible. Spark doesn't need backup copies of the data — it has the recipe to recompute any lost partition.

---

**Next Chapter**: [Chapter 18: What Comes Next →](Chapter-18-What-Comes-Next.md)
