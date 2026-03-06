# Chapter 1: SparkContext — The Front Door

## 1.1 What You Already Know

By now you have a clear picture of how RDDs work. It's essentially a chain of lazy functions — much like Java 8 streams.

When you write:

```scala
sc.textFile("data.txt")
    .flatMap(_.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
```

You've built a chain of objects in memory. Each RDD holds a reference to its parent and a function:

```
HadoopRDD
  "I know how to read splits from an HDFS file.
   Call my compute(split), I'll open the file at that split's offset
   and return an iterator of lines."
       │
       ▼
FlatMappedRDD
  "I hold a reference to HadoopRDD and the function _.split(" ").
   Call my compute(split), I'll call HadoopRDD.compute(split),
   get its iterator of lines, apply flatMap(_.split(" ")),
   and return an iterator of words."
       │
       ▼
MappedRDD
  "I hold a reference to FlatMappedRDD and the function word => (word, 1).
   Call my compute(split), I'll call FlatMappedRDD.compute(split),
   get its iterator of words, apply map(word => (word, 1)),
   and return an iterator of (word, 1) pairs."
       │
       ▼
ShuffledRDD
  "I'm different. I don't chain to my parent's compute().
   I fetch data from the network — from shuffle files that
   someone else wrote. But who writes those files? And when?"
```

Nothing has executed. No file has been read. No word has been split. It's all just descriptions — recipes waiting to be cooked.

Then you call `collect()`. And *something* happens.

---

## 1.2 The Two Questions

There are actually two burning questions at this point, and they're both about the same gap in our understanding.

### Question 1: Who processes all the splits?

Suppose `data.txt` is a large file on HDFS — say, 1 terabyte. HDFS stores it as blocks (typically 64 MB or 128 MB each). That's roughly 8,000 to 16,000 blocks.

But who decides there are 16,000 splits? Not the scheduler. The **RDD** does, at construction time. When you call `sc.textFile("data.txt")`, HadoopRDD asks Hadoop's InputFormat: "How would you split this file for reading?" Hadoop looks at the file in HDFS, sees 16,000 blocks, and returns 16,000 InputSplit objects — each one describing a chunk of the file (file path, byte offset, length, which machines have copies of this block). HadoopRDD wraps each InputSplit into a Spark Split. That's it — the split count comes from the data, not from the scheduler.

FlatMappedRDD and MappedRDD don't change the split count — they inherit their parent's splits (remember from Book 1: narrow dependencies preserve partitioning). So the entire chain `HadoopRDD → FlatMappedRDD → MappedRDD` has 16,000 splits.

Now here's the gap. We know that `compute(split)` processes one split. But who calls `compute()` on each of the 16,000 splits? Does one machine loop through all of them sequentially? That would take forever. Do they run in parallel? On which machines? Who decides?

The RDD itself doesn't know. HadoopRDD knows *how* to read a split — but it has no idea *who* will call `compute()`, or *where*, or *how many at a time*. The RDD is just a recipe. It doesn't run itself.

Someone needs to look at those 16,000 splits and say: "OK, split 0 lives on Machine A, so let's run `compute(split 0)` on Machine A. Split 1 lives on Machine B, so run it there. And let's do 100 of them at a time, in parallel."

That someone is the **scheduler**.

### Question 2: What about the shuffle?

Even if we solve the "who processes the splits" problem, there's a second issue. Our chain has a `reduceByKey`, which means there's a **ShuffleDependency** in the middle.

Think about why this is a problem. `MappedRDD` partition 0 produces pairs like `("apple", 1), ("banana", 1), ("apple", 1)`. `MappedRDD` partition 1 produces `("banana", 1), ("cherry", 1)`. After `reduceByKey`, all the "apple" pairs need to end up in the same place, and all the "banana" pairs need to end up in the same place — regardless of which partition they came from.

You can't do that by just chaining `compute()` calls. Partition 0 of ShuffledRDD can't get its data by calling `compute()` on one parent partition — it needs pieces from *every* parent partition, reorganized by key.

So someone needs to:

1. Run the `HadoopRDD → FlatMappedRDD → MappedRDD` chain on **every** partition first
2. Sort each partition's output by key and write it to files on disk
3. **Then** let ShuffledRDD fetch the right pieces from those files

Both questions point to the same answer: we need a system that takes the RDD graph, figures out what to run, where to run it, in what order, and how many at a time. That system is the scheduler. And the front door to the scheduler is **SparkContext**.

---

## 1.3 What `collect()` Actually Does

When you call `counts.collect()`, here's what happens at the top level:

```scala
// Inside RDD.collect():
def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
}
```

It calls `sc.runJob()` — a method on SparkContext. It passes two things:
- `this` — the final RDD (ShuffledRDD)
- A function — `iter => iter.toArray` ("turn each partition's iterator into an array")

That's it. `collect()` doesn't know about stages, tasks, shuffles, or parallelism. It just says: "Hey SparkContext, run this function on every partition of this RDD and give me the results."

`count()` does the same thing but with a counting function. `reduce()` does it with a reducing function. `first()` does it on just one partition. **Every action funnels through `sc.runJob()`.**

---

## 1.4 `runJob()` — The Single Gateway

`runJob()` is the single gateway between the RDD world (Book 1) and the scheduler world (this book). It takes four things:

| Parameter | What it is |
|-----------|-----------|
| `rdd` | The final RDD in the chain (ShuffledRDD) |
| `func` | What to do with each partition's iterator |
| `partitions` | Which partitions to process |
| `allowLocal` | Can we skip the scheduler for trivial cases? |

And the method itself is almost disappointingly thin:

```scala
def runJob[T, U](rdd: RDD[T], func: ..., partitions: Seq[Int], allowLocal: Boolean): Array[U] = {
    logInfo("Starting job...")
    val result = scheduler.runJob(rdd, func, partitions, allowLocal)
    logInfo("Job finished in ...")
    result
}
```

Log the start. Hand everything to the scheduler. Log the end. Return the result.

SparkContext doesn't figure out stages. It doesn't create tasks. It doesn't decide which machine processes which split. It just passes the request to the scheduler and waits. The scheduler does all the thinking — and that's what the rest of this book is about.

---

## 1.5 What the Scheduler Will Do (Preview)

To answer our two burning questions, here's a preview of what the scheduler does when it receives the `runJob` call for our word count. We'll spend the rest of the book understanding each step.

**For the 16,000 splits problem:** The scheduler creates one **Task** per split. Each task is a self-contained unit of work: "call `compute(split 4,372)` on the RDD chain, apply the function, return the result." These tasks are shipped to worker machines and run in parallel — hundreds at a time. The scheduler tracks which tasks have finished, which are still running, and which need to be retried if a machine crashes.

**For the shuffle problem:** The scheduler looks at the RDD graph and finds the ShuffleDependency. It draws a line there: "Everything before the shuffle is Stage 0. Everything after is Stage 1." Stage 0 must finish completely before Stage 1 can start — because Stage 1 needs the reorganized data that Stage 0 produces.

```
Stage 0 (16,000 tasks):                    Stage 1 (N tasks):
  For each split:                            For each output partition:
    HadoopRDD.compute(split)                   Fetch shuffle data from all
    → FlatMappedRDD.compute(split)             Stage 0 machines
    → MappedRDD.compute(split)                 → Merge by key
    → Sort by key, write to files              → Apply collect function
                                               → Return results to driver
         ─── must finish first ───→
```

Stage 0 runs 16,000 tasks in parallel (as many as the cluster allows). Each task processes one split, sorts the output by key, and writes files. When all 16,000 tasks finish, Stage 1 starts. Stage 1's tasks fetch the sorted data, merge it, and send results back.

That's the big picture. The rest of this book fills in every detail.

---

## 1.6 But First — What Did SparkContext Set Up?

Before any of this can work, SparkContext's constructor set up the infrastructure. When you wrote `new SparkContext("local[4]", "WordCount")`, three things happened:

**1. It created the toolbox (SparkEnv).** All the runtime services — serializers to turn tasks into bytes for shipping to workers, a shuffle file server so machines can exchange data, trackers that know where cached data and shuffle files live. We'll explore this in Chapter 2.

**2. It created the scheduler.** Based on the `master` string:
- `"local"` or `"local[4]"` → a LocalScheduler (thread pool on your machine)
- A Mesos URL → a MesosScheduler (tasks run on a cluster)

Both implement the same interface. Your code doesn't know or care which one is running. The word count works identically on your laptop or on 1,000 machines.

**3. It set up ID counters.** Every RDD gets a unique ID (0, 1, 2, ...). Every shuffle gets a unique ID. These are used throughout the system to track what's what.

---

## 1.7 The `allowLocal` Shortcut

One small but clever detail. When you call `first()`, it passes `allowLocal = true`. This tells the scheduler: "If this is trivial — no shuffles, just one partition — don't bother creating tasks. Just call `rdd.iterator(split)` right here on the driver and give me the answer."

This is why `sc.parallelize(List(1,2,3)).first()` is nearly instant. It short-circuits the entire scheduling machinery. No tasks, no parallelism — just a direct `compute()` call.

But `collect()` passes `allowLocal = false` — it processes all partitions, so it needs the full scheduler.

---

## 1.8 Summary

| Question | Answer |
|----------|--------|
| Who calls `compute()` on each of the 16,000 splits? | The scheduler creates one Task per split and runs them in parallel across machines. |
| How does the shuffle get handled? | The scheduler splits the RDD graph into stages at shuffle boundaries. Stage 0 writes sorted files. Stage 1 fetches and merges them. |
| What does `collect()` do? | Calls `sc.runJob(this, iter => iter.toArray)` — passes the RDD and a function to SparkContext. |
| What does `sc.runJob()` do? | Passes everything to the scheduler. It's a thin wrapper. |
| What did SparkContext's constructor do? | Created the toolbox (SparkEnv), the scheduler, and ID counters. |

The scheduler is where the action is. But before we open it up, let's quickly look at the toolbox it uses — SparkEnv.

---

**Next Chapter**: [Chapter 2: SparkEnv — The Toolbox →](Chapter-02-SparkEnv.md)
