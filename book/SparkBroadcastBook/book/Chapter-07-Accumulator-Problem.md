# Chapter 7: The Problem — Why Accumulators Exist

## 7.1 The Opposite Direction

Broadcast sends data from the driver to workers. But sometimes you need data to flow the other way — from workers back to the driver.

Imagine you're processing a large dataset and you want to count how many records are malformed:

```scala
var errorCount = 0    // on the driver

sc.textFile("data.txt")
    .map(line => {
        if (isMalformed(line)) {
            errorCount += 1    // ← this doesn't work!
        }
        parseLine(line)
    })
    .saveAsTextFile("output")
```

This looks reasonable but it's completely broken. Here's why.

From Book 2, you know that the `map` function gets serialized and shipped to worker machines. When a worker deserializes the task, it gets its own *copy* of `errorCount`. When it increments `errorCount`, it's incrementing a local copy on the worker. The driver's `errorCount` stays at 0.

```
Driver:  errorCount = 0     ← never changes

Worker A: errorCount = 0 → 1 → 2 → 3     ← local copy, thrown away after task
Worker B: errorCount = 0 → 1 → 2          ← local copy, thrown away after task
Worker C: errorCount = 0 → 1              ← local copy, thrown away after task
```

Each worker increments its own copy. Those copies are discarded when the task finishes. The driver never sees any of them.

---

## 7.2 What We Need

We need a variable that:

1. **Workers can add to** — each task can increment it
2. **The driver can read** — after the job finishes, the driver sees the total
3. **Workers can't read the global value** — a task only sees its own local additions (otherwise you'd need distributed consensus, which is expensive)

This is a **write-only** shared variable from the workers' perspective. Workers write (add). The driver reads. That's an accumulator.

```scala
val errorCount = sc.accumulator(0)    // create with initial value 0

sc.textFile("data.txt")
    .map(line => {
        if (isMalformed(line)) {
            errorCount += 1    // ← this works!
        }
        parseLine(line)
    })
    .saveAsTextFile("output")

println("Errors: " + errorCount.value)   // driver reads the total
```

After the job, `errorCount.value` on the driver contains the sum of all increments from all tasks across all machines.

---

## 7.3 The Burning Question

How does this work? Each task runs on a different machine, in a different JVM. How do the increments from 16,000 tasks across 50 machines get aggregated back into a single number on the driver?

The answer involves the same serialization trick we saw with broadcast — but in reverse. Let's see how in the next chapter.

---

## 7.4 Summary

| Question | Answer |
|----------|--------|
| What problem do accumulators solve? | Aggregating values from workers back to the driver (e.g., error counts, metrics). |
| Why doesn't a normal variable work? | Each task gets its own copy. Increments are local and discarded. |
| What can workers do with an accumulator? | Add to it (`+=`). They cannot read the global value. |
| What can the driver do? | Read the final aggregated value after the job completes. |

---

**Next Chapter**: [Chapter 8: How Accumulators Work — The Thread-Local Trick →](Chapter-08-How-Accumulators-Work.md)
