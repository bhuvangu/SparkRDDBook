# Chapter 7: ResultTask — Returning Results to the Driver

## 7.1 The Simplest Task

ShuffleMapTask has hardcoded logic: bucket by key, combine, write files. It's a complex machine with a specific purpose.

ResultTask is the opposite. It's almost embarrassingly simple. Its entire job:

1. Get the data for this partition (call `rdd.iterator(split)`)
2. Apply the user's function to it
3. Return the result

That's it. Two lines of real logic:

```scala
override def run(attemptId: Int): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    func(context, rdd.iterator(split))
}
```

Create a TaskContext (the identity card). Call the user's function on the partition's data. Return whatever the function returns.

---

## 7.2 The Function Comes from the Action

What makes ResultTask flexible is that the function (`func`) comes from the action the user called. Different actions provide different functions:

**`collect()`** provides: "Turn the iterator into an array"
```scala
func = (context, iter) => iter.toArray
// Result: Array[("apple", 3), ("banana", 1)]
```

**`count()`** provides: "Count the elements"
```scala
func = (context, iter) => {
    var result = 0L
    while (iter.hasNext) { result += 1L; iter.next }
    result
}
// Result: 42L
```

**`reduce(_ + _)`** provides: "Reduce the elements with the given function"
```scala
func = (context, iter) => iter.reduceLeft(f)
// Result: the partial reduction of this partition
```

The ResultTask doesn't know or care what the function does. It just runs it.

---

## 7.3 The `outputId` — Where Does My Result Go?

ResultTask has one field that ShuffleMapTask doesn't: `outputId`. This is the index in the results array where this task's result should be stored.

For `collect()` on an RDD with 3 partitions, it's straightforward:

```
ResultTask(partition=0, outputId=0) → results[0]
ResultTask(partition=1, outputId=1) → results[1]
ResultTask(partition=2, outputId=2) → results[2]
```

But for `take()`, which might process only some partitions, the mapping can differ:

```
ResultTask(partition=0, outputId=0) → results[0]
ResultTask(partition=2, outputId=1) → results[1]   ← partition 2, but slot 1
```

The `outputId` ensures results end up in the right position regardless of which partitions are processed.

---

## 7.4 ShuffleMapTask vs ResultTask — The Complete Picture

| | ShuffleMapTask | ResultTask |
|---|---|---|
| Stage type | Intermediate (shuffle map) | Final (result) |
| Logic | Hardcoded: bucket + combine + write | Flexible: runs user's function |
| Returns | Server URI (small string) | User's result (could be a large array) |
| Where result goes | DAGScheduler records it as a shuffle output location | DAGScheduler stores it in the results array |
| Complexity | ~40 lines of bucketing and file I/O | ~2 lines |

Together, they cover every task Spark ever runs. ShuffleMapTasks produce intermediate data. ResultTasks produce final answers. A job with no shuffles has only ResultTasks. A job with one shuffle has ShuffleMapTasks in Stage 0 and ResultTasks in Stage 1.

---

## 7.5 Summary

| Question | Answer |
|----------|--------|
| What does ResultTask do? | Runs the user's function on a partition and returns the result. |
| How complex is it? | Two lines: create TaskContext, call `func(context, rdd.iterator(split))`. |
| What is `func`? | The user's function — `iter.toArray` for collect, a counter for count, etc. |
| What is `outputId`? | The index in the results array where this task's result goes. |

We now understand both task types. The next question is: **who creates these tasks and in what order?** That's the DAGScheduler — the brain of the whole operation.

---

**Next Chapter**: [Chapter 8: DAGScheduler — Building Stages →](Chapter-08-DAGScheduler-Stages.md)
