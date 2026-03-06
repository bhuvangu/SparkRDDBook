# Chapter 8: How Accumulators Work — The Thread-Local Trick

## 8.1 The Same `readObject` Hook

Just like broadcast, accumulators use Java's `readObject` to trigger special behavior during deserialization. But where broadcast uses it to *download data*, accumulators use it to *register a thread-local copy*.

Here's the key insight: when an Accumulator is created on the driver, it's the "original." When it gets serialized as part of a task and deserialized on a worker, the deserialized copy is a "local" copy — it starts at zero and tracks only this task's additions.

```scala
class Accumulator[T](initialValue: T, param: AccumulatorParam[T]) {
  val id = Accumulators.newId
  @transient var value_ = initialValue   // current value (transient!)
  val zero = param.zero(initialValue)    // zero value for workers
  var deserialized = false

  Accumulators.register(this, true)      // register as original

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject
    value_ = zero              // start at zero on the worker
    deserialized = true        // mark as a worker copy
    Accumulators.register(this, false)   // register as local copy
  }
}
```

Let's trace what happens step by step.

---

## 8.2 On the Driver — Creating the Original

```scala
val errorCount = sc.accumulator(0)
```

This creates an Accumulator with `id = 1`, `value_ = 0`, and registers it as an "original" in the `Accumulators.originals` map:

```
Accumulators.originals = { 1 → errorCount(value=0) }
```

The driver holds the authoritative copy.

---

## 8.3 In the Task — The Serialization Round-Trip

The accumulator is captured by the closure `line => { errorCount += 1; ... }`. When the task is serialized, `value_` is `@transient` — it's not included. Only the `id`, `zero`, and `deserialized` flag travel with the task.

On the worker, `readObject` fires:
1. Sets `value_` to `zero` (0) — the worker copy starts fresh
2. Sets `deserialized = true` — so the worker can't call `.value` (read is forbidden on workers)
3. Registers this copy in `Accumulators.localAccums` for the current thread

```
Worker, Thread 1:
  Accumulators.localAccums = {
    Thread-1 → { 1 → errorCount(value=0) }
  }
```

Now when the task runs and calls `errorCount += 1`, it increments the **local** copy:

```
Task processes 100 lines, finds 3 malformed:
  errorCount.value_ = 0 → 1 → 2 → 3
```

---

## 8.4 After the Task — Sending Values Back

When a task finishes, the scheduler collects the accumulator values from the current thread:

```scala
val accumUpdates = Accumulators.values
// Returns: { 1 → 3 }   (accumulator id 1 has value 3)
```

These values are sent back to the driver as part of the task completion event (remember `taskEnded(task, Success, result, accumUpdates)` from Book 2, Chapter 9).

On the driver, the DAGScheduler calls:

```scala
Accumulators.add(accumUpdates)
```

This finds the original accumulator with `id = 1` and merges the worker's value:

```
originals[1].value_ = 0 + 3 = 3
```

---

## 8.5 The Full Picture

```
Driver:
  errorCount created (id=1, value=0)
  Registered in originals

Task serialized → shipped to Worker A
  readObject: local copy created (id=1, value=0)
  Task runs: value becomes 3
  Task finishes: accumUpdates = {1 → 3}
  Sent back to driver

Task serialized → shipped to Worker B
  readObject: local copy created (id=1, value=0)
  Task runs: value becomes 2
  Task finishes: accumUpdates = {1 → 2}
  Sent back to driver

Task serialized → shipped to Worker C
  readObject: local copy created (id=1, value=0)
  Task runs: value becomes 1
  Task finishes: accumUpdates = {1 → 1}
  Sent back to driver

Driver receives all three:
  Accumulators.add({1 → 3})  →  originals[1].value = 0 + 3 = 3
  Accumulators.add({1 → 2})  →  originals[1].value = 3 + 2 = 5
  Accumulators.add({1 → 1})  →  originals[1].value = 5 + 1 = 6

println(errorCount.value)  →  6
```

---

## 8.6 Why Thread-Local?

You might wonder: why does `Accumulators.localAccums` use the thread as a key?

Because multiple tasks can run simultaneously on the same machine (in different threads). Each task needs its own accumulator copy. If they shared one copy, their increments would interfere with each other, and the final values sent back would be wrong.

By keying on `Thread.currentThread`, each task's accumulator copies are isolated. When the task finishes, `Accumulators.values` reads only the current thread's copies, and `Accumulators.clear` removes them.

---

## 8.7 The `AccumulatorParam` — Custom Aggregation

The default accumulator adds numbers. But you can define custom aggregation:

```scala
trait AccumulatorParam[T] {
  def addInPlace(t1: T, t2: T): T    // how to merge two values
  def zero(initialValue: T): T        // the identity element
}
```

For example, you could create an accumulator that collects a set of strings, or tracks the min and max of a value, or builds a histogram. As long as the merge operation is associative and commutative (order doesn't matter), it works correctly across distributed tasks.

---

## 8.8 The Safety Guard

Notice this in the Accumulator class:

```scala
def value_= (t: T) {
    if (!deserialized) value_ = t
    else throw new UnsupportedOperationException("Can't use value_= in task")
}
```

On the driver (`deserialized = false`), you can set the value. On a worker (`deserialized = true`), trying to set the value throws an exception. Workers can only `+=` (add), never `=` (set). This prevents a common bug where a task accidentally overwrites the accumulator instead of adding to it.

Reading `.value` on a worker would also be misleading — you'd see only the local copy, not the global total. The API discourages this by design.

---

## 8.9 Summary

| Question | Answer |
|----------|--------|
| How do accumulator values get back to the driver? | Each task's local values are sent as part of the task completion event. The driver merges them into the original. |
| What is the `readObject` trick? | On deserialization, the accumulator registers a thread-local copy starting at zero. |
| Why thread-local? | So multiple tasks running in parallel on the same machine don't interfere with each other. |
| Can workers read the global value? | No. Workers only see their local additions. The driver reads the global total after the job. |
| Can you customize the aggregation? | Yes, via `AccumulatorParam`. Define `addInPlace` and `zero`. |

---

**Next Chapter**: [Chapter 9: Putting It All Together →](Chapter-09-Putting-It-Together.md)
