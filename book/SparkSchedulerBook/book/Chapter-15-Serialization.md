# Chapter 15: Serialization — Turning Objects into Bytes

## 15.1 Why Serialization Matters

Everything that moves in Spark must become bytes first:

- A task sent from the driver to a worker? Bytes over the network.
- Shuffle data written to disk? Bytes in a file.
- A result sent from a worker back to the driver? Bytes again.

Serialization is the process of turning a Java object into a byte array. Deserialization is the reverse. It sounds mundane, but it's one of the biggest performance factors in Spark. A slow serializer can bottleneck your entire job.

---

## 15.2 The Two-Level Design

Spark's serialization has a simple architecture:

```scala
trait Serializer {
  def newInstance(): SerializerInstance
}

trait SerializerInstance {
  def serialize[T](t: T): Array[Byte]
  def deserialize[T](bytes: Array[Byte]): T
  def outputStream(s: OutputStream): SerializationStream
  def inputStream(s: InputStream): DeserializationStream
}
```

Why two levels? Because some serialization libraries (like Kryo) aren't thread-safe. The `Serializer` is shared across threads, but each thread gets its own `SerializerInstance`. This is the same pattern as `Serializer` → `SerializerInstance` that you see in many Java frameworks.

The `outputStream`/`inputStream` methods are for streaming — writing or reading objects one at a time to/from a file or network connection. This is what ShuffleMapTask uses to write shuffle files.

---

## 15.3 JavaSerializer — Simple but Slow

The default serializer uses Java's built-in `ObjectOutputStream`:

```scala
def serialize[T](t: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteArray
}
```

It works with any class that implements `Serializable`. No setup required. But it's slow and produces large byte arrays because it includes full class metadata (class name, field names, type information) in every serialized object.

---

## 15.4 KryoSerializer — Fast but Requires Registration

Kryo is a third-party library that's much faster than Java serialization. Instead of writing full class names, it assigns each registered class a small integer ID. Instead of writing field names, it uses the field order.

The tradeoff: you need to register your classes upfront so Kryo knows the ID mappings.

Spark pre-registers common types: arrays, tuples, lists, Options, and Scala singletons like `None` and `Nil`. Users can register their own classes via a system property:

```scala
// Set: spark.kryo.registrator=com.example.MyRegistrator
class MyRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[MyCustomClass])
  }
}
```

The performance difference can be dramatic — Kryo is often 5-10x faster and produces byte arrays 2-5x smaller than Java serialization.

---

## 15.5 Two Serializers in SparkEnv

Remember from Chapter 2 — SparkEnv has two serializers:

| | Data Serializer (`serializer`) | Closure Serializer (`closureSerializer`) |
|---|---|---|
| Default | JavaSerializer | JavaSerializer |
| Configurable via | `spark.serializer` | `spark.closure.serializer` |
| Used for | Shuffle files, cached data | Task objects, user functions |
| Can use Kryo? | Yes (recommended) | Usually stays as Java |

Why separate them? Shuffle data is typically simple types (strings, numbers, tuples) that Kryo handles well. But task closures contain complex objects (RDD references, function objects, captured variables) that Java serialization handles more reliably.

In practice, switching the data serializer to Kryo is one of the easiest performance wins in Spark.

---

## 15.6 Where Serialization Happens

| What gets serialized | Which serializer | Where |
|---------------------|-----------------|-------|
| Task sent to worker | closureSerializer | LocalScheduler / MesosScheduler |
| Task result sent back | closureSerializer | LocalScheduler / Executor |
| Shuffle data to disk | serializer | ShuffleMapTask.run() |
| Shuffle data from network | serializer | SimpleShuffleFetcher.fetch() |

---

## 15.7 Summary

| Question | Answer |
|----------|--------|
| What is serialization? | Converting objects to bytes for storage or network transfer. |
| What are the two implementations? | JavaSerializer (simple, slow) and KryoSerializer (fast, needs registration). |
| Why two serializers in SparkEnv? | Data serializer for shuffle/cache (can be Kryo). Closure serializer for tasks (usually Java). |
| Why is Kryo faster? | Compact binary format with integer type IDs instead of full class names. |

---

**Next Chapter**: [Chapter 16: ClosureCleaner — Making Functions Serializable →](Chapter-16-ClosureCleaner.md)
