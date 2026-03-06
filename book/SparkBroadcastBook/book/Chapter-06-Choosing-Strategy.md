# Chapter 6: Choosing a Strategy — The Factory Pattern

## 6.1 Five Implementations, One Interface

Spark 0.5.0 has five broadcast implementations. The user's code always looks the same:

```scala
val bc = sc.broadcast(myData)
// ... inside tasks:
bc.value
```

How does Spark decide which implementation to use? And how does it swap between them without changing any user code?

---

## 6.2 The Broadcast Trait

Every implementation extends the same trait:

```scala
trait Broadcast[T] extends Serializable {
  val uuid = UUID.randomUUID
  def value: T
}
```

Two things: a UUID (the identity) and a `value` method (access the data). That's the entire contract. The user only ever interacts with this trait.

---

## 6.3 The Factory

A `BroadcastFactory` trait defines how to create broadcast variables:

```scala
trait BroadcastFactory {
  def initialize(isMaster: Boolean): Unit
  def newBroadcast[T](value_ : T, isLocal: Boolean): Broadcast[T]
}
```

Each implementation has its own factory: `HttpBroadcastFactory`, `TreeBroadcastFactory`, `BitTorrentBroadcastFactory`, etc.

When SparkContext starts, it reads a system property to decide which factory to use:

```scala
val broadcastFactoryClass = System.getProperty(
    "spark.broadcast.factory", "spark.broadcast.HttpBroadcastFactory")
broadcastFactory = Class.forName(broadcastFactoryClass).newInstance()
```

Default: HttpBroadcast. Want TreeBroadcast? Set `spark.broadcast.factory=spark.broadcast.TreeBroadcastFactory`. Want BitTorrent? Set `spark.broadcast.BitTorrentBroadcastFactory`.

Same plugin architecture as the serializer in SparkEnv (Book 2, Chapter 2). Swap implementations via a config property, no code changes.

---

## 6.4 When to Use What

| Scenario | Best choice | Why |
|----------|------------|-----|
| Small data (< 100 MB), small cluster | HttpBroadcast | Simple, fast enough, no overhead |
| Large data, medium cluster (10–100 machines) | TreeBroadcast | Spreads load without BitTorrent complexity |
| Large data, large cluster (100+ machines) | BitTorrentBroadcast | Best throughput, scales with cluster size |
| Development/testing | HttpBroadcast | Simplest to debug |

In practice, most Spark users never change this setting. HttpBroadcast works fine for typical broadcast sizes (a few MB to a few hundred MB). The tree and BitTorrent implementations shine when broadcasting truly large data (GBs) to large clusters.

---

## 6.5 The DfsBroadcast — The Fallback

There's one more implementation we haven't discussed: `DfsBroadcast`. It writes the data to HDFS (or a local filesystem) and workers read it from there. It's the simplest of all — no HTTP server, no peers, just a shared filesystem.

It's also used as a **fallback**. TreeBroadcast and BitTorrentBroadcast both fall back to reading from HDFS if the peer-to-peer transfer fails. This ensures that even if the fancy distribution protocol breaks, the broadcast variable still reaches every worker.

---

## 6.6 Summary

| Question | Answer |
|----------|--------|
| How does the user choose an implementation? | Set `spark.broadcast.factory` to the desired factory class. Default is HttpBroadcast. |
| Why a factory pattern? | So implementations can be swapped without changing user code or Spark internals. |
| What's the default? | HttpBroadcast — simple and good enough for most cases. |
| What's DfsBroadcast? | Writes to HDFS. Used as a fallback when peer-to-peer transfer fails. |

That completes Part 1 — broadcast variables. Now let's look at the other shared variable: accumulators.

---

**Next Chapter**: [Chapter 7: The Problem — Why Accumulators Exist →](Chapter-07-Accumulator-Problem.md)
