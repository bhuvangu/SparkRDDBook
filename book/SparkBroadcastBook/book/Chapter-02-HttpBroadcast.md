# Chapter 2: HttpBroadcast — The Simple Way

## 2.1 The Simplest Possible Solution

If you had to distribute a 2 GB file from one machine to 50 machines, what's the simplest thing you could do?

Put the file on an HTTP server. Tell the 50 machines the URL. Let them download it.

That's exactly what HttpBroadcast does. The driver writes the broadcast data to a file, starts an HTTP server, and when workers need the data, they download it via HTTP. No fancy protocols, no peer-to-peer — just a file server and HTTP clients.

---

## 2.2 The Driver Side — Writing the Data

When you call `sc.broadcast(lookupTable)`, the driver:

1. **Serializes** the object to a file: `broadcast-{uuid}` in a temp directory
2. **Serves** that file via an HTTP server (the same Jetty-based server used elsewhere in Spark)

```
Driver machine:
  /tmp/spark-broadcast/broadcast-550e8400-e29b-41d4-a716-446655440000
  ↑
  HTTP server at http://driver:45678/broadcast-550e8400-...
```

The file is written once. The HTTP server is started once (during `Broadcast.initialize()`). Every broadcast variable becomes a new file in the same directory, served by the same server.

---

## 2.3 The Worker Side — Downloading the Data

Here's where it gets interesting. The worker doesn't download the data when the task arrives. It downloads it **during deserialization** — when the task is being unpacked from bytes.

Remember from Book 2: a task gets serialized on the driver, sent to a worker, and deserialized there. The Broadcast object is part of the task's closure. When Java deserializes it, it calls a special method: `readObject`.

HttpBroadcast's `readObject` does this:

```
1. Check: "Do I already have this UUID's data cached on this machine?"
   → Yes: use the cached copy. Done.
   → No: continue to step 2.

2. Download the data from the driver's HTTP server:
   GET http://driver:45678/broadcast-550e8400-...

3. Deserialize the downloaded bytes back into the original object.

4. Cache it locally (so the next task on this machine finds it in step 1).

5. Set value_ to the downloaded object.
```

The actual code is short:

```scala
private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    HttpBroadcast.synchronized {
      val cachedVal = HttpBroadcast.values.get(uuid, 0)
      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        logInfo("Started reading broadcast variable " + uuid)
        value_ = HttpBroadcast.read[T](uuid)
        HttpBroadcast.values.put(uuid, 0, value_)
      }
    }
}
```

---

## 2.4 The Cache — One Copy Per Machine

The cache check in step 1 is crucial. Without it, every task on Machine A would download the 2 GB table separately. With it, only the **first** task downloads. Every subsequent task finds the data already cached.

The cache is stored in `HttpBroadcast.values` — which is backed by SparkEnv's cache (the same cache used for RDD partitions). The key is the broadcast variable's UUID. The value is the deserialized object.

```
Machine A:
  Task 0 arrives, deserializes Broadcast(uuid=550e8400)
    → Cache miss → download from driver → cache it
  Task 1 arrives, deserializes Broadcast(uuid=550e8400)
    → Cache hit → use cached copy (no download!)
  Task 2 arrives, deserializes Broadcast(uuid=550e8400)
    → Cache hit → use cached copy
  ...
  Task 319 arrives
    → Cache hit → use cached copy

Total downloads from driver: 1 (not 320)
```

---

## 2.5 What Gets Serialized in the Task?

This is a subtle but important point. When the Broadcast object is serialized as part of a task, what actually gets written to bytes?

Look at the class:

```scala
class HttpBroadcast[T](@transient var value_ : T, isLocal: Boolean)
extends Broadcast[T] with Logging with Serializable {
```

The `value_` field is marked `@transient`. Remember from Book 1 (Chapter 2): `@transient` means "don't serialize this field." So when the task is serialized, the 2 GB table is **not** included. Only the UUID (from the parent `Broadcast` trait) is serialized.

The task payload contains a tiny Broadcast object with just a UUID. The actual data travels separately — via the HTTP download during deserialization.

This is the whole trick: **the data and the reference travel through different channels.** The reference (UUID) travels with the task. The data travels via HTTP, once per machine.

---

## 2.6 The Bottleneck

HttpBroadcast is simple and correct. But it has an obvious problem: the driver is the only source.

```
                    Driver
                   (HTTP server)
                  ╱  │  │  │  ╲
                ╱    │  │  │    ╲
              ╱      │  │  │      ╲
            ╱        │  │  │        ╲
          W1        W2  W3  W4      W50
```

If you have 50 workers and a 2 GB broadcast, the driver sends 50 × 2 GB = 100 GB. All from one machine's network interface. If the driver has a 10 Gbps link, that's 80 seconds just for the data transfer — and during that time, the driver is saturated.

With 500 workers, it's 1 TB from one machine. That doesn't scale.

This is why Spark 0.5.0 has other broadcast implementations. But before we look at those, let's understand the mechanism that makes *all* of them work — the `readObject` serialization trick.

---

## 2.7 Summary

| Question | Answer |
|----------|--------|
| How does HttpBroadcast work? | Driver writes data to a file and serves it via HTTP. Workers download during task deserialization. |
| What gets serialized in the task? | Just the UUID (`value_` is `@transient`). The actual data travels via HTTP. |
| How is data shared between tasks on the same machine? | A cache keyed by UUID. First task downloads, subsequent tasks find it cached. |
| What's the problem? | The driver is the only source. With many workers, it becomes a bottleneck. |

---

**Next Chapter**: [Chapter 3: The Serialization Trick — How readObject Powers Everything →](Chapter-03-Serialization-Trick.md)
