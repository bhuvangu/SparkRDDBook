# Chapter 14: Shuffle I/O — Moving Data Across the Network

## 14.1 The Three Pieces of the Shuffle

We've now seen the shuffle from multiple angles. Let's put the complete picture together. The shuffle has three pieces:

**The writer** (ShuffleMapTask, Chapter 6) — reads a partition, sorts by key, writes files to local disk.

**The directory** (MapOutputTracker, Chapter 13) — tracks where the files are. Workers ask it for server URIs.

**The reader** (ShuffleFetcher, this chapter) — downloads files from remote machines via HTTP.

But there's a missing piece: who *serves* the files? ShuffleMapTask writes them to local disk, but a file on Machine A's disk isn't accessible to Machine D. Something needs to make those files available over the network.

That's the **ShuffleManager**.

---

## 14.2 ShuffleManager — The File Server

Every machine that runs Spark tasks has a ShuffleManager. It does two things:

1. **Creates a local directory** for shuffle files: `/tmp/spark-local-{uuid}/shuffle/`
2. **Starts an HTTP server** that serves files from this directory

When a ShuffleMapTask writes files to `shuffle/0/0/0`, those files are immediately accessible via HTTP:

```
Local file:  /tmp/spark-local-abc123/shuffle/0/0/0
HTTP URL:    http://192.168.1.5:45678/shuffle/0/0/0
```

The ShuffleMapTask returns the server URI (`http://192.168.1.5:45678`) as its result. The DAGScheduler records it. The MapOutputTracker serves it to workers. And the ShuffleFetcher uses it to download the data.

---

## 14.3 SimpleShuffleFetcher — The Downloader

When a reduce task needs shuffle data, it calls `ShuffleFetcher.fetch()`. The `SimpleShuffleFetcher` does this sequentially — one server at a time:

```
Reduce partition 0 needs data from all 3 map partitions:

1. Ask MapOutputTracker: "Where are shuffle 0's files?"
   → ["http://A:45678", "http://B:45679", "http://C:45680"]

2. Download from each map partition:
   GET http://A:45678/shuffle/0/0/0  → ("banana",1), ("cherry",1)
   GET http://B:45679/shuffle/0/1/0  → ("banana",1), ("dog",1)
   GET http://C:45680/shuffle/0/2/0  → ("cherry",1)

3. For each (key, value) pair received, pass it to the merge function
   (which builds the HashMap in ShuffledRDD.compute())
```

The URL pattern is: `{serverUri}/shuffle/{shuffleId}/{mapPartition}/{reducePartition}`

Spark 0.5.0 also has a `ParallelShuffleFetcher` that downloads from multiple servers concurrently — faster, but the same concept.

---

## 14.4 When a Download Fails

If an HTTP download fails (machine crashed, network error), the fetcher throws a `FetchFailedException`. This exception carries exactly the information the DAGScheduler needs to recover:

- Which shuffle? (`shuffleId`)
- Which map partition's output was lost? (`mapId`)
- Which server was it on? (`serverUri`)

The exception propagates up through the task, gets caught by the Executor (or LocalScheduler), and is reported to the DAGScheduler as a `FetchFailed` event. The DAGScheduler then resubmits the map stage for the lost partition, as we saw in Chapter 9.

---

## 14.5 The Complete Data Path

```
WRITE                          DIRECTORY                      READ
─────                          ─────────                      ────

ShuffleMapTask                 MapOutputTracker                ShuffleFetcher
  │                              │                              │
  │ rdd.iterator(split)          │                              │
  │ → produces (key, value)      │                              │
  │                              │                              │
  │ bucket by key                │                              │
  │ combine duplicates           │                              │
  │ write to local files         │                              │
  │                              │                              │
  │ return serverUri ──────────→ │ registerMapOutputs()         │
  │                              │ shuffleId → [uri, uri, uri]  │
  │                              │                              │
  │                              │ ←──────────────────────────  │ getServerUris()
  │                              │ return [uri, uri, uri]       │
  │                              │                              │
  │                              │                              │ HTTP GET each uri
  │  ←─────────────────────────────────────────────────────────  │ /shuffle/sid/mid/rid
  │  serve file via HTTP         │                              │
  │                              │                              │ deserialize pairs
  │                              │                              │ merge into HashMap
```

---

## 14.6 Summary

| Component | Role |
|-----------|------|
| ShuffleManager | Creates local shuffle directories, starts HTTP server, provides file paths and server URI |
| MapOutputTracker | Directory service: maps shuffleId → array of server URIs |
| SimpleShuffleFetcher | Downloads shuffle files via HTTP, deserializes (key, value) pairs |
| FetchFailedException | Carries failure info back to DAGScheduler for recovery |

| Question | Answer |
|----------|--------|
| How are shuffle files served? | Via an HTTP server started by ShuffleManager on each machine. |
| How does a reducer find the files? | Asks MapOutputTracker for server URIs, constructs URLs. |
| What happens if a download fails? | FetchFailedException → reported to DAGScheduler → map stage resubmitted. |

---

**Next Chapter**: [Chapter 15: Serialization — Turning Objects into Bytes →](Chapter-15-Serialization.md)
