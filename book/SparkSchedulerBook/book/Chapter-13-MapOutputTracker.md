# Chapter 13: MapOutputTracker — Where Did the Shuffle Data Go?

## 13.1 The Problem

After Stage 0 completes, shuffle files are scattered across multiple machines:

```
Machine A has: shuffle/0/0/0, shuffle/0/0/1   (map partition 0's output)
Machine B has: shuffle/0/1/0, shuffle/0/1/1   (map partition 1's output)
Machine C has: shuffle/0/2/0, shuffle/0/2/1   (map partition 2's output)
```

When Stage 1 starts, its tasks need to fetch this data. But how does a task running on Machine D know that the files are on Machines A, B, and C?

It needs a directory service — something that maps "shuffle #0" to a list of server URIs. That's the MapOutputTracker.

---

## 13.2 The Concept: A Phone Book for Shuffle Data

Think of MapOutputTracker as a phone book. The DAGScheduler writes entries when shuffle map stages complete. Workers look up entries when they need to fetch shuffle data.

```
Phone book (on the driver):
  Shuffle 0 → ["http://A:45678", "http://B:45679", "http://C:45680"]
  Shuffle 1 → ["http://A:45678", "http://D:45681"]
```

The driver maintains the authoritative copy. Workers cache entries locally so they don't call the driver for every fetch.

---

## 13.3 The Lifecycle

**When a stage is created:** The DAGScheduler calls `registerShuffle(shuffleId, numMaps)`. This creates an empty entry — an array of `null` values, one per map partition.

```
registerShuffle(0, 3)  →  serverUris[0] = [null, null, null]
```

**When a shuffle map stage completes:** The DAGScheduler calls `registerMapOutputs(shuffleId, uris)`. This fills in the array with the server URIs from each completed ShuffleMapTask.

```
registerMapOutputs(0, ["http://A:45678", "http://B:45679", "http://C:45680"])
```

**When a worker needs shuffle data:** It calls `getServerUris(shuffleId)`. If the worker has the URIs cached locally, it returns them immediately. Otherwise, it asks the driver via a remote actor message.

```
Worker on Machine D:
  getServerUris(0)
  → Not cached locally → ask the driver
  → Driver returns ["http://A:45678", "http://B:45679", "http://C:45680"]
  → Cache locally for next time
```

---

## 13.4 The Generation Number — Handling Machine Failures

Here's a subtle problem. A worker caches shuffle locations locally. Then Machine B crashes. The worker's cache still says "map partition 1 is on Machine B." If it tries to fetch from there, it'll fail.

The solution is a **generation number**. Every time a map output is lost, the generation increments:

```
Generation 0: Everything is fine.
  serverUris[0] = ["http://A:45678", "http://B:45679", "http://C:45680"]

Machine B crashes!
  unregisterMapOutput(shuffleId=0, mapId=1, "http://B:45679")
  serverUris[0] = ["http://A:45678", null, "http://C:45680"]
  Generation incremented to 1.
```

Every task carries the generation number from when it was created (remember `task.generation` from Chapter 5). When a worker runs a task, it checks: "Is my generation older than this task's generation?" If so, it clears its local cache and re-fetches from the driver.

```
Worker's local generation: 0
Task's generation: 1        ← newer!
→ Clear local cache
→ Re-fetch from driver (which now has the updated locations)
→ Update local generation to 1
```

This ensures that after a failure, workers don't use stale locations pointing to a dead machine.

---

## 13.5 The Complete Flow

```
1. DAGScheduler creates Stage 0
   → mapOutputTracker.registerShuffle(0, 3)

2. ShuffleMapTasks complete:
   partition 0 on host-A → returns "http://A:45678"
   partition 1 on host-B → returns "http://B:45679"
   partition 2 on host-C → returns "http://C:45680"

3. Stage 0 finished!
   → mapOutputTracker.registerMapOutputs(0,
       ["http://A:45678", "http://B:45679", "http://C:45680"])

4. Stage 1 starts. ResultTask on host-D needs shuffle data.
   → ShuffledRDD.compute() calls shuffleFetcher.fetch(shuffleId=0, reduceId=0)
   → ShuffleFetcher calls mapOutputTracker.getServerUris(0)
   → Worker on host-D doesn't have them cached → asks driver
   → Driver returns the three URIs
   → ShuffleFetcher downloads from all three

5. Host-B crashes!
   → mapOutputTracker.unregisterMapOutput(0, 1, "http://B:45679")
   → Generation incremented to 1
   → Stage 0 resubmitted for partition 1 only
   → New ShuffleMapTask runs on host-E → returns "http://E:45681"
   → mapOutputTracker updated:
     ["http://A:45678", "http://E:45681", "http://C:45680"]
```

---

## 13.6 Summary

| Question | Answer |
|----------|--------|
| What is MapOutputTracker? | A directory service that maps shuffle IDs to server URIs. |
| Where does it run? | Master on the driver, clients on workers. Connected via remote actors. |
| When are locations registered? | When a shuffle map stage completes. |
| How do workers find locations? | Call `getServerUris()` — checks local cache first, then asks the driver. |
| What is the generation number? | A version counter. Increments when a map output is lost. Workers clear their caches when they see a newer generation. |

---

**Next Chapter**: [Chapter 14: Shuffle I/O — Moving Data Across the Network →](Chapter-14-Shuffle-IO.md)
