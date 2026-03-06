# 📘 Understanding Spark's Shared Variables — Broadcast and Accumulators

A guide to how Spark distributes read-only data to every machine and collects write-only metrics back, explained for Java developers.

Based on the actual source code of **Apache Spark 0.5.0** — the same codebase as Books 1 and 2.

---

## Prerequisites

This is **Book 3** in the series. You should have read:
- **Book 1**: Understanding RDD — the data model
- **Book 2**: Understanding the Scheduler — stages, tasks, execution

You need to know how tasks get shipped to remote machines and executed there (Book 2, Chapters 5 and 10).

---

## The Two Shared Variables

Every Spark program has three primitives:
- **RDDs** — distributed collections (Books 1 and 2)
- **Broadcast variables** — read-only data sent to every machine (this book, Part 1)
- **Accumulators** — write-only counters aggregated back to the driver (this book, Part 2)

```
Driver                              Workers
──────                              ───────

Broadcast variable ──── read ────→  Every task can read it
(large lookup table,                (but no task can change it)
 ML model, config)

Accumulator        ←── write ────   Every task can add to it
(error count,                       (but no task can read the global value)
 bytes processed)
```

---

## Table of Contents

### Part 1: Broadcast — Sending Data to Every Machine
- [Chapter 1: The Problem — Why Broadcast Exists](Chapter-01-The-Problem.md)
- [Chapter 2: HttpBroadcast — The Simple Way](Chapter-02-HttpBroadcast.md)
- [Chapter 3: The Serialization Trick — How readObject Powers Everything](Chapter-03-Serialization-Trick.md)
- [Chapter 4: TreeBroadcast — Scaling the Distribution](Chapter-04-TreeBroadcast.md)
- [Chapter 5: BitTorrentBroadcast — Peer-to-Peer Distribution](Chapter-05-BitTorrentBroadcast.md)
- [Chapter 6: Choosing a Strategy — The Factory Pattern](Chapter-06-Choosing-Strategy.md)

### Part 2: Accumulators — Collecting Data Back
- [Chapter 7: The Problem — Why Accumulators Exist](Chapter-07-Accumulator-Problem.md)
- [Chapter 8: How Accumulators Work — The Thread-Local Trick](Chapter-08-How-Accumulators-Work.md)

### Part 3: The Big Picture
- [Chapter 9: Putting It All Together](Chapter-09-Putting-It-Together.md)
