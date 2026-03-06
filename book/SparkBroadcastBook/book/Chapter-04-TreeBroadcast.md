# Chapter 4: TreeBroadcast — Scaling the Distribution

## 4.1 The Bottleneck We Need to Solve

HttpBroadcast has one source: the driver. With 50 workers downloading 2 GB each, the driver sends 100 GB. With 500 workers, it's 1 TB. The driver's network link is the bottleneck.

The insight: once Worker 1 has the data, *it* can serve it to Worker 2. Worker 1 doesn't need to sit idle while Workers 2–50 wait for the driver. If we organize the workers into a tree, the load spreads across multiple machines.

---

## 4.2 The Idea: A Distribution Tree

Instead of every worker downloading from the driver:

```
HttpBroadcast (star topology):

        Driver
      ╱ │ │ │ ╲
    W1 W2 W3 W4 W5 ... W50

Driver sends 50 copies. Bottleneck: driver's network.
```

TreeBroadcast organizes workers into a tree:

```
TreeBroadcast (tree topology, MaxDegree=2):

              Driver
             ╱      ╲
           W1        W2
          ╱  ╲      ╱  ╲
        W3    W4  W5    W6
       ╱  ╲
     W7    W8
     ...
```

The driver sends the data to 2 workers (its "children"). Each of those sends to 2 more. Each of those sends to 2 more. The load fans out exponentially.

With `MaxDegree = 2` (binary tree) and 50 workers:
- **HttpBroadcast**: driver sends 50 copies. Time ≈ 50 × transfer_time.
- **TreeBroadcast**: 6 levels deep (log₂(50) ≈ 6). Time ≈ 6 × transfer_time. Each machine sends at most 2 copies.

That's roughly 8x faster for 50 workers. For 500 workers, it's about 9 levels vs. 500 copies — 55x faster.

---

## 4.3 How It Works

TreeBroadcast splits the data into **blocks** (default 4 KB each). A 2 GB broadcast becomes ~500,000 blocks. This block-level chunking enables streaming: a worker can start serving blocks to its children before it has received all blocks itself.

The system has three components:

**The Tracker** (on the driver) — a server that workers contact to find the Guide for a specific broadcast variable. Think of it as a phone book: "broadcast UUID → guide address."

**The Guide** (on the driver, one per broadcast variable) — manages the tree. When a worker connects, the Guide picks a parent for it (the node with the most leechers, to fill the tree level by level) and tells the worker where to download from.

**The Server** (on every machine that has data) — serves blocks to children. Once a worker has received blocks, it starts its own server so it can serve those blocks to its children in the tree.

---

## 4.4 The Flow

```
1. Driver creates broadcast variable
   → Splits data into blocks
   → Starts Guide (manages the tree)
   → Starts Server (serves blocks)
   → Registers with Tracker

2. Worker's task deserializes Broadcast object (readObject)
   → Cache miss — need to download
   → Contacts Tracker: "Where is the Guide for UUID xxx?"
   → Tracker returns Guide's address

3. Worker contacts Guide
   → Guide picks a parent: "Download from Worker 3 at host:port"
   → Guide adds this worker to the tree

4. Worker downloads blocks from its assigned parent
   → Starts its own Server (so it can serve its future children)
   → As blocks arrive, they become available to serve

5. Worker finishes downloading
   → Reports back to Guide
   → Caches the data locally
```

---

## 4.5 The Streaming Advantage

Because data is split into blocks, a worker doesn't have to wait until it has everything before serving. As soon as Worker 1 receives block 0 from the driver, it can serve block 0 to Worker 3 — even while it's still receiving blocks 1, 2, 3, ... from the driver.

This means the tree levels overlap in time:

```
Time →
Driver:   [block 0][block 1][block 2][block 3]...
Worker 1:     [block 0][block 1][block 2]...        ← starts after receiving block 0
Worker 3:         [block 0][block 1]...              ← starts after W1 has block 0
Worker 7:             [block 0]...                   ← starts after W3 has block 0
```

The pipeline fills up quickly. By the time the driver finishes sending, most of the tree has already received most of the data.

---

## 4.6 The Tradeoff

TreeBroadcast is much better than HttpBroadcast for large clusters. But it has a rigidity: the tree structure is fixed once a worker joins. If a worker in the middle of the tree crashes, its entire subtree loses its source. The Guide doesn't dynamically reassign parents.

Also, the tree is built incrementally as workers connect. The first workers get good positions (close to the root). Late workers end up deep in the tree and have to wait longer.

BitTorrentBroadcast (Chapter 5) solves both of these problems with a more flexible peer-to-peer approach.

---

## 4.7 Summary

| Question | Answer |
|----------|--------|
| What problem does TreeBroadcast solve? | The driver bottleneck. Spreads the load across a tree of workers. |
| How does it work? | Workers form a tree. Each downloads from its parent and serves its children. |
| What is the Guide? | A coordinator on the driver that assigns parents to new workers. |
| Why split into blocks? | Enables streaming — workers can serve blocks before receiving everything. |
| What's the tradeoff? | Rigid tree structure. If a middle node fails, its subtree is cut off. |

---

**Next Chapter**: [Chapter 5: BitTorrentBroadcast — Peer-to-Peer Distribution →](Chapter-05-BitTorrentBroadcast.md)
