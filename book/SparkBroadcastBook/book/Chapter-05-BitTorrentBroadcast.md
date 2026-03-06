# Chapter 5: BitTorrentBroadcast — Peer-to-Peer Distribution

## 5.1 The Limitation of Trees

TreeBroadcast is a big improvement over HttpBroadcast. But it has a structural problem: each worker has exactly one source (its parent in the tree). If that parent is slow or crashes, the worker is stuck.

Also, the tree doesn't use the network efficiently. Worker 1 might have blocks 0–100 and Worker 2 might have blocks 101–200, but in a tree, Worker 2 can only get blocks from its parent — it can't grab blocks 0–100 from Worker 1 even though Worker 1 has them.

What if every worker could download from *any* other worker that has the blocks it needs? That's the BitTorrent model.

---

## 5.2 How BitTorrent Works (The Real-World Protocol)

If you've ever used BitTorrent to download a file, you know the basic idea:

1. A file is split into **blocks** (small chunks)
2. A **tracker** knows which peers have which blocks
3. Each peer downloads blocks it's missing from peers that have them
4. As soon as a peer gets a block, it can share that block with others
5. Peers exchange blocks in parallel — everyone is both downloading and uploading

The result: the more peers join, the *faster* the distribution gets. Each new peer adds both demand and supply. This is the opposite of HttpBroadcast, where more workers means more load on the single source.

---

## 5.3 Spark's BitTorrentBroadcast

Spark 0.5.0 implements a simplified version of this protocol. Here are the key components:

**The Guide** (on the driver) — like a BitTorrent tracker. Workers periodically report which blocks they have. The Guide responds with a list of peers that have blocks the worker needs.

**Block-level tracking** — each worker maintains a `BitSet` (one bit per block) indicating which blocks it has. The Guide collects these BitSets from all workers.

**Peer Chatter** — each worker runs multiple `TalkToPeer` threads that connect to other workers and exchange blocks. A worker can download from several peers simultaneously.

**Block selection** — when choosing which block to request, the worker can use different strategies:
- **Random**: pick any block the peer has that we don't
- **Rarest first**: pick the block that fewest peers have (to spread rare blocks faster)

---

## 5.4 The Flow

```
1. Driver creates broadcast variable
   → Splits 2 GB into ~500,000 blocks (4 KB each)
   → Starts Guide (the tracker)
   → Starts Server (serves blocks to peers)
   → Has all blocks → BitSet is all 1s

2. Worker A's task deserializes Broadcast (readObject)
   → Cache miss → contacts Tracker → gets Guide address
   → Starts its own Server (so peers can download from it)
   → Starts TalkToGuide thread (periodically reports status, gets peer list)
   → Starts PeerChatterController (manages TalkToPeer threads)

3. Worker A's TalkToGuide:
   → "I have blocks: {}" (empty BitSet)
   → Guide responds: "Here are peers with blocks: [Driver at host:port]"

4. Worker A's TalkToPeer connects to Driver:
   → Exchanges BitSets: "I have {} you have {all}"
   → Requests block 42 → receives it
   → Requests block 17 → receives it
   → Updates local BitSet: {17, 42}

5. Worker B arrives, contacts Guide:
   → Guide responds: "Peers: [Driver, Worker A]"
   → Worker B can download from BOTH Driver and Worker A!
   → Gets block 42 from Worker A, block 99 from Driver (in parallel)

6. As more workers arrive, more peers are available
   → Worker C can download from Driver, A, and B
   → The swarm grows — distribution accelerates
```

---

## 5.5 The End Game

There's a subtle problem in BitTorrent: the last few blocks. When a worker has 99% of the blocks, it's hard to find a peer that has the specific 1% it's missing. The worker might connect to peer after peer, none of whom have the right blocks.

Spark's BitTorrentBroadcast handles this with an **end game fraction** (default: 95%). Once a worker has received 95% of the blocks, it changes strategy: it starts requesting blocks that are already in transit from other peers. This means the same block might be downloaded twice, but it avoids the "last few blocks" stall.

```
Normal mode (< 95% complete):
  "I need block 42. Is anyone else already requesting it?"
  → Yes → skip it, ask for a different block
  → No → request it

End game mode (≥ 95% complete):
  "I need block 42. I don't care if someone else is requesting it too."
  → Request it anyway. Duplicates are OK — finishing fast matters more.
```

---

## 5.6 Why This Is Better

| | HttpBroadcast | TreeBroadcast | BitTorrentBroadcast |
|---|---|---|---|
| Sources | 1 (driver only) | 1 per worker (its parent) | Many (any peer with the block) |
| Parallelism | None | Limited by tree depth | Full — multiple peers simultaneously |
| Fault tolerance | Driver dies = stuck | Parent dies = subtree stuck | One peer dies = others still available |
| Scaling | Worse with more workers | Better (log N) | Best (more workers = more sources) |
| Complexity | Simple | Medium | High |

The tradeoff is complexity. HttpBroadcast is ~50 lines. BitTorrentBroadcast is ~800 lines with multiple threads, socket management, BitSet tracking, and peer selection strategies.

---

## 5.7 The Rarest-First Strategy

One of the most interesting details: when choosing which block to request from a peer, the "rarest first" strategy counts how many copies of each block exist across all known peers. It then requests the block with the fewest copies.

Why? Because rare blocks are the bottleneck. If block 42 exists on only one peer and that peer goes down, block 42 is lost. By prioritizing rare blocks, the swarm ensures that every block gets spread to multiple peers quickly, making the system more resilient.

This is the same strategy used by real BitTorrent clients — it's a well-studied algorithm from peer-to-peer networking.

---

## 5.8 Summary

| Question | Answer |
|----------|--------|
| What problem does BitTorrentBroadcast solve? | Maximizes distribution speed by letting every worker be both a downloader and an uploader. |
| How does it work? | Data is split into blocks. Workers exchange blocks peer-to-peer. A Guide tracks who has what. |
| What is the end game? | When a worker is 95%+ complete, it allows duplicate block requests to avoid stalling on the last few blocks. |
| What is rarest-first? | Request the block that fewest peers have, to spread rare blocks quickly and improve resilience. |
| Why not always use BitTorrent? | Complexity. For small broadcasts or small clusters, HttpBroadcast is simpler and fast enough. |

---

**Next Chapter**: [Chapter 6: Choosing a Strategy — The Factory Pattern →](Chapter-06-Choosing-Strategy.md)
