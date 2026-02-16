# Chapter 11: UnionRDD — Combining Datasets

Sometimes you have multiple RDDs with the same type of data and you want to combine them into one. `UnionRDD` does exactly this — like concatenating multiple `ArrayList`s in Java.

---

## 11.1 The Use Case

```scala
val logsJanuary = sc.textFile("logs-2024-01.txt")
val logsFebruary = sc.textFile("logs-2024-02.txt")
val allLogs = logsJanuary.union(logsFebruary)
// or: val allLogs = logsJanuary ++ logsFebruary
```

The `allLogs` RDD contains all lines from both files. No data is moved or copied — UnionRDD just **logically joins** the two RDDs.

---

## 11.2 How Splits Work

UnionRDD's splits are the **concatenation** of all parent RDDs' splits:

```scala
@transient
val splits_ : Array[Split] = {
    val array = new Array[Split](rdds.map(_.splits.size).sum)
    var pos = 0
    for (rdd <- rdds; split <- rdd.splits) {
        array(pos) = new UnionSplit(pos, rdd, split)
        pos += 1
    }
    array
}
```

**Example:**
```
RDD A has 3 splits: [A₀, A₁, A₂]
RDD B has 2 splits: [B₀, B₁]

UnionRDD splits: [UnionSplit(0,A,A₀), UnionSplit(1,A,A₁), UnionSplit(2,A,A₂), 
                   UnionSplit(3,B,B₀), UnionSplit(4,B,B₁)]

Total: 5 splits
```

Each `UnionSplit` remembers: "I am split #3 of the UnionRDD, but I actually represent split B₀ from RDD B."

---

## 11.3 How Compute Works

```scala
override def compute(s: Split): Iterator[T] = s.asInstanceOf[UnionSplit[T]].iterator()
```

And in `UnionSplit`:
```scala
def iterator() = rdd.iterator(split)
```

Dead simple — to compute a UnionSplit, just **delegate** to the original parent RDD and split. UnionRDD doesn't transform data at all; it just routes each split to the right parent.

---

## 11.4 Dependencies — RangeDependency

```scala
@transient override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for ((rdd, index) <- rdds.zipWithIndex) {
        deps += new RangeDependency(rdd, 0, pos, rdd.splits.size)
        pos += rdd.splits.size
    }
    deps.toList
}
```

```
RDD A (3 splits) → RangeDependency(inStart=0, outStart=0, length=3)
  Union splits 0,1,2 → A splits 0,1,2

RDD B (2 splits) → RangeDependency(inStart=0, outStart=3, length=2)
  Union splits 3,4 → B splits 0,1
```

All dependencies are **narrow** — each UnionRDD partition maps to exactly one parent partition. No shuffle needed!

---

## 11.5 Summary

| Property | UnionRDD |
|----------|----------|
| **Splits** | Concatenation of all parents' splits |
| **Compute** | Delegate to the original parent's iterator |
| **Dependencies** | RangeDependency (narrow) per parent |
| **Shuffle?** | No |
| **Data movement?** | None — just logical combination |

---

**Next Chapter**: [Chapter 12: CartesianRDD — The Cross Product →](Chapter-12-CartesianRDD.md)
