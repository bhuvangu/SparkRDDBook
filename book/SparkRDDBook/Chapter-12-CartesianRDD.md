# Chapter 12: CartesianRDD — The Cross Product

A Cartesian product combines every element of one dataset with every element of another. If you've ever written a nested for-loop, you understand this.

---

## 12.1 The Java Analogy

```java
List<String> colors = Arrays.asList("red", "blue");
List<Integer> sizes = Arrays.asList(1, 2, 3);

// Cartesian product — nested for-loop
List<Pair<String, Integer>> result = new ArrayList<>();
for (String color : colors) {
    for (Integer size : sizes) {
        result.add(new Pair<>(color, size));
    }
}
// Result: [(red,1), (red,2), (red,3), (blue,1), (blue,2), (blue,3)]
```

In Spark:
```scala
val colors = sc.parallelize(List("red", "blue"))
val sizes = sc.parallelize(List(1, 2, 3))
val product = colors.cartesian(sizes)
// Result: [(red,1), (red,2), (red,3), (blue,1), (blue,2), (blue,3)]
```

---

## 12.2 How Splits Work — Multiplication

If RDD A has `m` splits and RDD B has `n` splits, CartesianRDD has `m × n` splits:

```scala
val numSplitsInRdd2 = rdd2.splits.size

@transient
val splits_ = {
    val array = new Array[Split](rdd1.splits.size * rdd2.splits.size)
    for (s1 <- rdd1.splits; s2 <- rdd2.splits) {
        val idx = s1.index * numSplitsInRdd2 + s2.index
        array(idx) = new CartesianSplit(idx, s1, s2)
    }
    array
}
```

**Example**: RDD A (2 splits) × RDD B (3 splits) = 6 CartesianSplits:

```
         B₀        B₁        B₂
A₀    Split 0   Split 1   Split 2     ← A₀ paired with each of B
A₁    Split 3   Split 4   Split 5     ← A₁ paired with each of B

Split 0 = (A₀, B₀)  — all pairs from A's partition 0 × B's partition 0
Split 1 = (A₀, B₁)  — all pairs from A's partition 0 × B's partition 1
Split 2 = (A₀, B₂)  — all pairs from A's partition 0 × B's partition 2
Split 3 = (A₁, B₀)
Split 4 = (A₁, B₁)
Split 5 = (A₁, B₂)
```

---

## 12.3 How Compute Works — Nested Iteration

```scala
override def compute(split: Split) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    for (x <- rdd1.iterator(currSplit.s1); y <- rdd2.iterator(currSplit.s2)) yield (x, y)
}
```

**In Java terms:**
```java
public Iterator<Pair<T, U>> compute(Split split) {
    CartesianSplit cs = (CartesianSplit) split;
    Iterator<T> leftData = rdd1.iterator(cs.s1);
    Iterator<U> rightData = rdd2.iterator(cs.s2);
    
    // Nested loop: for each x in left, pair with each y in right
    List<Pair<T,U>> pairs = new ArrayList<>();
    List<U> rightList = toList(rightData);  // need to iterate right side multiple times
    while (leftData.hasNext()) {
        T x = leftData.next();
        for (U y : rightList) {
            pairs.add(new Pair<>(x, y));
        }
    }
    return pairs.iterator();
}
```

For CartesianSplit(A₀, B₁):
```
A₀ data: ["red", "blue"]
B₁ data: [2]

Pairs: ("red", 2), ("blue", 2)
```

---

## 12.4 Dependencies — Narrow with Math

```scala
override val dependencies = List(
    new NarrowDependency(rdd1) {
        def getParents(id: Int): Seq[Int] = List(id / numSplitsInRdd2)
    },
    new NarrowDependency(rdd2) {
        def getParents(id: Int): Seq[Int] = List(id % numSplitsInRdd2)
    }
)
```

The mapping uses division and modulo:
- Split `id` in CartesianRDD needs `rdd1` partition `id / numSplitsInRdd2`
- Split `id` in CartesianRDD needs `rdd2` partition `id % numSplitsInRdd2`

```
With numSplitsInRdd2 = 3:
Split 0: rdd1[0/3=0], rdd2[0%3=0] → (A₀, B₀) ✓
Split 1: rdd1[1/3=0], rdd2[1%3=1] → (A₀, B₁) ✓
Split 2: rdd1[2/3=0], rdd2[2%3=2] → (A₀, B₂) ✓
Split 3: rdd1[3/3=1], rdd2[3%3=0] → (A₁, B₀) ✓
Split 4: rdd1[4/3=1], rdd2[4%3=1] → (A₁, B₁) ✓
Split 5: rdd1[5/3=1], rdd2[5%3=2] → (A₁, B₂) ✓
```

**No shuffle needed!** Each CartesianRDD partition knows exactly which two parent partitions it needs.

---

## 12.5 Summary

| Property | CartesianRDD |
|----------|-------------|
| **Splits** | m × n (product of parents' split counts) |
| **Compute** | Nested for-loop over two parent partition iterators |
| **Dependencies** | Two NarrowDependencies (division/modulo mapping) |
| **Shuffle?** | No |
| **Warning** | Output size grows quadratically! |

---

**Next Chapter**: [Chapter 13: ShuffledRDD — The Network Shuffle →](Chapter-13-ShuffledRDD.md)
