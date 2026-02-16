# Chapter 16: SampledRDD — Random Sampling

`SampledRDD` returns a random subset of its parent's data. It's used for exploratory analysis, approximate computations, and building `RangePartitioner`s.

---

## 16.1 The Use Case

```scala
val fullData = sc.textFile("huge_file.txt")     // 100 million lines
val sample = fullData.sample(false, 0.01, 42)   // ~1% sample, ~1 million lines
val sampleWithReplacement = fullData.sample(true, 0.05, 42)  // ~5% with replacement
```

---

## 16.2 Deterministic Seeds Per Partition

Each partition gets its own random seed derived from the master seed:

```scala
@transient
val splits_ = {
    val rg = new Random(seed);
    prev.splits.map(x => new SampledRDDSplit(x, rg.nextInt))
}
```

This ensures:
- **Reproducibility**: Same seed → same sample every time
- **Independence**: Each partition samples independently with its own seed

---

## 16.3 How Compute Works

```scala
override def compute(splitIn: Split) = {
    val split = splitIn.asInstanceOf[SampledRDDSplit]
    val rg = new Random(split.seed);
    
    if (withReplacement) {
        // Sampling WITH replacement
        val oldData = prev.iterator(split.prev).toArray
        val sampleSize = (oldData.size * frac).ceil.toInt
        val sampledData = {
            for (i <- 1 to sampleSize)
                yield oldData(rg.nextInt(oldData.size))
        }
        sampledData.iterator
    } else {
        // Sampling WITHOUT replacement
        prev.iterator(split.prev).filter(x => (rg.nextDouble <= frac))
    }
}
```

**Without replacement** (the common case):
```java
// Java equivalent — beautifully simple
public Iterator<T> compute(Split split) {
    Random rng = new Random(split.seed);
    Iterator<T> parentData = prev.iterator(split.prev);
    
    // For each element, flip a biased coin
    return new FilteringIterator<>(parentData, 
        element -> rng.nextDouble() <= fraction  // keep with probability = fraction
    );
}
```

Each element independently has a `fraction` probability of being included. With `fraction = 0.1`, about 10% of elements pass through.

**With replacement**: Load the entire partition into an array, then randomly pick elements (can pick the same one multiple times).

---

## 16.4 Summary

| Property | SampledRDD |
|----------|-----------|
| **Splits** | Same as parent (each with a unique seed) |
| **Compute** | Without replacement: filter with random coin flip. With replacement: random index picks. |
| **Dependencies** | OneToOneDependency |
| **Shuffle?** | No |
| **Deterministic?** | Yes — same seed always produces same sample |

---

**Next Chapter**: [Chapter 17: PairRDDFunctions — The Key-Value Power Tools →](Chapter-17-PairRDDFunctions.md)
