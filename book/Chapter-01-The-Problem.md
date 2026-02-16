# Chapter 1: The Problem — Processing Big Data Across Machines

## 1.1 A Simple Scenario

Imagine you work at a large company. Your web servers produce log files — one line for every page a user visits. After a year, you have **1 terabyte** (1,000 GB) of log data.

Your boss asks: *"How many times was the `/checkout` page visited last year?"*

If you were a Java developer, you'd write something like this:

```java
// Pseudocode — single machine approach
BufferedReader reader = new BufferedReader(new FileReader("access.log"));
long count = 0;
String line;
while ((line = reader.readLine()) != null) {
    if (line.contains("/checkout")) {
        count++;
    }
}
System.out.println("Checkout visits: " + count);
```

Simple, right? There's just one problem: **1 TB of data doesn't fit on one machine's disk easily, and even if it did, reading it sequentially would take hours.**

## 1.2 Spreading the Data Across Machines

The solution is to spread the data. Instead of one giant file, you split it into, say, **100 chunks** of 10 GB each, and put each chunk on a different machine.

```
Machine 1:  chunk_001.log  (10 GB)
Machine 2:  chunk_002.log  (10 GB)
Machine 3:  chunk_003.log  (10 GB)
...
Machine 100: chunk_100.log (10 GB)
```

This is exactly what **HDFS (Hadoop Distributed File System)** does. When you store a file in HDFS, it automatically splits the file into **blocks** (typically 64 MB or 128 MB each) and distributes them across the machines in your cluster.

## 1.3 The Coordination Nightmare

Now, to count `/checkout` visits, you need to:

1. **Tell each machine**: "Read your chunk, count lines containing `/checkout`"
2. **Collect the results**: Get 100 numbers back
3. **Add them up**: Sum to get the total

Sounds manageable for this example. But what if you need to do something harder?

**Example: "What are the top 10 most visited pages?"**

Now you need to:
1. Tell each machine: "Read your chunk, count visits per page"
2. Collect the results: Get 100 `HashMap<String, Integer>`s back
3. **Merge all the HashMaps**: For each page URL, add up counts from all 100 machines
4. Sort by count, take top 10

What if a machine **crashes** halfway through? What if the network **drops**? What if one machine is **slow** and holds up the others?

If you had to write all this coordination, networking, failure recovery, and data shuffling code yourself, you'd be writing thousands of lines of infrastructure code before you even get to your actual business logic.

## 1.4 What We Really Want

What if you could write code like this?

```java
// Dream pseudocode
DistributedList<String> lines = readFromHDFS("access.log");

// This runs across 100 machines automatically:
long count = lines.filter(line -> line.contains("/checkout")).count();

// Or for top 10 pages:
DistributedList<Pair<String, Integer>> pageCounts = lines
    .map(line -> extractPageUrl(line))     // extract the URL from each line
    .map(url -> new Pair<>(url, 1))        // turn each URL into (url, 1)
    .reduceByKey((a, b) -> a + b);         // sum up all the 1s for each URL

List<Pair<String, Integer>> top10 = pageCounts
    .sortBy(pair -> pair.getValue())
    .take(10);
```

Notice what this code does NOT contain:
- ❌ No network code
- ❌ No "which machine has which data" logic
- ❌ No failure handling
- ❌ No thread management
- ❌ No data serialization

You just write the **logic** — filter, map, reduce — and *something* handles the distributed execution for you.

## 1.5 That "Something" Is the RDD

This is exactly what Apache Spark provides. The "DistributedList" in our dream pseudocode is called an **RDD — Resilient Distributed Dataset**.

Let's break down the name:

| Word | Meaning |
|------|---------|
| **Resilient** | If a machine crashes and some data is lost, Spark can **recover** it automatically (we'll learn how in Chapter 5 and Chapter 18) |
| **Distributed** | The data lives across **multiple machines** in the cluster, not on a single computer |
| **Dataset** | It's a **collection of elements** — like a Java `List<T>`, but spread across the cluster |

An RDD is Spark's answer to the question: *"How do I work with a collection of data that's too big for one machine, as easily as I'd work with a Java List?"*

## 1.6 The Key Insight: An RDD Is Not Data — It's a Recipe

Here's the most important thing to understand early on, and it's counterintuitive:

**An RDD does not hold data. It holds a *description* of how to get or compute data.**

Think of it like this:

| Concept | Java Analogy |
|---------|-------------|
| A `List<String>` | The actual data is in memory — you can look at element 0, element 1, etc. |
| An `RDD<String>` | A set of **instructions**: "To get the data, read these HDFS blocks, then apply this filter, then apply this map function" |

It's like a recipe in a cookbook vs. the actual cooked meal. The recipe tells you *how* to make the meal, but no cooking happens until someone decides to eat.

In Spark:
- **Creating an RDD** = Writing down the recipe (no computation happens)
- **Calling an action** (like `count()` or `collect()`) = Cooking the meal (computation happens now)

This is called **lazy evaluation**, and we'll explore it deeply in Chapters 8 and 9.

## 1.7 What You'll Learn in This Book

Over the next 18 chapters, you'll learn:

1. **The building blocks** that make up an RDD (Chapters 3–6)
2. **How the base RDD class works** (Chapter 7)
3. **How transformations and actions work** (Chapters 8–9)
4. **Every type of RDD** in Spark 0.5.0 and what it does (Chapters 10–17)
5. **How fault tolerance works** through lineage (Chapter 18)
6. **A complete end-to-end example** traced through the code (Chapter 19)

But first, since all of Spark's code is written in Scala, and you come from Java, let's spend one chapter getting comfortable reading Scala code.

---

**Next Chapter**: [Chapter 2: A Gentle Scala Survival Guide for Java Developers →](Chapter-02-Scala-for-Java-Developers.md)
