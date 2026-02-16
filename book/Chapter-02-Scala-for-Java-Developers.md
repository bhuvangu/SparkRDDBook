# Chapter 2: A Gentle Scala Survival Guide for Java Developers

You don't need to *learn* Scala to read this book. You just need to *read* it. This chapter teaches you exactly enough Scala syntax so that when you see Spark's source code, you can understand what it's doing. Every concept is shown as a Java → Scala translation.

---

## 2.1 Variables: `val` and `var`

In Java, you make a variable constant with `final`. In Scala, you use `val` (value — cannot be reassigned) or `var` (variable — can be reassigned).

**Java:**
```java
final int x = 10;     // cannot reassign
int y = 20;            // can reassign
y = 30;                // OK
```

**Scala:**
```scala
val x = 10             // cannot reassign (like final)
var y = 20             // can reassign
y = 30                 // OK
```

Notice that Scala **doesn't require you to write the type** — it figures it out automatically. This is called **type inference**. But you *can* write the type if you want:

```scala
val x: Int = 10        // explicit type
val name: String = "hello"
```

**You'll see in Spark code**: `val` is used almost everywhere. RDDs and their fields are almost always immutable.

---

## 2.2 Methods: `def`

**Java:**
```java
public int add(int a, int b) {
    return a + b;
}
```

**Scala:**
```scala
def add(a: Int, b: Int): Int = {
    return a + b
}
```

Key differences:
- Types come **after** the parameter name, separated by `:` (not before like Java)
- The return type comes **after** the parameter list, separated by `:`
- The `=` sign before the body is required
- You can actually **omit** `return` — Scala uses the last expression as the return value:

```scala
def add(a: Int, b: Int): Int = {
    a + b    // last expression is automatically returned
}
```

Even shorter — if the body is a single expression, you can drop the curly braces:

```scala
def add(a: Int, b: Int): Int = a + b
```

**You'll see in Spark code**: Methods like `def splits`, `def compute(split: Split)`, etc.

---

## 2.3 Classes

**Java:**
```java
public class Person {
    private String name;
    private int age;
    
    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }
    
    public String getName() { return name; }
    public int getAge() { return age; }
}
```

**Scala:**
```scala
class Person(val name: String, val age: Int)
```

That's it — one line! The constructor parameters are declared **right in the class header**. Adding `val` before them automatically makes them publicly accessible fields.

If you need a class body with additional methods:

```scala
class Person(val name: String, val age: Int) {
    def greet(): String = "Hello, I'm " + name
}
```

**You'll see in Spark code**: 
```scala
class HadoopRDD[K, V](
    sc: SparkContext,
    @transient conf: JobConf,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends RDD[(K, V)](sc) {
```

This is a class `HadoopRDD` with constructor parameters `sc`, `conf`, `inputFormatClass`, etc. It extends (inherits from) `RDD`.

---

## 2.4 Abstract Classes

**Java:**
```java
public abstract class Shape {
    public abstract double area();
    
    public void describe() {
        System.out.println("I am a shape with area " + area());
    }
}
```

**Scala:**
```scala
abstract class Shape {
    def area(): Double               // no body = abstract
    
    def describe(): Unit = {         // Unit = Java's void
        println("I am a shape with area " + area())
    }
}
```

In Scala, a method without a body is automatically abstract — no `abstract` keyword needed on the method.

**You'll see in Spark code**: `abstract class RDD[T]` — the base RDD class is abstract, with abstract methods like `def splits` and `def compute(split: Split)`.

---

## 2.5 Traits (≈ Java Interfaces)

A Scala `trait` is like a Java interface, but it **can have method implementations** (like Java 8+ default methods).

**Java:**
```java
public interface Serializable {
    // marker interface, no methods
}

public interface Comparable<T> {
    int compareTo(T other);
}
```

**Scala:**
```scala
trait Serializable {
    // marker trait
}

trait Comparable[T] {
    def compareTo(other: T): Int
}
```

To implement a trait, use `extends` for the first one and `with` for additional ones:

**Java:**
```java
public class Person implements Serializable, Comparable<Person> { ... }
```

**Scala:**
```scala
class Person extends Serializable with Comparable[Person] { ... }
```

**You'll see in Spark code**:
```scala
trait Split extends Serializable {
    val index: Int
}
```

This is a trait (interface) `Split` that extends Java's `Serializable` and requires an `index` field.

---

## 2.6 Generics (Type Parameters)

Java uses `<T>`, Scala uses `[T]`. That's the only difference.

**Java:**
```java
public class Box<T> {
    private T content;
    public T get() { return content; }
}
Box<String> box = new Box<>();
```

**Scala:**
```scala
class Box[T] {
    var content: T = _    // _ means "default value"
    def get(): T = content
}
val box = new Box[String]
```

**You'll see in Spark code**: `class RDD[T]`, `class ShuffledRDD[K, V, C]`, `class HadoopRDD[K, V]` — these are all generic classes.

---

## 2.7 The `override` Keyword

In Java, `@Override` is optional (just a good practice annotation). In Scala, `override` is **required** — the compiler will error if you forget it.

**Java:**
```java
@Override
public String toString() { return "hello"; }
```

**Scala:**
```scala
override def toString(): String = "hello"
```

**You'll see in Spark code**:
```scala
override def splits = prev.splits
override val dependencies = List(new OneToOneDependency(prev))
override def compute(split: Split) = prev.iterator(split).map(f)
```

---

## 2.8 Lambdas (Anonymous Functions)

Java 8 introduced lambdas. Scala has had them from the start, and uses them heavily.

**Java:**
```java
Function<String, Integer> length = s -> s.length();
Predicate<String> isLong = s -> s.length() > 10;
list.stream().map(s -> s.toUpperCase()).collect(Collectors.toList());
```

**Scala:**
```scala
val length = (s: String) => s.length
val isLong = (s: String) => s.length > 10
list.map(s => s.toUpperCase)
```

When the function body is simple and the type is obvious, Scala allows an even shorter notation using `_` as a placeholder:

```scala
list.map(_.toUpperCase)      // same as: list.map(s => s.toUpperCase)
list.filter(_.length > 10)   // same as: list.filter(s => s.length > 10)
```

For two-parameter functions:
```scala
list.reduce(_ + _)           // same as: list.reduce((a, b) => a + b)
```

**You'll see in Spark code**: 
```scala
rdd.map(x => (x, 1))
rdd.filter(line => line.contains("/checkout"))
rdd.reduce((a, b) => a + b)
```

---

## 2.9 Collections: Array, List, Seq, Iterator

| Scala | Java Equivalent |
|-------|----------------|
| `Array[T]` | `T[]` (Java array) |
| `List[T]` | `java.util.LinkedList<T>` (immutable) |
| `Seq[T]` | `java.util.List<T>` (general sequence interface) |
| `Iterator[T]` | `java.util.Iterator<T>` |
| `Map[K, V]` | `java.util.Map<K, V>` |
| `ArrayBuffer[T]` | `java.util.ArrayList<T>` (mutable, growable) |

Creating collections:
```scala
val arr = Array(1, 2, 3)           // Java: new int[]{1, 2, 3}
val list = List(1, 2, 3)           // immutable linked list
val buf = new ArrayBuffer[Int]()   // like ArrayList<Integer>
buf += 10                          // like buf.add(10)
```

Scala collections come with built-in `map`, `filter`, `flatMap`, `reduce`, etc. — no need for `.stream()` like in Java 8:

```scala
val numbers = List(1, 2, 3, 4, 5)
numbers.map(_ * 2)        // List(2, 4, 6, 8, 10)
numbers.filter(_ > 3)     // List(4, 5)
numbers.flatMap(n => List(n, n * 10))  // List(1, 10, 2, 20, 3, 30, 4, 40, 5, 50)
```

**You'll see in Spark code**: `def splits: Array[Split]` — every RDD must return an array of its partitions.

---

## 2.10 `Option[T]` (≈ Java's `Optional<T>`)

Scala uses `Option[T]` to represent a value that might or might not exist.

**Java:**
```java
Optional<String> maybeName = Optional.of("Alice");
Optional<String> empty = Optional.empty();
```

**Scala:**
```scala
val maybeName: Option[String] = Some("Alice")
val empty: Option[String] = None
```

**You'll see in Spark code**:
```scala
val partitioner: Option[Partitioner] = None    // "this RDD has no partitioner"
override val partitioner = Some(part)          // "this RDD uses this partitioner"
```

---

## 2.11 Tuples (Pairs, Triples)

Scala has built-in tuples — like a lightweight class that holds 2 or more values.

**Java (no built-in tuples, you need a library or custom class):**
```java
// Using a custom Pair class
Pair<String, Integer> pair = new Pair<>("Alice", 25);
pair.getFirst();   // "Alice"
pair.getSecond();  // 25
```

**Scala:**
```scala
val pair = ("Alice", 25)          // type: (String, Int)  — which is shorthand for Tuple2[String, Int]
pair._1   // "Alice"
pair._2   // 25
```

**You'll see in Spark code**: `RDD[(K, V)]` — this means "an RDD of tuples, where each element is a (key, value) pair". This is how Spark represents key-value data, like a distributed `Map`.

---

## 2.12 Pattern Matching (≈ Java's `switch` on Steroids)

**Java:**
```java
switch (shape) {
    case "circle":
        System.out.println("round");
        break;
    case "square":
        System.out.println("boxy");
        break;
    default:
        System.out.println("unknown");
}
```

**Scala:**
```scala
shape match {
    case "circle" => println("round")
    case "square" => println("boxy")
    case _        => println("unknown")     // _ means "anything else"
}
```

But Scala's pattern matching is much more powerful — it can match on **types**:

```scala
dependency match {
    case s: ShuffleDependency[_, _, _] => 
        println("This is a shuffle with id " + s.shuffleId)
    case _ => 
        println("This is a narrow dependency")
}
```

**You'll see in Spark code** (from `CoGroupedRDD.scala`):
```scala
for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
    case NarrowCoGroupSplitDep(rdd, itsSplit) => {
        // handle narrow case
    }
    case ShuffleCoGroupSplitDep(shuffleId) => {
        // handle shuffle case
    }
}
```

---

## 2.13 `@transient` (Same as Java)

The `@transient` annotation works exactly like Java's `transient` keyword — it tells the serialization system: **"Don't serialize this field."**

**Java:**
```java
public class MyClass implements Serializable {
    transient int tempValue;  // won't be serialized
}
```

**Scala:**
```scala
class MyClass extends Serializable {
    @transient
    val tempValue = 42       // won't be serialized
}
```

**Why this matters in Spark**: RDD objects get serialized and sent to remote machines for execution. Some fields (like locally computed split arrays or references to SparkContext) shouldn't be sent over the network — they'll be recreated on the remote machine. These are marked `@transient`.

**You'll see in Spark code**:
```scala
@transient
val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))
```

---

## 2.14 `ClassManifest` (≈ Java's `Class<T>`)

In Java, generics are "erased" at runtime — a `List<String>` becomes just a `List` after compilation. Sometimes you need the type info at runtime (e.g., to create an array of `T`).

**Java solution:** Pass `Class<T>` explicitly:
```java
public <T> T[] createArray(Class<T> type, int size) {
    return (T[]) Array.newInstance(type, size);
}
```

**Scala solution:** Use `ClassManifest` (later renamed to `ClassTag`) — it's like an invisible `Class<T>` parameter that the compiler fills in automatically:

```scala
def createArray[T: ClassManifest](size: Int): Array[T] = new Array[T](size)
```

The `[T: ClassManifest]` part means: "The caller must provide a ClassManifest for T, but the compiler will do this automatically."

**You'll see in Spark code**:
```scala
abstract class RDD[T: ClassManifest](@transient sc: SparkContext)
```

Translation: "RDD is a generic class parameterized by type T. The runtime type information for T is available via ClassManifest." You can mostly **ignore this** when reading the code — just think of it as `class RDD<T>` in Java.

---

## 2.15 Implicit Conversions (Auto-Magic Type Conversion)

This is one Scala feature with no direct Java equivalent. It allows Scala to **automatically convert** one type to another.

**Concept**: If you have an `RDD[(String, Int)]` (an RDD of key-value pairs), Spark automatically "adds" extra methods like `reduceByKey`, `groupByKey`, etc. — even though those methods aren't defined in the base `RDD` class.

How? Through an **implicit conversion** defined in `SparkContext`:

```scala
// Somewhere in SparkContext:
implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)]) = 
    new PairRDDFunctions(rdd)
```

This says: "Whenever you have an `RDD[(K, V)]` and you call a method that `RDD` doesn't have (like `reduceByKey`), automatically wrap it in a `PairRDDFunctions` object and look for the method there."

**Java equivalent (roughly)**: It's like if Java could automatically call a wrapper constructor. Imagine:
```java
// Pseudocode — Java can't actually do this
rdd.reduceByKey(...)  
// Java would need: new PairRDDFunctions(rdd).reduceByKey(...)
// Scala does this wrapping automatically via implicits
```

**You'll see in Spark code**: Whenever you see `rdd.reduceByKey(...)` or `rdd.saveAsSequenceFile(...)`, these methods actually live on `PairRDDFunctions` and `SequenceFileRDDFunctions`, not on `RDD` itself.

---

## 2.16 Quick Reference Card

Here's a cheat sheet you can refer back to while reading Spark code:

| Scala | Java Equivalent | Example |
|-------|----------------|---------|
| `val x = 10` | `final int x = 10;` | Immutable variable |
| `var x = 10` | `int x = 10;` | Mutable variable |
| `def foo(a: Int): String` | `String foo(int a)` | Method declaration |
| `class Foo(val x: Int)` | `class Foo { final int x; Foo(int x) { this.x = x; } }` | Class with constructor |
| `abstract class Foo` | `abstract class Foo` | Abstract class |
| `trait Foo` | `interface Foo` | Interface (with possible default methods) |
| `extends A with B` | `extends A implements B` | Inheritance |
| `override def foo` | `@Override foo` | Override (required in Scala) |
| `Array[T]` | `T[]` | Array |
| `List[T]` | `List<T>` (immutable) | Linked list |
| `Seq[T]` | `List<T>` (general) | Sequence |
| `Iterator[T]` | `Iterator<T>` | Iterator |
| `Option[T]` / `Some(x)` / `None` | `Optional<T>` / `Optional.of(x)` / `Optional.empty()` | Maybe a value |
| `(a, b)` | `new Pair<>(a, b)` | Tuple |
| `x => x + 1` | `x -> x + 1` | Lambda |
| `_ + _` | `(a, b) -> a + b` | Lambda shorthand |
| `x match { case ... }` | `switch(x) { case ... }` | Pattern matching |
| `@transient` | `transient` | Don't serialize |
| `[T: ClassManifest]` | Passing `Class<T>` | Runtime type info |
| `Nil` | `Collections.emptyList()` | Empty list |
| `Unit` | `void` | No return value |

---

## 2.17 You're Ready!

With this chapter under your belt, you can now read any Scala code in Spark 0.5.0. You won't understand every Scala trick, but you'll understand the **structure and logic** — which is what matters.

Let's start building the RDD concept, one piece at a time.

---

**Next Chapter**: [Chapter 3: Split — What Is a Partition, Really? →](Chapter-03-Split.md)
