# Chapter 15: PipedRDD — Shelling Out to External Programs

`PipedRDD` lets you pipe each partition's data through an **external command** — any program that reads from stdin and writes to stdout. This is how you use non-JVM tools (Python scripts, shell commands, C programs) inside a Spark pipeline.

---

## 15.1 The Use Case

```scala
// Sort each partition using the Unix 'sort' command
val sorted = rdd.pipe("sort")

// Process data through a Python script
val processed = rdd.pipe("python3 process.py")

// Use awk to extract fields
val fields = rdd.pipe("awk '{print $2}'")
```

---

## 15.2 The Java Analogy

PipedRDD is essentially doing this for each partition:

```java
// Java equivalent of what PipedRDD does
Process process = Runtime.getRuntime().exec("sort");

// Thread 1: Feed data to the process's stdin
new Thread(() -> {
    PrintWriter out = new PrintWriter(process.getOutputStream());
    for (String element : partition) {
        out.println(element);
    }
    out.close();
}).start();

// Thread 2: Read stderr and print to console
new Thread(() -> {
    BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
    String line;
    while ((line = err.readLine()) != null) {
        System.err.println(line);
    }
}).start();

// Main: Read results from the process's stdout
BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
List<String> results = new ArrayList<>();
String line;
while ((line = in.readLine()) != null) {
    results.add(line);
}
```

---

## 15.3 How It Works

```scala
override def compute(split: Split): Iterator[String] = {
    val pb = new ProcessBuilder(command)
    val currentEnvVars = pb.environment()
    envVars.foreach { case(variable, value) => currentEnvVars.put(variable, value) }
    val proc = pb.start()
    val env = SparkEnv.get

    // Thread: Print process's stderr to our stderr
    new Thread("stderr reader for " + command) {
        override def run() {
            for(line <- Source.fromInputStream(proc.getErrorStream).getLines)
                System.err.println(line)
        }
    }.start()

    // Thread: Feed parent's data to process's stdin
    new Thread("stdin writer for " + command) {
        override def run() {
            SparkEnv.set(env)
            val out = new PrintWriter(proc.getOutputStream)
            for(elem <- parent.iterator(split)) out.println(elem)
            out.close()
        }
    }.start()

    // Return: Lines from process's stdout
    Source.fromInputStream(proc.getInputStream).getLines
}
```

Three things happen in parallel:
1. **Writer thread**: Feeds parent partition data to the process's stdin (one element per line)
2. **Error thread**: Reads process's stderr and prints it (for debugging)
3. **Main iterator**: Reads process's stdout, returning lines one at a time

---

## 15.4 Summary

| Property | PipedRDD |
|----------|---------|
| **Splits** | Same as parent |
| **Compute** | Spawn external process, pipe data through stdin/stdout |
| **Dependencies** | OneToOneDependency |
| **Shuffle?** | No |
| **Element type** | Always `String` (stdout lines) |
| **Use case** | Integrating non-JVM tools into Spark pipelines |

---

**Next Chapter**: [Chapter 16: SampledRDD — Random Sampling →](Chapter-16-SampledRDD.md)
