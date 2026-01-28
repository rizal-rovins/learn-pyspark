Now that we understand **Transformations** and **Actions**, we need to talk about the core data structure that powers them: the **RDD** (Resilient Distributed Dataset).

While modern Spark development (PySpark/Spark SQL) mostly uses **DataFrames**, RDDs are the fundamental building blocks running underneath everything. Understanding them explains *why* Spark behaves the way it does.

## What is an RDD?

**RDD** stands for **R**esilient **D**istributed **D**ataset. It is an immutable, distributed collection of elements that can be processed in parallel across the cluster.

Let’s break down the acronym:

*   **Resilient (Fault-Tolerant):** If a node fails while processing a partition of data, Spark can **reconstruct** that lost partition automatically using the lineage (the history of transformations) without needing to restart the entire job. For example, if a worker crashes while filtering and aggregating server logs, Spark simply re-reads the original log file partition and reapplies the filter and aggregation steps on a different node and the rest of the job continues uninterrupted.
*   **Distributed:** The data in an RDD is divided into logical chunks called **partitions**, which are computed on different nodes of the cluster simultaneously.
*   **Dataset:** It is simply a collection of data objects (like a list of strings, integers, or custom objects).

## Why do we still talk about RDDs?

You might ask: *"If DataFrames are faster and easier, why learn RDDs?"*

1.  **Under the Hood:** DataFrames are actually built *on top* of RDDs. When you run a DataFrame query, Spark compiles it down to RDD code.
2.  **Unstructured Data:** RDDs are powerful when your data has no schema (e.g., raw text files, media, complex logs) and you need low-level control.
3.  **Debugging:** Understanding RDD partitions and lineage helps you debug performance issues like data skew and excessive shuffling in DataFrames.


**Simply explained:**
- **Coding with RDDs tell Spark "HOW" to do something**: You explicitly define each transformation step with imperative code, giving you full control over the execution logic.
- **Coding with DataFrames tell Spark "WHAT" to do**: You declare your intent using high-level SQL-like operations, and the Catalyst optimizer figures out the best "how" automatically.

For example, with RDDs you manually specify "filter these tuples, then map this function, then reduce with this logic," whereas with DataFrames you simply say "select these columns where this condition is true" and Spark optimizes the execution plan for you.

## Key Characteristics of RDDs

### 1. Immutability
Once you create an RDD, you cannot change it. To modify data, you must apply a transformation (like `map` or `filter`) to create a *new* RDD. This immutability is key to Spark's fault tolerance.

### 2. In-Memory Computation
RDDs store data in memory (RAM) across executors. This is what makes Spark 10x-100x faster than MapReduce, which writes to disk after every step.

### 3. Lazy Evaluation
Just like DataFrames, RDDs are lazy. They don't load or process data until an **Action** (like `count()` or `collect()`) is called.

## Creating RDDs

There are two main ways to create an RDD in Spark:

**1. Parallelizing a Collection**
Taking a local list in your driver program and distributing it to the cluster.
```python
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
```

**2. Loading External Data**
Reading from a file system like HDFS, S3, or a local file.
```python
rdd = spark.sparkContext.textFile("path/to/file.txt")
```

## Transformations on RDDs

RDDs support the same **Transformations** and **Actions** we discussed in the previous post.

*   **Narrow Transformations:** `map`, `filter`, `flatMap` (No shuffle required).
*   **Wide Transformations:** `reduceByKey`, `groupByKey`, `distinct` (Requires shuffling data across partitions).

Let's look at the examples of these transformations. Try to reason *why* these are narrow or wide.

### Narrow Transformations (No Shuffle)

These operations work independently on each partition without moving data across the network.

#### `map(func)`
Applies a function to each element, producing exactly one output element per input element.

**Example:**
```python
# Double each number
rdd = sc.parallelize([1, 2, 3, 4, 5])
doubled = rdd.map(lambda x: x * 2)
# Result: [2, 4, 6, 8, 10]
```

<details>
<summary><strong>Why is this narrow?</strong></summary>

Each partition processes its own elements independently. If partition 1 has `[1, 2]` and partition 2 has `[3, 4, 5]`, doubling happens locally without any cross-partition communication.
</details>

#### `filter(func)`
Selects only elements that satisfy a condition.

**Example:**
```python
# Keep only even numbers
rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
evens = rdd.filter(lambda x: x % 2 == 0)
# Result: [2, 4, 6]
```

<details>
<summary><strong>Why is this narrow?</strong></summary>

Each partition filters its own data independently without the need to look at other partitions. The filtering decision for each element depends only on that element itself.
</details>

#### `flatMap(func)`
Applies a function that returns a sequence, then flattens all results into a single RDD.

**Example:**
```python
# Split sentences into individual words
rdd = sc.parallelize(["hello world", "spark is fast"])
words = rdd.flatMap(lambda line: line.split(" "))
# Result: ["hello", "world", "spark", "is", "fast"]
```

<details>
<summary><strong>Why is this narrow?</strong></summary>

Each sentence is split within its own partition. No data needs to move between partitions - partition 1 splits its sentences, partition 2 splits its sentences, all independently.
</details>

***

### Wide Transformations (Shuffle Required)

These operations require redistributing data across partitions because output depends on multiple input partitions.

#### `reduceByKey(func)`
Aggregates values for each key using a reduce function. Performs local aggregation within each partition before shuffling, making it more efficient.

**Example:**
```python
# Count word occurrences
rdd = sc.parallelize([("spark", 1), ("hadoop", 1), ("spark", 1), ("spark", 1)])
counts = rdd.reduceByKey(lambda a, b: a + b)
# Result: [("spark", 3), ("hadoop", 1)]
```

<details>
<summary><strong>Why is this wide?</strong></summary>

The key "spark" might exist in partition 1, partition 3, and partition 5. To calculate the correct total count, Spark must shuffle all "spark" entries to the same partition for final aggregation.
</details>

#### `groupByKey()`
Groups all values for each key together. Less efficient than `reduceByKey` because it shuffles all values before any aggregation.

**Example:**
```python
# Group transactions by user
rdd = sc.parallelize([("user1", 100), ("user2", 50), ("user1", 200), ("user2", 150)])
grouped = rdd.groupByKey()
# Result: [("user1", [100, 200]), ("user2", [50, 150])]
```

<details>
<summary><strong>Why is this wide?</strong></summary>

All values for "user1" must be brought together from wherever they exist across partitions. This requires shuffling all data across the network. Unlike `reduceByKey`, there's no local pre-aggregation to reduce the data volume.
</details>

#### `distinct()`
Returns only unique elements from the RDD.

**Example:**
```python
# Remove duplicate customer IDs
rdd = sc.parallelize([101, 102, 103, 101, 104, 102, 105])
unique = rdd.distinct()
# Result: [101, 102, 103, 104, 105]
```

<details>
<summary><strong>Why is this wide?</strong></summary>

To identify duplicates, Spark must compare elements across all partitions. ID 101 in partition 1 needs to be checked against partition 2, partition 3, etc. This requires a global shuffle to ensure no duplicates remain.
</details>

***

**Example: Word Count with RDDs**
```python
# 1. Read lines from a file
lines = sc.textFile("data.txt")

# 2. Split lines into words (Narrow Transformation)
words = lines.flatMap(lambda line: line.split(" "))

# 3. Create (word, 1) pairs (Narrow Transformation)
pairs = words.map(lambda word: (word, 1))

# 4. Count occurrences (Wide Transformation - Shuffle)
counts = pairs.reduceByKey(lambda a, b: a + b)

# 5. Trigger execution (Action)
output = counts.collect()
print(output)
```

## Summary

| Feature | Description |
| :--- | :--- |
| **Fundamental Unit** | The basic data unit in Spark. |
| **Immutable** | Read-only; creates new RDDs on change. |
| **Partitions** | The unit of parallelism; 1 partition = 1 task. |
| **Fault Tolerance** | Recomputes lost data using lineage graph. |
| **Type** | Low-level, no schema enforced (unlike DataFrames). |
