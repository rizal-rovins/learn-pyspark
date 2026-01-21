Now that we understand **Transformations** and **Actions**, we need to talk about the core data structure that powers them: the **RDD** (Resilient Distributed Dataset).

While modern Spark development (PySpark/Spark SQL) mostly uses **DataFrames**, RDDs are the fundamental building blocks running underneath everything. Understanding them explains *why* Spark behaves the way it does.

## What is an RDD?

**RDD** stands for **R**esilient **D**istributed **D**ataset. It is an immutable, distributed collection of elements that can be processed in parallel across the cluster.

Let’s break down the acronym:

*   **Resilient (Fault-Tolerant):** If a node fails while processing a partition of data, Spark can **reconstruct** that lost partition automatically using the lineage (the history of transformations) without needing to restart the entire job.
*   **Distributed:** The data in an RDD is divided into logical chunks called **partitions**, which are computed on different nodes of the cluster simultaneously.
*   **Dataset:** It is simply a collection of data objects (like a list of strings, integers, or custom objects).

## Why do we still talk about RDDs?

You might ask: *"If DataFrames are faster and easier, why learn RDDs?"*

1.  **Under the Hood:** DataFrames are actually built *on top* of RDDs. When you run a DataFrame query, Spark compiles it down to RDD code.
2.  **Unstructured Data:** RDDs are powerful when your data has no schema (e.g., raw text files, media, complex logs) and you need low-level control.
3.  **Debugging:** Understanding RDD partitions and lineage helps you debug performance issues like data skew and excessive shuffling in DataFrames.

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

## RDD Operations: A Quick Look

RDDs support the same **Transformations** and **Actions** we discussed in the previous post.

*   **Narrow Transformations:** `map`, `filter`, `flatMap` (No shuffle required).
*   **Wide Transformations:** `reduceByKey`, `groupByKey`, `distinct` (Requires shuffling data across partitions).

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
