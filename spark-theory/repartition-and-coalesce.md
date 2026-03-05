`repartition()` and `coalesce()` are Spark's two tools for controlling the number of **in-memory partitions** in a DataFrame. Both reshape how your data is distributed across executors. But they work very differently and are helpful in different use cases.

Let's break them down.

***

### What is a Partition?

> **A partition** = a chunk of data that lives on one executor and is processed by one task.

When Spark loads a DataFrame, it splits it into partitions automatically. Each partition is processed by a single task in parallel. The number of partitions directly controls parallelism:

- **Too few partitions** → some executors sit idle, tasks take too long
- **Too many partitions** → scheduling overhead, too many tiny tasks, driver bottleneck

This is how you check the current partition count of any DataFrame:

```python
df = spark.read.parquet("s3://my-bucket/events/")
print(df.rdd.getNumPartitions())  # e.g., 200
```

***

### What is `repartition()`?

> **`repartition(n)`** = triggers a **full shuffle** to redistribute data evenly across exactly `n` partitions.

```python
df_repartitioned = df.repartition(100)
```

Spark hashes every row, sends it across the network to its target partition, and rebuilds the DataFrame from scratch with `n` evenly sized partitions. You can also repartition **by a column**, which co-locates rows with the same column value into the same partition — useful before joins or aggregations on that column.

```python
# Repartition by column — rows with same user_id go to same partition
df_repartitioned = df.repartition(50, "user_id")
```

**What happens internally:**

1. Spark computes `hash(row) % n` (or `hash(col_value) % n` if a column is specified)
2. Every row is shuffled across the network to its target partition
3. The result: `n` balanced partitions, each holding roughly equal data

This is a **wide transformation** so it always triggers a shuffle stage in the DAG.

***

### What is `coalesce()`?

> **`coalesce(n)`** = reduces partitions to `n` by **merging existing partitions** without a full shuffle.

```python
df_coalesced = df.coalesce(10)
```

Instead of shuffling all data, Spark simply combines adjacent partitions on the same executor. No data moves across the network. This makes it extremely cheap — but it only works in one direction: **reducing** partitions, not increasing them.

```python
# After a filter that reduces data significantly
filtered_df = df.filter("status = 'active'")   # now 200 small partitions
result = filtered_df.coalesce(20)               # merge into 20 larger ones
```

**What happens internally:**

Spark computes a minimal merge that combines nearby partitions. Some partitions are absorbed into neighboring ones without moving data across executors. Because partitions aren't re-hashed, the result can be **slightly uneven**. For most use cases that's acceptable.

> **Important constraint:** You cannot use `coalesce()` to *increase* the number of partitions. Calling `df.coalesce(500)` on a 200-partition DataFrame is a no-op. It stays at 200.

***

### How to Identify If Spark Partitions Are Too Many or Too Few
In Spark (including Databricks), the number of partitions should be based on dataset size, partition size, and cluster parallelism. A common production guideline is to keep partition sizes between 128 MB and 256 MB. This range balances efficient parallel processing and manageable memory usage.

Too many partitions → very small partitions, leading to high task scheduling overhead, JVM overhead, and the small files problem.
Too few partitions → very large partitions, causing memory pressure, long-running tasks, and poor parallelism.


### The Shuffle Difference, Visualized

**Before:** 8 partitions spread across 4 executors

```
Executor 1: [P1] [P2]
Executor 2: [P3] [P4]
Executor 3: [P5] [P6]
Executor 4: [P7] [P8]
```

**After `coalesce(4)`** — partitions merged locally, no network movement:

```
Executor 1: [P1 + P2]
Executor 2: [P3 + P4]
Executor 3: [P5 + P6]
Executor 4: [P7 + P8]
```

**After `repartition(4)`** — full shuffle, all data rehashed and redistributed:

```
Executor 1: [new P1]  ← rows from old P1, P3, P5, P7
Executor 2: [new P2]  ← rows from old P2, P4, P6, P8
Executor 3: [new P3]  ← rows from old P1, P3, P5, P7
Executor 4: [new P4]  ← rows from old P2, P4, P6, P8
```

***

### `repartition()` vs `coalesce()`

| | **`repartition(n)`** | **`coalesce(n)`** |
|---|---|---|
| **Shuffle** | Full shuffle (wide transformation) | Minimal / no shuffle |
| **Direction** | Increase or decrease partitions | Decrease only |
| **Data distribution** | Perfectly even | Can be slightly uneven |
| **Performance cost** | High (network I/O) | Low (local merge) |
| **Column-based routing** | Yes, using `.repartition(n, "col")` | No |
| **Use after filter?** | Overkill: use `coalesce` | Yes, ideal |
| **Use before join?** | Yes: ensures even distribution | Not recommended |

***

### The Problem Each One Solves

**`coalesce()` solves the small file problem after filtering:**

You start with 500 partitions. After a heavy filter, 480 of them are nearly empty. Writing these produces 500 tiny files — metadata overhead kills your next read.

```python
# 500 partitions → most are tiny after filter
df_filtered = large_df.filter("event_type = 'purchase'")

# Merge into 20 reasonable partitions before writing
df_filtered.coalesce(20).write.parquet("s3://output/purchases/")
```

**`repartition()` solves skewed or under-parallelized DataFrames:**

After a narrow transformation or reading a small number of files, you end up with too few partitions which means not enough parallelism to use your cluster.

```python
# Read 4 files → 4 partitions, but cluster has 200 cores
df = spark.read.parquet("s3://input/")
print(df.rdd.getNumPartitions())  # 4 — terrible parallelism

# Force even distribution across 200 partitions
df.repartition(200).write.parquet("s3://output/")
```

***

### How Data Skew Interacts

`coalesce()` can **worsen skew** because it merges adjacent partitions without rebalancing. If P1 has 10M rows and P2 has 50K rows, merging them into one partition still leaves 10M rows in one task.

`repartition()` eliminates skew because hashing redistributes rows evenly regardless of their original location.

```python
from pyspark.sql.functions import spark_partition_id, count, col

df.groupBy(spark_partition_id().alias("partition")) \
  .count() \
  .orderBy(col("count").desc()) \
  .show(10)

# +---------+------+
# |partition| count|
# +---------+------+
# |        3| 85000|
# |        1| 82000|
# |        7|  1200|   <-- skewed (nearly empty)
# |        5|   900|
# +---------+------+
```

If you see one partition with 10x the rows of others, use `repartition()` — not `coalesce()`.

***

### When to Use What

**Use `coalesce()` when:**
- You want to **reduce** partitions after a filter or narrow transformation
- You're writing output and want fewer files without paying shuffle cost
- Your data is already reasonably balanced across partitions
- You're at the **end of a pipeline** — no more wide transformations after

**Use `repartition()` when:**
- You need to **increase** partitions (only option)
- Your data is **skewed** and needs rebalancing
- You're about to run a **join or group-by** and want co-located, even distribution
- You want to route rows by column using `.repartition(n, "col")`

**Common mistake:** Using `repartition()` right before writing output "just to be safe." If your data is already balanced, you're paying full shuffle cost for no benefit. Use `coalesce()` instead.


***

### Practical Example: End-to-End Pipeline

```python
# Step 1: Read — many small files → too many partitions
raw_df = spark.read.parquet("s3://raw/events/")
print(raw_df.rdd.getNumPartitions())  # 800 — too many small files

# Step 2: Heavy filter — now most partitions are tiny
filtered_df = raw_df.filter("event_date = '2024-01-15'")

# Step 3: repartition by join key — guarantees even + co-located for the join
prepped_df = filtered_df.repartition(50, "user_id")

# Step 4: Join — benefits from column-based repartition above
result = prepped_df.join(users_df, "user_id")

# Step 5: coalesce before write — reduce files without another shuffle
result.coalesce(20).write.parquet("s3://output/processed/")
```

***

### How It Interacts with AQE

Spark's **Adaptive Query Execution (AQE)** can automatically coalesce shuffle partitions at runtime using `spark.sql.adaptive.coalescePartitions.enabled = true`. This means AQE does its own version of `coalesce()` after each shuffle stage by merging small post-shuffle partitions automatically.

But AQE only acts on **shuffle boundaries**. If you have too many partitions from a file read (pre-shuffle), AQE won't help, you need explicit `repartition()` or `coalesce()`.

Think of AQE as a **reactive safety net** for shuffle output. Explicit `repartition()` and `coalesce()` are your **proactive controls** for partition count before and after transformations.

### Summary

- **`coalesce(n)`** merges partitions locally — cheap, no shuffle, only reduces. Use it before writes or after filters.
- **`repartition(n)`** reshuffles everything — expensive, but perfectly balanced, and the only way to increase partitions or route by column.
- **Never use `repartition()` when `coalesce()` is sufficient** — you're paying shuffle tax for nothing.
- **AQE complements both** — it handles post-shuffle coalescing reactively, but explicit control is still needed for pre-shuffle partition management.