Partitioning and Bucketing are two of Spark's most effective storage optimizations. Both cut down unnecessary data reads. Both speed up queries. But they work in completely different ways and mixing them up can hurt more than help.

Let's break them down.

***

### What is Partitioning?

> **Partitioning** = physically splitting data into separate folders on disk based on column values, so Spark can skip entire directories during reads.

When you write a DataFrame using `.partitionBy()`, Spark creates a **directory per unique value** in the partition column.

```python
df.write \
  .partitionBy("country", "year") \
  .parquet("s3://my-bucket/sales/")
```

This produces a folder structure like:

```
sales/
├── country=IN/
│   ├── year=2023/
│   └── year=2024/
├── country=US/
│   ├── year=2023/
│   └── year=2024/
```

When a query filters on `country = 'IN'`, Spark reads **only** the `country=IN/` directory — completely skipping the rest. This is called **partition pruning**.

***

### How Partitioning Works Internally

Spark uses the `.partitionBy()` method of the `DataFrameWriter` class.  Each partition folder can hold multiple Parquet/ORC files.

The number of files written inside each partition folder equals the **number of in-memory partitions (tasks) that contain data for that partition value** at the time of writing.

So if your DataFrame has 200 in-memory partitions and 5 of them contain rows where `country = 'IN'`, you get **5 files** inside the `country=IN/` folder - one file per task that wrote to it.

**The Problem it Solves:**

You have 10TB of sales data and your analysts always filter by `region`. Without partitioning, every query scans the full 10TB. With partitioning by `region`, a query for `region = 'APAC'` reads only ~2TB — an 80% I/O reduction.

***

### The Small File Problem

> **Watch out:** partitioning on a **high-cardinality column** (like `user_id` or `timestamp`) creates thousands of tiny folders — each with tiny files. This destroys performance.

**Rule of thumb:** Partition on columns with **low-to-medium cardinality** — `date`, `country`, `region`, `status`.

> **What is cardinality?** Cardinality is simply the number of distinct values in a column. A `country` column with 50 unique values is **low cardinality**. A `user_id` column with 10 million unique values is **high cardinality**. For partitioning, low cardinality is good because it creates a manageable number of folders. High cardinality creates thousands of tiny folders, which kills performance.

If a partition column has 10,000 unique values, you end up with 10,000 folders. Reading metadata for that many directories kills the NameNode (HDFS) or S3 LIST operations. Stick to columns where the number of distinct values is in the **tens to low hundreds**.

***

### What is Bucketing?

> **Bucketing** = distributing rows into a **fixed number of hash-based files** (buckets) based on a column, so Spark knows which rows are co-located — eliminating shuffle during joins and aggregations.

```python
df.write \
  .bucketBy(8, "user_id") \
  .sortBy("user_id") \
  .saveAsTable("orders_bucketed")
```

Spark applies `hash(user_id) % 8` to each row, placing it in one of 8 bucket files.  When two tables are bucketed on the **same column with the same number of buckets**, a join between them requires **zero shuffle** — Spark knows that matching `user_id` rows are already in the same bucket files on both sides.

**Important constraint:** Bucketing only works with **Hive/Spark managed tables** (`.saveAsTable()`), not with raw file writes.

***

### How Bucketing Eliminates Shuffle

**The Problem Without Bucketing:**

```python
orders_df.join(users_df, "user_id")
# Spark must shuffle BOTH tables by user_id → expensive network I/O
```

**With Bucketing (both tables bucketed by `user_id` into same N buckets):**

```python
# orders and users both bucketed by user_id into 8 buckets
orders_df.join(users_df, "user_id")
# Spark reads bucket 1 of orders → joins with bucket 1 of users
# No shuffle. No network. Just local reads.
```

Each executor reads its corresponding bucket files from both tables and joins them locally.  This can improve join performance by **10x or more** on large datasets.

***

### Combining Partitioning + Bucketing

The real power comes from **using both together**:

```python
df.write \
  .partitionBy("country") \          # coarse-grained: folder per country
  .bucketBy(16, "user_id") \         # fine-grained: hash rows within each country
  .sortBy("user_id") \
  .saveAsTable("events_optimized")
```

- **Partitioning** handles range-based filters — skip entire `country=IN/` folders
- **Bucketing** handles join efficiency — rows with the same `user_id` are in the same bucket file across all tables

This pattern is widely used in Lakehouse architectures for large fact tables.

***

### Partitioning vs Bucketing

| | **Partitioning** | **Bucketing** |
|---|---|---|
| **API** | `.partitionBy("col")` | `.bucketBy(N, "col")` |
| **Storage** | Separate folder per value | Fixed N files via hash |
| **Column type** | Low-cardinality (date, region) | High-cardinality (user_id, order_id) |
| **Best for** | Filter / range queries | Joins & aggregations |
| **Shuffle elimination** | Partial (partition pruning) | Full (co-located data) |
| **Flexibility** | Dynamic (any number of values) | Fixed N buckets upfront |
| **Requires table?** | No (works with file writes) | Yes (Hive/Spark managed table) |
| **Pitfall** | Too many small files if high-cardinality | Wrong N buckets → uneven distribution |

***

### Choosing the Right N for Buckets

**The Problem:** If you choose too few buckets, each bucket file is huge which defeats the purpose. Too many, and you're back to the small file problem.

**Rule of thumb:**
- Target **128MB–256MB per bucket file**
- Formula: `N = total_data_size / target_bucket_size`
- For a 100GB table targeting 200MB per bucket: `N = 100GB / 200MB = 500 buckets`

Both joined tables must use the **exact same N** for shuffle elimination to kick in. If table A has 16 buckets and table B has 32, Spark will still shuffle.

***

### When to Use What

**Use Partitioning when:**
- Queries frequently filter on the column (`WHERE date = '2024-01-01'`)
- The column has low cardinality (< ~500 distinct values)
- You're optimizing **read performance** and I/O costs

**Use Bucketing when:**
- You repeatedly **join large tables** on the same key
- You run frequent **group-by aggregations** on the same column
- The join key has **high cardinality** (user IDs, order IDs)
- You want to eliminate sort-merge join shuffle entirely

**Use Both when:**
- You have a large fact table joined frequently by `user_id` AND filtered by `date`
- Classic pattern: `partitionBy("date").bucketBy(N, "user_id")`

***

### Practical Example: E-commerce Orders

```python
# Write orders table — partitioned by date, bucketed by user_id
orders_df.write \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .bucketBy(32, "user_id") \
    .sortBy("user_id") \
    .saveAsTable("orders")

# Write users table — bucketed by the same column + same N
users_df.write \
    .mode("overwrite") \
    .bucketBy(32, "user_id") \
    .sortBy("user_id") \
    .saveAsTable("users")

# This join now:
# 1. Prunes all partitions except order_date = '2024-01-15' (partition pruning)
# 2. Joins orders + users with ZERO shuffle (bucket co-location)
result = spark.table("orders") \
    .filter("order_date = '2024-01-15'") \
    .join(spark.table("users"), "user_id")
```

***

### How It Interacts with AQE

Bucketing and AQE work together — but with a nuance:

- If two tables are **properly bucketed** (same column, same N), AQE's sort-merge → broadcast join conversion is **skipped**, since Spark already knows no shuffle is needed.
- If bucketing is **mismatched** (different N), AQE still kicks in and tries to optimize the resulting sort-merge join dynamically.
- Partition pruning from `.partitionBy()` reduces the data AQE has to work with — **less data = better AQE statistics = smarter decisions**.

Think of partitioning and bucketing as **proactive optimization** (you set it up upfront), while AQE is **reactive optimization** (Spark adjusts at runtime). The best pipelines use both.

***

### Summary

- **Partition** on low-cardinality columns for fast filtering. Avoid high-cardinality partition columns — they cause small file problems.
- **Bucket** on high-cardinality join keys to eliminate shuffle between large tables. Both tables must use the same column and same N.
- **Combine both** for large fact tables: `partitionBy("date").bucketBy(N, "user_id")`.
- Bucketing requires Spark/Hive managed tables — it doesn't work with raw `.parquet()` writes.