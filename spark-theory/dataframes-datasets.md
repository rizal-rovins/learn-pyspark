DataFrames (and Datasets in Scala/Java) are Spark’s higher-level, schema-aware abstractions that make ETL code simpler and enable powerful query optimizations that plain RDD code can’t easily get. 

They’re the default choice for most modern batch + streaming pipelines because Spark can optimize the whole plan before running it (thanks to lazy evaluation).

## What is a DataFrame?

A **DataFrame** is a distributed table-like collection of rows organized into named columns with a known schema (like a table in a warehouse, but computed in parallel).  
In PySpark, when most people say “Spark”, they usually mean building pipelines with `spark.read...`, `select`, `filter`, `join`, `groupBy`, and `write` on DataFrames.

DataFrames are an **abstraction layer** sitting on top of RDDs. They add a schema (structure) to the data, which allows Spark to understand *what* is in your data (columns and types) rather than just treating it as a "blob" of objects like RDDs do. So, while you write DataFrames, **RDDs are still the engine** doing the actual heavy lifting underneath. You just don't have to manage them manually anymore.

## DataFrame vs Dataset

A **Dataset** (mainly in Scala/Java) is a typed API built on top of the same underlying engine as DataFrames, giving compile-time type safety for rows/objects while keeping SQL-style optimizations.  
In PySpark, “Dataset” isn’t a separate everyday API - DataFrames are the standard interface for structured processing.

| Concept | Best for | What you get |
|---|---|---|
| RDD | Low-level control, unstructured/complex custom logic | Manual control, less automatic optimization |
| DataFrame | 95% of DE ETL use cases | Schema + SQL-like ops + optimizer |
| Dataset (Scala/Java) | Typed pipelines + optimizer | Types + DataFrame performance model |

## Why DataFrames are fast

DataFrames let Spark treat your pipeline like a **query**, so it can reorder and combine operations before execution (instead of running each line immediately).
They also enable optimizations like pushing filters and column selection down to the data source, so Spark reads fewer rows and fewer columns from storage.
This is why writing `df.filter(...).select(...)` feels instant until an action triggers the actual work.

## Common DataFrame operations (DE-focused)

Transformations (build the plan):
- `select`, `selectExpr` (projection)
- `filter` / `where` (row filtering)
- `withColumn` (derive columns)
- `join` (combine facts/dimensions)
- `groupBy().agg(...)` (aggregations)
- `dropDuplicates`, `distinct` (dedup)

Actions (trigger execution):
- `count`, `show`, `collect` (be careful with `collect`)
- `write.format(...).mode(...).save(...)` (common in pipelines)
- `foreachBatch` (streaming)

## Example: Typical ETL with DataFrames

```python
from pyspark.sql import functions as F

# Read (lazy)
orders = spark.read.parquet("s3://lake/raw/orders/")
customers = spark.read.parquet("s3://lake/raw/customers/")

# Transform (still lazy)
orders_2025 = (
    orders
      .filter(F.col("order_date") >= F.lit("2025-01-01"))
      .select("order_id", "customer_id", "order_date", "amount", "status")
      .withColumn("is_completed", F.col("status") == F.lit("COMPLETED"))
)

enriched = (
    orders_2025
      .join(customers.select("customer_id", "segment"), on="customer_id", how="left")
)

daily_metrics = (
    enriched
      .groupBy(F.to_date("order_date").alias("day"), "segment")
      .agg(
          F.sum("amount").alias("revenue"),
          F.countDistinct("order_id").alias("orders"),
          F.sum(F.col("is_completed").cast("int")).alias("completed_orders")
      )
)

# Action: triggers the full DAG + optimizations
daily_metrics.write.mode("overwrite").parquet("s3://lake/gold/daily_segment_metrics/")
```

Because Spark can “see” the full chain before running, it can apply optimizations such as predicate pushdown (filter early at the source) and projection pushdown (read only required columns) when the source/format supports it.