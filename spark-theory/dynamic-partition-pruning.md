 **Dynamic Partition Pruning (DPP)** is a query-time optimization where Spark automatically prunes irrelevant partitions of a large fact table using filter results from a joined dimension table. Its works without you writing any extra code.

>**Pruning** simply means **cutting away the unnecessary parts** so you only work with what's relevant. The word comes from gardening: when you prune a tree, you cut off dead or extra branches so the tree grows better and stronger.
>
>In computing and data, the same idea applies: **skip or remove anything that won't contribute to the result**, so the system runs faster and does less work.

This is what separates DPP from static partition pruning. With static pruning, Spark skips partitions at *parse time* because the filter value is hardcoded in the query:

```python
# Static Pruning: Spark knows at parse time to read only the partitions where product_id in (p1, p2, p3)
orders_df.filter("product_id in ('p1','p2','p3')")
```

With DPP, the pruning condition isn't known until *runtime* because it's derived from another table's filtered result:

```python
# Dynamic Pruning: the partition filter comes from joining the dimension table
orders_df.join(products_df.filter("category = 'electronics'"), "product_id")

# In plain SQL, this is equivalent to:
# SELECT *
# FROM orders
# INNER JOIN products
#     ON orders.product_id = products.product_id
# WHERE products.category = 'electronics'

```

Spark can't know which `product_id` partitions to skip until it evaluates `products_df.filter(...)` at runtime. DPP solves exactly this.

***

### The Star Schema Problem DPP Solves

DPP was introduced in **Spark 3.0** and is designed specifically for **star schema** patterns: one large fact table joined with smaller dimension tables.

Without DPP:

```
fact_orders (partitioned by product_id) ← full scan, 10TB
     JOIN
products (filtered: category = 'Electronics') ← small, 10MB
```

Spark would scan the entire `fact_orders` table even though only some of the Electronics category partition is relevant.

With DPP, Spark automatically:
1. Scans and filters the small `products` table first (gets all `product_id` for category = 'Electronics')
2. Broadcasts the result as a hash table
3. **Reuses that same broadcast** as a `PartitionFilter` on `fact_orders` : scanning only the matching partition

***

### How DPP Works Internally

DPP is implemented through two rules inside the Spark SQL engine:

- **`PartitionPruning`** — a logical plan optimizer rule that detects when a join filter can be pushed down to prune the other side's partitions
- **`PlanDynamicPruningFilters`** — a physical planner rule that wires the broadcast result into the partition scan

The key insight: **Spark reuses the BroadcastExchange** that was already computed for the join. It doesn't scan the dimension table twice. The same broadcast hash table that eliminates shuffle *also* acts as the partition filter.

You can confirm DPP is firing using `EXPLAIN`:

```python
spark.table("fact_orders") \
    .join(spark.table("products").filter("category =  'Electronics'"), "product_id") \
    .explain()
```

Look for `dynamicpruningexpression(...)` inside `PartitionFilters` in the physical plan output — that's the DPP subquery at work.

```
FileScan parquet [product_id]
  PartitionFilters: [isnotnull(product_id),
    dynamicpruningexpression(product_id IN dynamicpruning#210)]
```

***

### The 3 Conditions for DPP to Fire

DPP is **not always triggered**. All three conditions must be true:

1. **The fact table must be partitioned** on the join key (`.partitionBy("col")`)
2. **The dimension table must be small enough to broadcast**: it either fits under `spark.sql.autoBroadcastJoinThreshold` (default 10MB), or you hint it explicitly with `broadcast()`
3. **`spark.sql.optimizer.dynamicPartitionPruning.enabled = true`** (default: `true` in Spark 3.0+)

If the dimension table is too large to broadcast, DPP won't fire because there's nothing to reuse.

> **The config `spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly`** (default: `true`) means DPP only activates when a `BroadcastExchange` can be reused. If you set it to `false`, Spark will build a separate subquery just for pruning which adds overhead and is rarely worth it.

***

### Practical Example

```python
# Write fact table partitioned by product_id
orders_df.write \
    .partitionBy("product_id") \
    .mode("overwrite") \
    .saveAsTable("orders")

# DPP fires here — no extra code needed
# orders is partitioned by product_id
result = spark.table("orders") \
    .join(
        spark.table("products").filter("category = 'electronics'"),
        "product_id"
    ) \
    .groupBy("category") \
    .count()

result.show()
result.explain()

# Step 1:  Scan products → apply filter category = 'electronics'
#          → Result: product_id IN {1, 2, 5, 7}

# Step 2:  Broadcast the result (tiny hash table, ~bytes)
#          → {1→Laptop, 2→Phone, 5→Headphones, 7→Tablet}

# Step 3:  Inject as PartitionFilter on orders scan
#          → Read ONLY partitions: product_id=1, 2, 5, 7
#          → Skip partitions:      product_id=3, 4, 6, 8
```

Even though you never wrote `filter("product_id in (1, 2, 5, 7)")` on the fact table, Spark injects it automatically.

***

### DPP vs Static Pruning

| | **Static Pruning** | **Dynamic Partition Pruning** |
|---|---|---|
| **When filter is known** | Parse time | Runtime |
| **Triggered by** | Hardcoded filter value | Join with dimension table |
| **Requires partitioned table** | Yes | Yes (fact table) |
| **Requires broadcast** | No | Yes (dimension table) |
| **Shuffle eliminated** | No | Partial (I/O pruning) |
| **Setup needed** | `partitionBy()` | `partitionBy()` + small dim table |
| **Works automatically** | Yes | Yes (Spark 3.0+) |

***

### How DPP Interacts with AQE

DPP and AQE are complementary but operate at different stages:

- **DPP fires before the fact table scan** — it reduces *how much data enters the pipeline*
- **AQE fires during execution** — it optimizes *how Spark processes that data* (coalescing partitions, handling skew, converting join strategies)

DPP feeds AQE better inputs. Less data scanned means AQE's runtime statistics are more accurate, leading to smarter decisions downstream.

***

### When DPP Doesn't Work

- **Fact table is not partitioned on the join key** — DPP has nothing to prune
- **Dimension table is too large to broadcast** — increase `spark.sql.autoBroadcastJoinThreshold` or use `broadcast()` hint explicitly
- **Join type is not supported** — DPP works with inner joins and certain outer joins; full outer joins don't qualify
- **Partition column type mismatch** — if fact table uses `IntegerType` and dimension uses `LongType`, the cast may prevent DPP from matching

Use `EXPLAIN` output and check for `dynamicpruningexpression` in `PartitionFilters` to verify DPP is actually being applied. If it's absent, check the above conditions one by one.

***

### Summary

- **DPP automatically prunes fact table partitions** using a joined dimension table's filtered values.
- It fires when: fact table is partitioned on the join key + dimension table is broadcastable + Spark 3.0+
- Internally it **reuses the BroadcastExchange** built for the join: the same hash table does double duty as both join executor and partition filter
- Use `EXPLAIN` and look for `dynamicpruningexpression` in `PartitionFilters` to confirm it's active
- Pair with AQE for a complete optimization stack: DPP reduces input data, AQE optimizes the execution on what remains