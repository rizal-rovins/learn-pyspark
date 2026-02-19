A join combines rows from two tables based on a matching condition (e.g. `employee.dept_id = department.id`).  Think of it as a lookup - for each row on the left, find the related row(s) on the right and stitch them together.

### Join Types

These define **what rows you want back**:

| Join Type | What You Get |
|---|---|
| **Inner** | Only rows with a match in *both* tables  |
| **Left Outer** | All rows from left; NULLs where no right match  |
| **Right Outer** | All rows from right; NULLs where no left match  |
| **Full Outer** | All rows from both; NULLs on the unmatched side  |
| **Cross** | Every combination of left × right (Cartesian product)  |
| **Left Semi** | Only left rows that *have* a match on the right (no right columns returned)  |
| **Left Anti** | Only left rows that *have no* match on the right - useful for finding records in one table which are not present in the other  |

Spark offers multiple join strategies to handle different data scenarios. Understanding when and why Spark picks each strategy is critical for optimizing join performance - the wrong choice can turn a 5-minute query into a 2-hour nightmare.

### Why Join Strategy Matters

A join between two DataFrames can execute in vastly different ways depending on data sizes, available memory, and join keys. Spark's optimizer automatically selects a strategy, but you can override it using **join hints** when you know your data better than the optimizer does.

The four main join strategies are:

### 1. Broadcast Hash Join (BHJ)

**How It Works:** Spark takes the smaller table, broadcasts (copies) it to **every executor** in the cluster, then builds an in-memory hash table on each executor. The larger table stays partitioned, and each executor performs a local hash lookup to find matches.

**When Spark Uses It:**
- One side is smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10MB)
- You explicitly hint it with `BROADCAST`

**Configuration:**
```python
# Automatically broadcast tables < 10MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# Use hint to force broadcast
small_df.join(large_df.hint("BROADCAST"), "user_id")
# Or use programmatic API
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "user_id")
```

**Advantages:**
- **No shuffle required** - the larger table stays in place
- **Extremely fast** - local hash lookups are O(1)
- Single-stage execution (no stage boundary)

**Disadvantages:**
- **Memory risk** - if the broadcast table is too large, executors run out of memory and the job fails
- **Driver bottleneck** - the driver collects and distributes the small table

**Best For:** Joining a large fact table with small dimension tables (e.g., transactions × customers where customers < 10MB).

**Aliases:** `BROADCASTJOIN`, `MAPJOIN`

***

### 2. Shuffle Hash Join (SHJ)

**How It Works:** Both tables are **shuffled** based on the join key, so rows with the same key land on the same partition. Spark then builds an in-memory hash table from the **smaller side** of each partition and probes it with rows from the larger side.

**When Spark Uses It:**
- You explicitly hint it with `SHUFFLE_HASH`
- One side is much smaller than the other (but too large to broadcast)
- `spark.sql.join.preferSortMergeJoin` is set to `false`
- OR with AQE: all post-shuffle partitions are small enough (< `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`)

**Configuration:**
```python
# Hint for shuffle hash join
df1.join(df2.hint("SHUFFLE_HASH"), "user_id")

# Prefer shuffle hash over sort-merge globally
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

# With AQE, convert to shuffle hash if partitions are small
spark.conf.set("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "64MB")
```

**Advantages:**
- **No sorting required** - faster than sort-merge join when partitions fit in memory
- Hash lookups are faster than merge scans

**Disadvantages:**
- **Requires shuffle** - network I/O and disk spill overhead
- **Memory pressure** - needs to build hash table per partition; if partitions are too large, it causes OOM
- **Vulnerable to data skew** - uneven key distribution creates huge partitions that don't fit in memory

**Best For:** Medium-sized tables where one side is notably smaller and partitions fit comfortably in memory.

***

### 3. Sort-Merge Join (SMJ)

**How It Works:** Both tables are **shuffled** by join key and then **sorted** within each partition. Spark then performs a merge scan (like merging two sorted arrays) to find matching rows.

**When Spark Uses It:**
- **Default strategy** for large-to-large table joins when broadcast isn't possible
- Join keys are sortable
- `spark.sql.join.preferSortMergeJoin` is `true` (default)

**Configuration:**
```python
# Hint for sort-merge join
df1.join(df2.hint("MERGE"), "user_id")
# Aliases: SHUFFLE_MERGE, MERGEJOIN

# Prefer sort-merge join (default behavior)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
```

**Advantages:**
- **Memory efficient** - doesn't need to build hash tables; sorts and scans sequentially
- **Handles large datasets** - works even when partitions don't fit in memory (spills to disk gracefully)
- **Skew-friendly with AQE** - AQE can split skewed partitions during sort-merge joins

**Disadvantages:**
- **Sorting overhead** - expensive CPU operation, especially on large partitions
- **Requires shuffle** - both sides must be redistributed across the network
- Slower than shuffle hash join when partitions fit in memory

**Best For:** Large table-to-large table joins, especially when neither side is small enough to broadcast or hash.

***

### 4. Shuffle-and-Replicate Nested Loop Join (Cartesian Join)

**How It Works:** For **non-equi joins** (joins without equality conditions like `df1.col > df2.col`) or **cross joins**, Spark uses nested loops. Each row from one side is compared to every row from the other side.

>In this join algorithm, *shuffle* doesn't refer to a true shuffle because records with the same keys aren't sent to the same partition. Instead, the entire partition from both datasets are copied over the network. When the partitions from the datasets are available, a Nested Loop join is performed. If there are `X` number of records in the first dataset and `Y` number of records in the second dataset in each partition, each record in the second dataset is joined with every record in the first dataset. This continues in a loop `X × Y` times in every partition.

**When Spark Uses It:**
- Non-equi join conditions (`!=`, `>`, `<`, etc.)
- Cross joins (no join condition)
- Fallback when no other strategy applies

**Configuration:**
```python
# Hint for nested loop join
df1.join(df2.hint("SHUFFLE_REPLICATE_NL"), df1.id > df2.id)
```

**Advantages:**
- **Only option** for non-equi joins

**Disadvantages:**
- **Extremely expensive** - O(N × M) complexity
- **High risk of OOM** - can produce massive result sets

**Best For:** Avoid unless absolutely necessary. If you need a cross join, ensure both sides are small.

***

### Join Strategy Priority

When you specify multiple hints, Spark follows this priority order:

1. **BROADCAST** (highest priority)
2. **MERGE**
3. **SHUFFLE_HASH**
4. **SHUFFLE_REPLICATE_NL** (lowest priority)

**Example:**
```python
# BROADCAST hint takes precedence over MERGE
df1.join(df2.hint("BROADCAST").hint("MERGE"), "user_id")  # Uses broadcast join
```

If both sides have the same hint (e.g., both have `BROADCAST`), Spark picks the smaller side based on statistics.

***

### Join Strategy Comparison

| Strategy | Shuffle Required | Memory Usage | Best For | Risk |
|---|---|---|---|---|
| **Broadcast Hash** | No | High (broadcast to all executors) | Small × Large tables | OOM if broadcast table too large |
| **Shuffle Hash** | Yes | Medium (hash table per partition) | Medium tables, one side smaller | OOM if partitions too large, data skew |
| **Sort-Merge** | Yes | Low (sequential scan) | Large × Large tables | Sorting overhead |
| **Nested Loop** | Yes | Very High | Non-equi joins only | Cartesian explosion |

***

### Practical Decision Tree

```
Is one table < 10MB?
├─ YES → Broadcast Hash Join
└─ NO → Are both tables large (> 1GB)?
    ├─ YES → Sort-Merge Join (default, memory-safe)
    └─ NO → Is one side much smaller AND partitions fit in memory?
        ├─ YES → Shuffle Hash Join (hint it if needed)
        └─ NO → Sort-Merge Join
```

***

### Real-World Example

```python
# Scenario: 10GB transactions table joining 50MB users table
transactions = spark.read.parquet("transactions")  # 10GB
users = spark.read.parquet("users")  # 50MB

# Bad: Uses sort-merge join (shuffle both sides, sort both sides)
result = transactions.join(users, "user_id")

# Good: Broadcast the smaller table (no shuffle, no sort)
from pyspark.sql.functions import broadcast
result = transactions.join(broadcast(users), "user_id")

# Scenario: 5GB sales × 8GB products (both large)
sales = spark.read.parquet("sales")  # 5GB
products = spark.read.parquet("products")  # 8GB

# Default sort-merge join is correct here
result = sales.join(products, "product_id")

# If partitions are small after shuffle, force shuffle hash for speed
result = sales.join(products.hint("SHUFFLE_HASH"), "product_id")
```

***

### Configuration Summary

| Configuration | Default | Impact |
|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Tables smaller than this are broadcast  |
| `spark.sql.join.preferSortMergeJoin` | `true` | Prefer sort-merge over shuffle hash  |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | `0` (disabled) | Convert SMJ → SHJ if partitions < threshold  |

***

### Tips for Optimization

1. **Profile your joins:** Check the Spark UI SQL tab to see which strategy was chosen and why
2. **Use statistics:** Run `ANALYZE TABLE` to help Spark make better decisions
3. **Test hints:** If the default strategy is slow, try hinting a different strategy and compare execution times
4. **Watch for data skew:** If one key has 10x more rows, sort-merge join with AQE is your best bet
5. **Increase broadcast threshold cautiously:** Raising `autoBroadcastJoinThreshold` can speed up joins, but risk OOM errors

***

### The Bottom Line

Spark's join strategy selection is usually smart, but not perfect. When joining small dimension tables with large fact tables, **always consider broadcast hints**. For large-to-large joins, trust the default sort-merge join unless you have evidence that shuffle hash would be faster. And remember: **AQE can dynamically convert sort-merge to broadcast joins at runtime** if it detects a small table after filtering.

**Key takeaway:** The right join strategy can make your query 10x faster - profile, measure, and hint strategically.

***