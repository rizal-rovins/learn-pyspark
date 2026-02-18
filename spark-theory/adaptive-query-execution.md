Adaptive Query Execution lets Spark re-optimize your query **while it's running** based on what it actually sees in your data, not just pre-execution guesses. Enabled by default since Spark 3.2.0, AQE adjusts query plans on the fly using real runtime statistics.

## Why AQE Matters

Traditional query optimizers make all decisions **before** execution starts, relying on statistics that can be outdated, missing, or just plain wrong. AQE changes this behaviour: it observes what's actually happening during execution and adjusts the plan dynamically.

*Think of it like GPS navigation that reroutes you based on live traffic, rather than sticking to a route planned hours ago.*

## The Five Core Optimizations

AQE delivers five major features that address the most common Spark performance bottlenecks:

### 1. Coalescing Post-Shuffle Partitions

> **Coalesce** = reduce the number of partitions by merging neighboring ones in-place, with zero data movement across the network.

**The Problem:** You have `spark.sql.shuffle.partitions = 200` configured globally. You run a `groupBy("country")` aggregation on your dataset with 50 unique countries. After the shuffle completes, you end up with 200 partitions where only 50 contain data - the remaining 150 are completely empty. Even worse, each of those 50 partitions holding data is only ~2MB in size. Now 200 tasks are scheduled, but 150 do nothing and the other 50 finish in milliseconds, wasting scheduler resources.

**How AQE solves this:** After the shuffle completes, AQE examines the actual partition sizes and **merges small contiguous partitions together** to hit a target size (default 64MB via `spark.sql.adaptive.advisoryPartitionSizeInBytes`). In this case, AQE would coalesce those 50 small partitions (2MB each = 100MB total) into ~2 optimally-sized partitions of ~50MB each.

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")  # default
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")  # target size
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")  # minimum size
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "true")  # prioritize parallelism
```

>**Note:** When `parallelismFirst` is true (default), Spark prioritizes maximizing parallelism over hitting the target size, only respecting the minimum 1MB threshold. Set this to false on busy clusters to improve resource utilization.

**The result:** Instead of 200 small partitions, you get 5-10 partitions of reasonable size. Fewer tasks = less scheduling overhead = faster execution.

***

### 2. Splitting Skewed Shuffle Partitions (in RebalancePartitions)

**The Problem:** Beyond joins, data skew can occur during `REBALANCE` operations where some partitions end up much larger than others.

> **What is REBALANCE?** The `REBALANCE` hint is used to rebalance query result output partitions so that every partition is a reasonable size (not too small, not too big). It's particularly useful when writing query results to storage to avoid too many small files or too few large files. Example: `SELECT /*+ REBALANCE(country) */ * FROM sales`. This hint only works when AQE is enabled.

**AQE's Solution:** When rebalancing partitions, AQE detects skewed partitions and **splits them into smaller chunks** according to the advisory partition size. Small partitions below a threshold (20% of advisory size by default) get merged.

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled", "true")  # default
spark.conf.set("spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor", "0.2")  # merge if < 20% of advisory size
```

**When It Helps:** Queries using `REBALANCE` hints or operations that trigger partition rebalancing.

***

### 3. Converting Sort-Merge Join to Broadcast Join

**The Problem:** Your query optimizer estimated table B is 1024MB (so it chose sort-merge join), but at runtime it's actually only 8MB. A broadcast join would've been 10x faster.

**AQE's Solution:** After the shuffle reads complete, AQE re-examines the actual data sizes. If either join side is small enough (under `spark.sql.adaptive.autoBroadcastJoinThreshold`), AQE **converts the sort-merge join to a broadcast hash join mid-execution**.

**Configuration:**
```python
# Uses same threshold as spark.sql.autoBroadcastJoinThreshold unless overridden
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB")  # or set explicitly
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")  # enables local shuffle reads
```

**Performance Win:** With `localShuffleReader` enabled, Spark avoids sorting both join sides and reads shuffle files locally to save network traffic.

***

### 4. Converting Sort-Merge Join to Shuffled Hash Join

**The Problem:** Sort-merge join includes expensive sorting operations on both sides. If all post-shuffle partitions are small enough to fit in memory, a hash join would be faster.

**AQE's Solution:** When all partitions after shuffle are smaller than a configured threshold, AQE **converts sort-merge join to shuffled hash join**, eliminating the sort overhead.

**Configuration:**
```python
# 0 by default (disabled) - set to non-zero to enable
spark.conf.set("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "64MB")
# If this is >= advisoryPartitionSizeInBytes AND all partitions < this value,
# Spark prefers shuffled hash join over sort-merge join
```

**When It Helps:** Queries with joins where post-shuffle partitions are uniformly small and can fit in executor memory.

***

### 5. Optimizing Skew Join (Splitting Skewed Join Partitions)

**The Problem:** In a `groupBy("user_id")` or join operation, one celebrity user has 10 million records while everyone else has 100. That one massive partition causes a single executor to grind for hours while others sit idle (the dreaded straggler task).

**AQE's Solution:** AQE detects skewed partitions during sort-merge joins by identifying partitions that are **5x larger than the median size AND above 256MB**. It then **splits** these skewed partitions into smaller sub-partitions (and replicates the other side if needed) that can be processed in parallel.

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")  # default
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5.0")  # must be 5x median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")  # must be > 256MB
spark.conf.set("spark.sql.adaptive.forceOptimizeSkewedJoin", "false")  # force optimization even with extra shuffle
```

**Example:** If partition 47 contains 2GB of data (for user_id = 'celebrity123') and the median partition size is 200MB, AQE splits it into 8 sub-partitions of ~250MB each, distributing the load across multiple executors.

**Advanced:** Set `forceOptimizeSkewedJoin = true` to enable skew optimization even when it introduces extra shuffle.

***

## How AQE Works Under the Hood

1. **Query Plan Created:** Spark builds the initial execution plan using catalog statistics or educated guesses
2. **Stage Execution:** Spark starts executing stages and collects **runtime statistics** (actual row counts, data sizes, partition distributions)
3. **Re-Optimization:** At stage boundaries (after shuffles), AQE triggers re-planning using the fresh runtime stats
4. **Plan Adjustment:** The remainder of the query runs with an optimized plan based on reality, not estimates

You can see AQE in action in the Spark UI under the SQL tab - look for `Statistics(..., isRuntime=true)` in the plan details.

***

## Scenarios where AQE is beneficial:

1. **Wide transformations with unpredictable outputs:** Filters that drastically reduce data size, joins with unknown cardinality
2. **Data skew scenarios:** Aggregations or joins on non-uniform keys (city, product_id, user_id)
3. **Missing/stale statistics:** When `ANALYZE TABLE` hasn't been run or tables are frequently updated
4. **Queries with multiple stages:** More stage boundaries = more opportunities for AQE to re-optimize
5. **Dynamic workloads:** Ad-hoc queries where you can't pre-tune partition counts

***

## Scenarios where AQE is NOT beneficial:

1. **Narrow transformations only:** If your query is just `select().filter()`, there's nothing to re-optimize
2. **Single-stage queries:** No shuffle = no runtime statistics to leverage
3. **Perfectly tuned queries:** If you've already set optimal partition counts and join strategies
4. **Tiny datasets:** AQE overhead might outweigh benefits for extremely small data

***

## Practical Example

```python
# Scenario: Join a large table (10GB) with a small table (estimated 50MB, actually 8MB)
large_df = spark.read.parquet("transactions")  # 10GB
small_df = spark.read.parquet("users").filter("country = 'IN'")  # Initially 50MB, filtered to 8MB

result = large_df.join(small_df, "user_id")

# Without AQE:
# - Optimizer chooses sort-merge join (50MB > 10MB broadcast threshold)
# - Both sides shuffled and sorted
# - Expensive network I/O

# With AQE (enabled by default):
# 1. Sort-merge join starts executing
# 2. After shuffle, AQE sees small_df is actually only 8MB
# 3. Converts to broadcast join mid-execution
# 4. Saves sorting overhead and network traffic via local shuffle reader
# Result: 3-5x faster execution
```

***

## The Bottom Line

AQE turns Spark into a dynamic optimizer that adjusts your query based on what it actually sees in your data at runtime. You no longer need to:
- Perfectly tune `spark.sql.shuffle.partitions` for every query - AQE coalesces dynamically
- Manually handle data skew - AQE splits skewed partitions automatically
- Second-guess join strategies - AQE converts to optimal joins at runtime

For most workloads, just keep AQE enabled (it's on by default) and let Spark do the heavy lifting. For advanced scenarios with extreme data skew or specialized requirements, tweak the threshold configs to match your data patterns or provide custom cost evaluators.

***