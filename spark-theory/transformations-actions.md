Transformations and actions are the two building blocks of every Spark job: transformations *define* what should happen to data, and actions *trigger* execution to produce a result or write output. This split is what enables Spark’s lazy evaluation and efficient optimization via its execution plan (DAG).

## The core idea
A **transformation** creates a new RDD/DataFrame from an existing one (it describes a step in your pipeline - like a select, filter, join etc) and is evaluated lazily. An **action** asks Spark to materialize a result (return to the driver, write to storage, or otherwise “finish” the computation), which is what triggers a job in Spark’s execution model.

## Transformations (lazy building blocks)
Transformations are “recipe steps” that Spark records in the lineage/DAG rather than executing immediately, allowing Spark to optimize the plan before running it. Common transformation examples include `select`, `filter`, `withColumn`, `groupBy`, `join`, `distinct`, `repartition`, and `union`.

Two useful sub-types matter for performance:
- **Narrow transformations**: Each output partition depends on a single input partition (typically no shuffle), e.g., `filter`, `map`, `select`.
- **Wide transformations**: Output partitions depend on many input partitions (often causes shuffle), e.g., `groupBy`, `join`, `distinct`, `repartition`. For example - when you group by `customer_id` and need to sum up the `amount`, all the rows of same `customer_id` (which may be present in different partitions across the executors) needs to be brought to the same partition to accurately calculate the total sum for that `customer_id`.

![Narrow v/s Wide Transformation](/images/narrow-wide-transformations.png)

### Performance Implications of Transformations

**1. Narrow Transformations: Pipelining (High Efficiency)**
*   **No Data Movement:** Since the data required for the computation resides on the same partition, there is no need to transfer data over the network between executors.
*   **Pipelining:** Spark optimizes narrow transformations by collapsing them into a single stage. For example, if you write `df.filter(...).map(...).select(...)`, Spark fuses these three operations into a single task. The engine reads a record, filters it, maps it, and selects it in one pass, without writing intermediate results to memory or disk.
*   **Speed:** These are extremely fast and memory-efficient.

**2. Wide Transformations: The Shuffle Cost (High Overhead)**
*   **The Shuffle:** This is the most expensive operation in Spark. It involves:
    *   **Disk I/O:** Writing intermediate data to disk (spilling) to ensure fault tolerance and memory management.
    *   **Network I/O:** Transferring data across different nodes in the cluster to group related keys together.
    *   **Serialization/Deserialization:** Significant CPU overhead to serialize data for transport and deserialize it at the destination.
*   **Stage Boundaries:** Wide transformations break the execution plan (DAG) into **Stages**. Spark cannot proceed to the next stage until the current stage (the shuffle) is complete. This acts as a blocking operation, preventing parallelization across the boundary.
*   **Data Skew:** Wide transformations are prone to data skew. If one key (e.g., a popular `customer_id`) has significantly more data than others, one partition will become massive, causing the specific executor processing it to run out of memory (OOM) or lag behind the others (straggler tasks).


## Actions (what triggers execution)
Actions force Spark to execute the DAG and either return something to the driver or write results externally. Typical actions include `count`, `collect`, `take`, `first`, `show`, `write.save(...)`, and (in RDD land) `reduce`.

A practical way to think about it:
- Transformations = “build the plan”
- Actions = “run the plan now” (and Spark breaks it into jobs/stages/tasks during execution)

## Spark behavior in practice
The biggest “aha” is that multiple transformations can look like they run instantly, until an action appears - then Spark executes *everything needed* to compute that action’s result. This is why calling two actions on the same uncached pipeline can recompute the same upstream work twice, unless the intermediate result is persisted/cached.

**Example (PySpark DataFrame):**
```python
df2 = (df
       .filter("country = 'IN'")          # narrow transformation (each input partition is filtered independently -> produces one output partition per input partition)
       .select("user_id", "amount")       # narrow transformation (columns are selected per partition independently)
       .groupBy("user_id").sum("amount")  # wide transformation (shuffle happens -> same user_id across partitions end up in the same partition -> amounts are summed)
      )

df2.show(10)    # action -> triggers a job
df2.count()     # action -> may trigger another job (recompute) unless cached
```

### Transformation vs action
| Aspect | Transformations | Actions |
|---|---|---|
| What it does | Defines a new dataset from an existing one | Materializes a result (driver/output sink)  |
| Execution | Lazy (builds lineage/DAG) | Triggers execution (job starts)  |
| Typical output | Another DataFrame/RDD | A value, a collection, or written data |

### Narrow vs wide transformations
| Aspect | Narrow | Wide |
|---|---|---|
| Partition dependency | 1-to-1 (local) | Many-to-many (redistribution) |
| Shuffle risk | Low | Data is shuffled  |
| Examples | `filter`, `select`, `withColumn` | `groupBy`, `join`, `distinct`, `repartition` |
