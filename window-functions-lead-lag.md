Lead and lag functions are essential tools for analyzing sequential data, particularly in scenarios where comparisons between current, previous, and next rows are needed. These functions operate over **windows** and allow you to perform time-series analysis, trend detection, and other row-relative operations.  

If you're familiar with SQL, the PySpark implementation mirrors SQL's `LEAD` and `LAG` functionality but is accessible through the `pyspark.sql.functions` module.  

---

## Understanding Lead and Lag  

### 1. **`lead(column, offset, default)`**  
- Fetches the value of a specified column from the **next row** within the same window.  
- **Parameters**:  
  - `column`: Column to fetch the value from.  
  - `offset`: The number of rows to look ahead (default is 1).  
  - `default`: Value to return if the offset is out of bounds.  

#### Use Cases:  
- Compare current and next values to identify growth or decline trends.  
- Predict the next transaction or event for a user or entity.  

---

### 2. **`lag(column, offset, default)`**  
- Fetches the value of a specified column from the **previous row** within the same window.  
- **Parameters**:  
  - `column`: Column to fetch the value from.  
  - `offset`: The number of rows to look back (default is 1).  
  - `default`: Value to return if the offset is out of bounds.  

#### Use Cases:  
- Compare current and previous values for calculating differences or trends.  
- Retrieve historical context for an entity or event.  

---

## Window Specification  

Both `lead` and `lag` functions require a **window specification** that defines:  
1. **Partitioning**: How the dataset is divided into groups.  
2. **Ordering**: How rows within each partition are ordered.  

```python
from pyspark.sql.window import Window
window_spec = Window.partitionBy("group_column").orderBy("ordering_column")
```

---

## Practical Examples  

### Example 1: Calculate Next and Previous Transaction Amounts  

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy("customer_id").orderBy("date")

# Add lead and lag columns
df = df.withColumn("next_transaction", F.lead("amount", 1).over(window_spec))
df = df.withColumn("previous_transaction", F.lag("amount", 1).over(window_spec))
```

#### Output:  
```
+------------+----------+------+-----------------+---------------------+
| customer_id|      date|amount|next_transaction |previous_transaction |
+------------+----------+------+-----------------+---------------------+
|           1|2024-01-01|   100|              200|                 null|
|           1|2024-01-02|   200|              300|                  100|
|           1|2024-01-03|   300|             null|                  200|
|           2|2024-01-01|    50|               80|                 null|
|           2|2024-01-02|    80|              120|                   50|
|           2|2024-01-03|   120|             null|                   80|
+------------+----------+------+-----------------+---------------------+
```

---

### Example 2: Calculate Purchase Difference  

```python
# Calculate difference between current and previous transactions
df = df.withColumn("purchase_difference", F.col("amount") - F.lag("amount", 1).over(window_spec))
```

#### Output:  
```
+------------+----------+------+-------------------+
| customer_id|      date|amount|purchase_difference|
+------------+----------+------+-------------------+
|           1|2024-01-01|   100|               null|
|           1|2024-01-02|   200|                100|
|           1|2024-01-03|   300|                100|
|           2|2024-01-01|    50|               null|
|           2|2024-01-02|    80|                 30|
|           2|2024-01-03|   120|                 40|
+------------+----------+------+-------------------+
```

---

### Example 3: Predict Future Events  

Using `lead`, you can create a column indicating the next event for an entity, such as a customer's next product purchase or subscription status.  

```python
df = df.withColumn("next_status", F.lead("status", 1).over(window_spec))
```

#### Output:  
```
+------------+----------+----------+--------------+
| customer_id|      date|    status|   next_status|
+------------+----------+----------+--------------+
|           1|2024-01-01|   browsing|       carted|
|           1|2024-01-02|     carted|    purchased|
|           1|2024-01-03|  purchased|         null|
+------------+----------+----------+--------------+
```

---

### Example 4: Fill Missing Data with Defaults  

Lead and lag allow you to define a default value when the offset goes out of bounds.  

```python
df = df.withColumn("previous_transaction", F.lag("amount", 1, 0).over(window_spec))
```

#### Output:  
```
+------------+----------+------+--------------------+
| customer_id|      date|amount|previous_transaction|
+------------+----------+------+--------------------+
|           1|2024-01-01|   100|                   0|
|           1|2024-01-02|   200|                 100|
|           1|2024-01-03|   300|                 200|
+------------+----------+------+--------------------+
```

---

### Example 5: Identify Changes in Status  

By comparing the current row with the previous row, you can identify status changes for an entity.  

```python
df = df.withColumn("status_change", F.col("status") != F.lag("status", 1).over(window_spec))
```

#### Output:  
```
+------------+----------+----------+-------------+
| customer_id|      date|    status|status_change|
+------------+----------+----------+-------------+
|           1|2024-01-01| browsing |        false|
|           1|2024-01-02|   carted |         true|
|           1|2024-01-03| purchased|         true|
+------------+----------+----------+-------------+
```

---

### Sample Code

Here’s the full code that you can execute directly in the **[PySpark Online Compiler](../pyspark-online-compiler)**:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Sample data
data = [
    (1, "2024-01-01", 100),
    (1, "2024-01-02", 200),
    (1, "2024-01-03", 300),
    (2, "2024-01-01", 50),
    (2, "2024-01-02", 80),
    (2, "2024-01-03", 120),
]

# Create DataFrame
spark = SparkSession.builder.appName("LeadLagTutorial").getOrCreate()
df = spark.createDataFrame(data, ["customer_id", "date", "amount"])

# Define window specification
window_spec = Window.partitionBy("customer_id").orderBy("date")

# Apply lead and lag
df = df.withColumn("next_amount", F.lead("amount", 1).over(window_spec))
df = df.withColumn("previous_amount", F.lag("amount", 1).over(window_spec))
df = df.withColumn("amount_diff", F.col("amount") - F.lag("amount", 1).over(window_spec))

df.show()
```

### Output:  
```
+------------+----------+------+-----------+--------------+------------+
| customer_id|      date|amount|next_amount|previous_amount|amount_diff|
+------------+----------+------+-----------+--------------+------------+
|           1|2024-01-01|   100|        200|          null|        null|
|           1|2024-01-02|   200|        300|           100|         100|
|           1|2024-01-03|   300|       null|           200|         100|
|           2|2024-01-01|    50|         80|          null|        null|
|           2|2024-01-02|    80|        120|            50|          30|
|           2|2024-01-03|   120|       null|            80|          40|
+------------+----------+------+-----------+--------------+------------+
```

This tutorial demonstrates the power of **lead** and **lag** in analyzing sequential data. For more advanced use cases, combine these functions with other window operations!