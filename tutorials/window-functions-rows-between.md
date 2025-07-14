Window functions in PySpark allow you to perform calculations across rows that are related to the current row, based on some defined window. The `ROWS BETWEEN` clause lets you define a range of rows around the current row for the window function to consider.

### Syntax

```python
from pyspark.sql.window import Window

# Define a window specification
window_spec = Window.partitionBy(<columns>).orderBy(<column>).rowsBetween(<start>, <end>)

# Apply a window function over the window
df.withColumn("new_column", <window_function>("column").over(window_spec))
```

### Key Concepts

- **`partitionBy(<columns>)`**: This splits the data into groups based on the columns you provide.
- **`orderBy(<column>)`**: This sorts the rows within each group.
- **`rowsBetween(<start>, <end>)`**: This defines the range of rows to consider for each row.

### Parameters for `ROWS BETWEEN`

1. **`<start>`**: Defines where the window starts relative to the current row.
   - **`Window.unboundedPreceding`**: Starts from the first row.
   - **`x PRECEDING`**: Starts `x` rows before the current row.
   - **`Window.currentRow`**: Starts at the current row.

2. **`<end>`**: Defines where the window ends relative to the current row.
   - **`Window.unboundedFollowing`**: Ends at the last row.
   - **`x FOLLOWING`**: Ends `x` rows after the current row.
   - **`Window.currentRow`**: Ends at the current row.

### Examples

#### 1. **Cumulative Sum**
To calculate a running total, the window starts from the first row and ends at the current row.

```python
from pyspark.sql.functions import sum

# Window specification for cumulative sum
window_spec = Window.partitionBy("category").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Apply cumulative sum
df = df.withColumn("cumulative_sales", sum("sales").over(window_spec))

# Show the result
df.show()
```

**Explanation**:
- **`Window.unboundedPreceding`**: Starts from the first row.
- **`Window.currentRow`**: Ends at the current row.

---

#### 2. **Moving Average (3-day window)**
To calculate the average of the past 3 days, the window starts 2 rows before the current row and ends at the current row.

```python
from pyspark.sql.functions import avg

# Window specification for moving average over the last 3 rows
window_spec = Window.partitionBy("category").orderBy("date").rowsBetween(-2, Window.currentRow)

# Apply moving average
df = df.withColumn("moving_avg_sales", avg("sales").over(window_spec))

# Show the result
df.show()
```

**Explanation**:
- **`-2`**: Starts 2 rows before the current row.
- **`Window.currentRow`**: Ends at the current row.

---

#### 3. **Excluding Current and Future Rows**
To calculate a cumulative sum without including the current and future rows, the window starts from the first row and ends one row before the current row.

```python
from pyspark.sql.functions import sum

# Window specification excluding future rows
window_spec = Window.partitionBy("category").orderBy("date").rowsBetween(Window.unboundedPreceding, -1)

# Apply cumulative sum excluding current and future rows
df = df.withColumn("cumulative_sales_excluding_future", sum("sales").over(window_spec))

# Show the result
df.show()
```

**Explanation**:
- **`Window.unboundedPreceding`**: Starts from the first row.
- **`-1`**: Ends one row before the current row.

---

### PySpark Online Compiler Code

Here’s the full code that you can execute directly in the **[PySpark Online Compiler](../pyspark-online-compiler)**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg
from pyspark.sql.window import Window

# Sample data
data = [
    ("2024-01-01", "A", 100),
    ("2024-01-02", "A", 150),
    ("2024-01-03", "A", 200),
    ("2024-01-01", "B", 50),
    ("2024-01-02", "B", 75),
    ("2024-01-03", "B", 100)
]

# Create DataFrame
spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()
df = spark.createDataFrame(data, ["date", "category", "sales"])

# 1. Cumulative Sum
window_spec = Window.partitionBy("category").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_cumulative_sum = df.withColumn("cumulative_sales", sum("sales").over(window_spec))

# 2. Moving Average (3-day window)
window_spec_avg = Window.partitionBy("category").orderBy("date").rowsBetween(-2, Window.currentRow)
df_moving_avg = df_cumulative_sum.withColumn("moving_avg_sales", avg("sales").over(window_spec_avg))

# 3. Excluding Current & Future Rows
window_spec_excluding_future = Window.partitionBy("category").orderBy("date").rowsBetween(Window.unboundedPreceding, -1)
df_final = df_moving_avg.withColumn("cumulative_sales_excluding_curennt_n_future", sum("sales").over(window_spec_excluding_future))

# Show the result
df_final.show()
```

### Expected Output:

```
+----------+--------+-----+-----------------+-----------------+----------------------------------+
|      date|category|sales| cumulative_sales| moving_avg_sales| cumulative_sales_excluding_future|
+----------+--------+-----+-----------------+-----------------+----------------------------------+
|2024-01-01|       A|  100|             100 |            100  |                             NULL |
|2024-01-02|       A|  150|             250 |            125  |                             100  |
|2024-01-03|       A|  200|             450 |            150  |                             250  |
|2024-01-01|       B|   50|              50 |             50  |                             NULL |
|2024-01-02|       B|   75|             125 |             62.5|                             50   |
|2024-01-03|       B|  100|             225 |            75   |                             125  |
+----------+--------+-----+-----------------+-----------------+----------------------------------+
```

### Summary

With the `ROWS BETWEEN` clause, you can define a specific range of rows around the current row to calculate various analytics like cumulative sums, moving averages, and more. The key idea is to control the scope of rows the window function will consider using the start and end parameters. This allows for flexible and complex calculations on your data.

