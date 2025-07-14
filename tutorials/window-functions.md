Window functions in PySpark allow you to perform computations over subsets of rows related to the current row, grouped by a specific partition and ordered within that partition. These functions are conceptually similar to SQL window functions, offering powerful tools for ranking, aggregation, and analytical computations.

In this tutorial, we’ll explore the core functionalities of PySpark window functions and their use cases. If you’re familiar with SQL, you’ll recognize these operations but implemented in PySpark using the `pyspark.sql.functions` module and `Window` from `pyspark.sql.window`.

---

## **Window Specification in PySpark**  

To define a window in PySpark, you use the `Window` specification, which includes:  

1. **`partitionBy`**: Similar to SQL's `PARTITION BY`, it divides data into groups based on one or more columns.  
2. **`orderBy`**: Similar to SQL's `ORDER BY`, it orders the rows within each partition.  

Example:  

```python
from pyspark.sql import Window

window_spec = Window.partitionBy("category").orderBy("sales")
```

This specification groups rows by `category` and orders them by `sales` within each category.

---

## **Aggregate Functions in Window Context**  

Aggregate functions such as `sum`, `avg`, `min`, and `max` can be applied over a defined window to perform operations like running totals or averages.

| **Function**  | **Description**              |
|---------------|------------------------------|
| `sum`         | Running total over a window. |
| `avg`         | Running average.             |
| `min`         | Minimum value.               |
| `max`         | Maximum value.               |
| `count`       | Counts rows in the window.   |

### **Use Cases**

1. **Calculate Running Totals**  
   Example: Compute cumulative sales for each category.  

   ```python
   from pyspark.sql.functions import col, sum

   df = df.withColumn("cumulative_sales", sum("sales").over(window_spec))
   ```

2. **Find Maximum Sales per Partition**  
   Example: Find the highest sales value within each category.  

   ```python
   from pyspark.sql.functions import max

   df = df.withColumn("max_sales", max("sales").over(window_spec))
   ```

3. **Calculate Average Sales**  
   Example: Compute the average sales within each category.  

   ```python
   from pyspark.sql.functions import avg

   df = df.withColumn("average_sales", avg("sales").over(window_spec))
   ```

---

## **Ranking Functions**  

Ranking functions are used to assign a rank or sequence to rows within a window partition. See the Complete Example section at the end for output generated.

| **Function**    | **Description**                                        | **Example**                                                                                              |
|------------------|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `rank`          | Assigns a rank to each row, leaving gaps for ties.      | `df.withColumn("rank", rank().over(windowSpec))`<br>Output: `1, 2, 2, 4` for tied rows.                  |
| `dense_rank`    | Assigns a rank to each row without leaving gaps.        | `df.withColumn("dense_rank", dense_rank().over(windowSpec))`<br>Output: `1, 2, 2, 3` for tied rows.      |
| `row_number`    | Assigns a unique sequential number to each row.         | `df.withColumn("row_number", row_number().over(windowSpec))`<br>Output: `1, 2, 3, 4`.                   |

### **Use Cases**

1. **Rank Products by Sales**  
   Example: Assign ranks to products within each category based on their sales.  

   ```python
   from pyspark.sql.functions import rank

   df = df.withColumn("rank", rank().over(window_spec))
   ```

2. **Dense Rank for Top Performers**  
   Example: Assign dense ranks to identify the top products.  

   ```python
   from pyspark.sql.functions import dense_rank

   df = df.withColumn("dense_rank", dense_rank().over(window_spec))
   ```

3. **Unique Row Numbers**  
   Example: Assign a unique row number to each product within its category.  

   ```python
   from pyspark.sql.functions import row_number

   df = df.withColumn("row_number", row_number().over(window_spec))
   ```

---

## **Combining Multiple Window Functions**  

You can apply multiple window functions to a DataFrame by defining window specifications and reusing them.

### Example: Combine Ranking and Aggregation  

```python
from pyspark.sql.functions import sum, rank, avg

df = df \
    .withColumn("rank", rank().over(window_spec)) \
    .withColumn("cumulative_sales", sum("sales").over(window_spec)) \
    .withColumn("average_sales", avg("sales").over(window_spec))
```

---

## **Complete Example**  

Here’s a detailed example demonstrating the discussed window functions that you can execute directly in the **[PySpark Online Compiler](../pyspark-online-compiler)**::  

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, rank, dense_rank, row_number
from pyspark.sql.window import Window

# Sample data
data = [
    ("Electronics", "Phone", 1000),
    ("Electronics", "Laptop", 1500),
    ("Electronics", "Tablet", 800),
    ("Furniture", "Chair", 300),
    ("Furniture", "Table", 300),
    ("Furniture", "Desk", 600),
]

# Create DataFrame
spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()
df = spark.createDataFrame(data, ["category", "product", "sales"])

# Define window specification
window_spec = Window.partitionBy("category").orderBy("sales")

# Apply window functions
df_transformed = df \
    .withColumn("rank", rank().over(window_spec)) \
    .withColumn("dense_rank", dense_rank().over(window_spec)) \
    .withColumn("row_number", row_number().over(window_spec)) \
    .withColumn("cumulative_sales", sum("sales").over(window_spec)) \
    .withColumn("average_sales", avg("sales").over(window_spec))

df_transformed.show()
```

---

### **Output**  

```
+-----------+-------+-----+----+----------+----------+----------------+-------------+
|   category|product|sales|rank|dense_rank|row_number|cumulative_sales|average_sales|
+-----------+-------+-----+----+----------+----------+----------------+-------------+
|Electronics| Tablet|  800|   1|         1|         1|             800|        800.0|
|Electronics|  Phone| 1000|   2|         2|         2|            1800|        900.0|
|Electronics| Laptop| 1500|   3|         3|         3|            3300|       1100.0|
|  Furniture|  Chair|  300|   1|         1|         1|             600|        300.0|
|  Furniture|  Table|  300|   1|         1|         2|             600|        300.0|
|  Furniture|   Desk|  600|   3|         2|         3|            1200|        400.0|
+-----------+-------+-----+----+----------+----------+----------------+-------------+

```

---

## **Summary**

This tutorial covered:  

- Window specifications with `partitionBy` and `orderBy`.  
- Aggregate functions (`sum`, `avg`, `min`, `max`, `count`) for running totals and averages.  
- Ranking functions (`rank`, `dense_rank`, `row_number`) for assigning ranks and sequences.  

In **Part 2**, we’ll dive into advanced window functions such as `lag`, `lead`, and working with custom ranges using `rowsBetween`.