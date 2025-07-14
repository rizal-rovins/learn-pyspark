Grouping in PySpark is similar to SQL's `GROUP BY`, allowing you to summarize data and calculate aggregate metrics like counts, sums, and averages. This tutorial explains the basics of grouping in PySpark.

---

### Grouping Data with `groupBy()`

In PySpark, you group data using the `groupBy()` method. This groups rows based on the values of one or more columns.

```python
# Example: Grouping by a single column
grouped_df = df.groupBy("department")

# Example: Grouping by multiple columns
grouped_df = df.groupBy("department", "location")
```

After grouping, you can perform aggregations (e.g., counting, summing) on each group. After aggregation, `alias()` method is usually used to rename the aggregated column.

---

### Why Use `F` for Functions?

In PySpark, most aggregation functions are available in the `pyspark.sql.functions` module. It's a common practice to import this module as `F` for two reasons:

1. **Clarity**: It distinguishes PySpark functions (e.g., `F.sum`) from Python built-ins (e.g., `sum`).
2. **Readability**: Using `F` makes the code concise and easier to understand.

```python
#Aggregation basics
from pyspark.sql import functions as F

# Performing a single aggregation
grouped_sum = df.groupBy("department").sum("salary")

# Using F for aggregation and renaming the aggregate column
aggregated_df = df.groupBy("department").agg(F.sum("salary").alias("total_salary"))

#Performing multiple aggregations
aggregated_df = df.groupBy("department").agg(
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("average_salary"),
    F.count("*").alias("employee_count")
)
```

---

### Common Aggregation Functions

Here’s a breakdown of the main aggregation functions you can use after grouping:

1. **`count()`**: Counts the number of rows in each group.

    ```python
    # Counting rows per department
    grouped_count = df.groupBy("department").count()
    ```

2. **`sum()`**: Calculates the total of a numeric column.

    ```python
    # Summing salaries in each department
    grouped_sum = df.groupBy("department").sum("salary")
    ```

3. **`avg()`**: Computes the average of a numeric column.

    ```python
    # Average salary per department
    grouped_avg = df.groupBy("department").avg("salary")
    ```

4. **`min()` and `max()`**: Finds the minimum or maximum value for each group.

    ```python
    # Minimum and maximum salary per department
    grouped_min_max = df.groupBy("department").min("salary").max("salary")
    ```

5. **`countDistinct()`**: Counts the distinct values in a column for each group.

    ```python
    # Counting distinct job roles per department
    distinct_count = df.groupBy("department").agg(F.countDistinct("job_role").alias("unique_roles"))
    ```

---

### Using `agg()` for Multiple Aggregations

To perform multiple aggregations at once, use the `agg()` method. This is useful when you want different metrics for different columns.

```python
# Multiple aggregations
aggregated_df = df.groupBy("department").agg(
    F.avg("salary").alias("average_salary"),
    F.sum("bonus").alias("total_bonus")
)
```

---

### Sample Code
This code demonstrates grouping in PySpark. Copy the code and try it out in our [PySpark Online Compiler](../pyspark-online-compiler)!
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySpark Grouping Example") \
    .getOrCreate()

# Sample DataFrame
data = [
    ("Alice", "HR", 5000),
    ("Bob", "IT", 6000),
    ("Charlie", "Finance", 7000),
    ("David", "IT", 6000),
    ("Eve", "HR", 5500),
    ("Frank", "Finance", 8000),
]
columns = ["name", "department", "salary"]

df = spark.createDataFrame(data, columns)

# Show original data
print("Original Data:")
df.show()

# Group by department and calculate aggregates
print("Group by Department - Count:")
df.groupBy("department").count().show()

print("Group by Department - Sum of Salaries:")
df.groupBy("department").sum("salary").show()

print("Group by Department - Average Salary:")
df.groupBy("department").agg(avg("salary")).show()

print("Group by Department - Multiple Aggregates:")
df.groupBy("department").agg(
    count("name").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("average_salary")
).show()

# Stop Spark Session
spark.stop()
```

### Summary

Grouping in PySpark is a powerful way to summarize data, similar to SQL's `GROUP BY`. The `F` module provides access to aggregation functions, ensuring clarity and conciseness in your code. 

Whether you need simple counts or complex multi-metric aggregations, PySpark makes it straightforward to handle large datasets efficiently.