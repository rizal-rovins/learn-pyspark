In PySpark, filtering data is akin to SQL’s `WHERE` clause but offers additional flexibility for large datasets. Filtering operations help you isolate and work with only the data you need, efficiently leveraging Spark’s distributed power. PySpark provides several ways to filter data using `filter()` and `where()` functions, with various options for defining filter conditions.

---

### 1. Basic Filtering: Using `filter()` and `where()` Methods

The primary methods for filtering in PySpark are `filter()` and `where()`. Both are functionally identical and can take either expressions or column conditions, providing versatility depending on your data needs.

```python
# Example of basic filtering
filtered_df = df.filter(df["age"] > 30)
# or
filtered_df = df.where("age > 30")
```

For SQL users, the `.where()` method feels intuitive, mirroring SQL syntax.

---

### 2. Combining Conditions with Logical Operators

You can filter based on multiple conditions using logical operators like `&` (AND), `|` (OR), and `~` (NOT). These allow you to specify complex filter criteria similar to SQL.

- **AND Condition (`&`)**:
  
    ```python
    filtered_df = df.filter((df["age"] > 30) & (df["salary"] > 50000))
    ```

- **OR Condition (`|`)**:
  
    ```python
    filtered_df = df.filter((df["age"] > 30) | (df["department"] == "HR"))
    ```

- **NOT Condition (`~`)**:

    ```python
    filtered_df = df.filter(~(df["department"] == "HR"))
    ```

These logical operators are particularly useful when filtering on multiple columns.

---

### 3. Filtering with String Conditions

If you’re familiar with SQL, PySpark’s `filter()` and `where()` methods support string-based expressions, allowing you to write SQL-like conditions directly.

```python
filtered_df = df.filter("age > 30 AND department = 'HR'")
```

String conditions make filtering code concise, especially when you’re dealing with several conditions.

---

### 4. Filtering with the `isin()` Method

The `isin()` method is ideal when filtering based on a set of values, similar to SQL’s `IN` operator. This is useful for checking membership in a specific list or set.

```python
# Filtering where the department is either 'HR' or 'Finance'
filtered_df = df.filter(df["department"].isin("HR", "Finance"))
```

With `isin()`, you can efficiently filter multiple values within a single column.

---

### 5. Filtering with `startswith()`

The `startswith()` function is used to filter rows where the value in a column starts with a specific substring. This is similar to using `LIKE 'abc%'` in SQL.

```python
# Filtering rows where the name starts with 'A'
filtered_df = df.filter(df["name"].startswith("A"))
```

This would keep rows where the `name` column begins with the letter "A". This function is case-sensitive, so it won’t match lowercase 'a' unless specified.

---

### 6. Filtering with `endswith()`

Similarly, the `endswith()` function allows filtering rows based on whether a column value ends with a specified substring. This works like SQL’s `LIKE '%xyz'`.

```python
# Filtering rows where the name ends with 'son'
filtered_df = df.filter(df["name"].endswith("son"))
```

This will keep rows where the `name` column ends with "son", such as "Anderson" or "Jackson".

---

### 7. Filtering with `like()` and `rlike()` for Pattern Matching

For pattern matching, PySpark provides the `like()` and `rlike()` functions, which work similarly to SQL’s `LIKE` and `REGEXP` operators.

- **Basic Pattern Matching with `like()`**: This is helpful for simple wildcard filtering, such as filtering names that start with a particular letter.

    ```python
    # Names starting with 'A'
    filtered_df = df.filter(df["name"].like("A%"))
    ```

- **Regex Pattern Matching with `rlike()`**: Use `rlike()` for more complex patterns, as it supports regular expressions.

    ```python
    # Names containing 'son'
    filtered_df = df.filter(df["name"].rlike("son"))
    ```

Pattern matching options like these are great for string filtering and are especially helpful for datasets with unstructured text.

---

### 8. Null Handling: `isNull()` and `isNotNull()`

Handling `NULL` values is often crucial in filtering operations. You can use `isNull()` and `isNotNull()` to specifically filter rows based on the presence or absence of null values.

```python
# Filter rows where age is NULL
filtered_df = df.filter(df["age"].isNull())

# Filter rows where age is NOT NULL
filtered_df = df.filter(df["age"].isNotNull())
```

These methods are especially useful in datasets where missing values are common, allowing you to clean and focus on populated data.

---

### 9. Filtering with SQL Queries (Using `sql()` Method)

For users familiar with SQL syntax, PySpark allows you to run SQL queries on a DataFrame by first registering it as a temporary table.

```python
# Register the DataFrame as a temporary table
df.createOrReplaceTempView("employees")

# Use SQL query for filtering
filtered_df = spark.sql("SELECT * FROM employees WHERE age > 30")
```

This approach can be particularly effective if you’re more comfortable with SQL or if your filter conditions are complex.

---

### Sample Code
This code demonstrates filtering in PySpark. Copy the code and try it out in our [PySpark Online Compiler](../pyspark-online-compiler)!
```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySpark Filtering Example") \
    .getOrCreate()

# Sample DataFrame
data = [
    ("Alice", "HR", 5000),
    ("Bob", "IT", 6000),
    ("Charlie", "Finance", 7000),
    ("David", "IT", 4000),
    ("Eve", "HR", 5500),
    ("Frank", "Finance", 8000),
]
columns = ["name", "department", "salary"]

df = spark.createDataFrame(data, columns)

# Show original data
print("Original Data:")
df.show()

# Filter rows with salary greater than 6000
print("Filter: Salary > 6000")
df.filter(df.salary > 6000).show()

# Filter rows belonging to a specific department
print("Filter: Department = 'IT'")
df.filter(df.department == "IT").show()

# Combine multiple filter conditions
print("Filter: Salary > 5000 and Department = 'HR'")
df.filter((df.salary > 5000) & (df.department == "HR")).show()

# Using SQL-like where clause
print("Filter: Salary < 6000 using where()")
df.where("salary < 6000").show()

# Filter using isin (e.g., department is either IT or HR)
print("Filter: Department in ('IT', 'HR')")
df.filter(df.department.isin("IT", "HR")).show()

# Filter rows where a column is null or not null
from pyspark.sql.functions import col
data_with_nulls = [
    ("Alice", None, 5000),
    ("Bob", "IT", 6000),
    (None, "Finance", 7000),
    ("David", "IT", 4000),
]
df_with_nulls = spark.createDataFrame(data_with_nulls, columns)

print("Filter: Rows where department is not null")
df_with_nulls.filter(col("department").isNotNull()).show()

print("Filter: Rows where name is null")
df_with_nulls.filter(col("name").isNull()).show()

# Stop Spark Session
spark.stop()
```

### Summary

With PySpark, filtering operations can be fine-tuned using various methods, from basic conditions to pattern matching, null handling, and SQL queries. This flexibility lets you tailor your filters efficiently for any data scenario, making PySpark a powerful choice for handling large datasets with SQL-like familiarity.