Joins in PySpark are similar to SQL joins, enabling you to combine data from two or more DataFrames based on a related column. This tutorial explores the different join types and how to use different parameter configurations.

---

### Types of Joins in PySpark

PySpark’s `join()` function supports the following join types:

| Join Type       | Description                                   |
|------------------|-----------------------------------------------|
| `inner`         | Matches rows from both DataFrames.            |
| `left`          | All rows from the left DataFrame, with `null` for non-matches. |
| `right`         | All rows from the right DataFrame, with `null` for non-matches. |
| `outer`         | All rows from both DataFrames, with `null` for non-matches. |
| `cross`         | Cartesian product of both DataFrames.         |
| `left_semi`     | This is just an inner join of the two DataFrames, but only returns columns of left DataFrame |
| `left_anti`     | Rows from the left where no match exists in the right. |

---

### Syntax and Parameters for `join()`

The `join()` method has the following signature:
```python
DataFrame.join(other, on=None, how=None)
```

- **`other`**: The DataFrame to join with.
- **`on`**:  
  The column(s) to join on. Options include:  
  - A single column name as a string (e.g., `'user_id'`).  
  - Multiple column names as a list (e.g., `['user_id', 'location']`).  
  - A custom condition or expression (e.g., `df1["id"] == df2["user_id"]`).  
  - Left as `None` if the join keys have the same names in both DataFrames.
- **`how`**: The type of join (e.g., `"inner"`, `"left"`, `"right"`, `"outer"`, `"cross"`).

---

### 1. Inner Join (Default Join)

The default join type in PySpark is an inner join. Use it when you want to keep only rows with matching values in both DataFrames.

```python
result = df1.join(df2, on="id", how="inner")
result.show()
```
**Explanation**:
- `on="id"`: Specifies the column to join on.
- `how="inner"`: Explicitly sets the join type (default).

#### Real-Life Examples of Inner Join

1. **Customer Orders**:  
   Combine a list of customers and their orders to show only customers who have placed orders.  

2. **Employee Projects**:  
   Match employees and project assignments to display only employees currently assigned to projects.  

3. **Product Sales**:  
   Merge products and sales data to show only products that have been sold.  
---

### 2. Left Outer Join

Use a left join to keep all rows from the left DataFrame, with `null` for non-matching rows from the right DataFrame. 

```python
result = df1.join(df2, on="id", how="left")
result.show()
```
#### Real-Life Examples of Left Join

1. **Customer Orders**:  
   Combine a list of all customers with their orders to ensure customers without any orders are also included.  

2. **Employee Attendance**:  
   Merge employee data with attendance records to identify employees who haven’t logged attendance.  

3. **Product Inventory**:  
   Match all products with their stock details, including products currently out of stock. 

---

### 3. Left Anti Join

An anti join returns rows from the left DataFrame where there is no match in the right DataFrame.

```python
result = df1.join(df2, on="id", how="left_anti")
result.show()
```
#### Real-Life Examples of Left Anti Join

1. **Unmatched Customers**:  
   Find customers from a customer list who haven’t placed any orders.  

2. **Unassigned Employees**:  
   Identify employees who are not assigned to any project in the project assignments dataset.  

3. **Unsold Products**:  
   List all products that have never been sold based on a sales dataset.  

---

### Joining on Multiple Columns

To join on multiple columns, pass a list to the `on` parameter:

```python
result = df1.join(df2, on=["id", "department"], how="inner")
result.show()
```

**Explanation**:
- `on=["id", "department"]`: Joins on both `id` and `department` columns.

---

### Joining Without Specifying `on`

If the column names are the same in both DataFrames, you can omit the `on` parameter. PySpark will automatically join on columns with the same name.

```python
result = df1.join(df2, how="inner")
result.show()
```

---

### Using Conditions in Joins

You can specify custom join conditions using expressions. This is useful when the join columns have different names or when you need to add additional logic.

```python
from pyspark.sql import functions as F

result = df1.join(df2, df1["id"] == df2["user_id"], how="inner")
result.show()
```

**Explanation**:
- `df1["id"] == df2["user_id"]`: Custom join condition.

---

### Joining with Multiple Conditions

To join two DataFrames with multiple conditions, you can pass a conjunction of conditions to the `join()` method using logical operators like `&` (AND) or `|` (OR). 

```python
joined_df = sales_df.join(
    customers_df,
    (sales_df["customer_id"] == customers_df["customer_id"]) & (sales_df["region"] == customers_df["region"]),
    "inner"
)
```
---

### Joining 2 Dataframes, Selecting all columns from the first and some columns from the second

```python
result_df = sales_df.join(customers_df, on = ["customer_id"], how="inner").select
(
    sales_df["*"],  # All columns from sales_df
    customers_df["name"].alias("customer_name")  # Specific column from customers_df
)
```

### Sample Code
This code demonstrates join dataframes in PySpark. Copy the code and try it out in our [PySpark Online Compiler](../pyspark-online-compiler)!
```python
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("PySpark Join Example") \
    .getOrCreate()

# Sample DataFrame 1 (employees)
data1 = [
    (1, "Alice", "HR"),
    (2, "Bob", "IT"),
    (3, "Charlie", "Finance"),
    (4, "David", "IT")
]
columns1 = ["id", "name", "dept"]
df1 = spark.createDataFrame(data1, columns1)

# Sample DataFrame 2 (departments)
data2 = [
    ("HR", "Human Resources"),
    ("IT", "Information Technology"),
    ("Marketing", "Marketing"),
]
columns2 = ["dept", "dept_name"]
df2 = spark.createDataFrame(data2, columns2)

# Perform joins
# Inner Join
inner_join = df1.join(df2, on="dept", how="inner")
print("Inner Join Result:")
inner_join.show()

# Left Join
left_join = df1.join(df2, on="dept", how="left")
print("Left Join Result:")
left_join.show()

# Full Outer Join
outer_join = df1.join(df2, on="dept", how="outer")
print("Full Outer Join Result:")
outer_join.show()

# Stop the Spark session
spark.stop()
```

### Best Practices for Joins in PySpark

- **Specify Column Names Clearly**: Use the `on` parameter explicitly for clarity.
- **Alias Columns to Avoid Conflicts**: Use `select` or `withColumnRenamed` if joining on columns with the same name but different data.
- **Use Conditions for Custom Logic**: When column names differ or additional logic is needed, use conditional expressions.

With these techniques, you can leverage PySpark's join capabilities to handle complex data combinations effectively.