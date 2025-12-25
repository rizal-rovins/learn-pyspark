PySpark provides a range of functions to perform arithmetic and mathematical operations, making it easier to manipulate numerical data. These functions are part of the `pyspark.sql.functions` module and can be applied to DataFrame columns.

Here we will go through the most commonly used functions. You can refer to official documention for the entire list of functions [here](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#mathematical-functions).

---

### **Arithmetic Operations in PySpark**

Arithmetic operations are straightforward and can be performed directly on DataFrame columns using Python operators.

| Operation    | Example Syntax                       | Description                        |
|--------------|--------------------------------------|------------------------------------|
| Addition     | `df.withColumn("sum", df["a"] + df["b"])` | Adds values in columns `a` and `b`. |
| Subtraction  | `df.withColumn("diff", df["a"] - df["b"])` | Subtracts `b` from `a`.            |
| Multiplication | `df.withColumn("product", df["a"] * df["b"])` | Multiplies `a` and `b`.            |
| Division     | `df.withColumn("quotient", df["a"] / df["b"])` | Divides `a` by `b`.                |
| Modulo       | `df.withColumn("remainder", df["a"] % df["b"])` | Computes remainder of `a/b`.       |

---

### **Math Functions in PySpark**

#### **Basic Functions**
| Function | Syntax Example                        | Description                                 |
|----------|---------------------------------------|---------------------------------------------|
| `abs`    | `df.withColumn("abs_val", abs(col("a")))` | Absolute value of a column.                 |
| `round`  | `df.withColumn("rounded", round(col("a"), 2))` | Rounds to 2 decimal places.                |
| `ceil`   | `df.withColumn("ceil_val", ceil(col("a")))` | Rounds up to the nearest integer.          |
| `floor`  | `df.withColumn("floor_val", floor(col("a")))` | Rounds down to the nearest integer.        |

#### **Exponential and Logarithmic Functions**
| Function | Syntax Example                         | Description                                  |
|----------|----------------------------------------|----------------------------------------------|
| `exp`    | `df.withColumn("exp_val", exp(col("a")))` | Exponential value of a column.              |
| `log`    | `df.withColumn("log_val", log(col("a")))` | Natural logarithm of a column.              |
| `log10`  | `df.withColumn("log10_val", log10(col("a")))` | Base-10 logarithm of a column.              |
| `pow`    | `df.withColumn("power_val", pow(col("a"), 3))` | Raises column `a` to the power of 3.        |
| `sqrt`   | `df.withColumn("sqrt_val", sqrt(col("a")))` | Square root of a column.                    |

#### **Trigonometric Functions**
| Function | Syntax Example                         | Description                                  |
|----------|----------------------------------------|----------------------------------------------|
| `sin`    | `df.withColumn("sin_val", sin(col("a")))` | Sine of a column (in radians).              |
| `cos`    | `df.withColumn("cos_val", cos(col("a")))` | Cosine of a column (in radians).            |
| `tan`    | `df.withColumn("tan_val", tan(col("a")))` | Tangent of a column (in radians).           |

---

### **Examples**

#### Example 1: Using Arithmetic Operations
Copy the code and try it out in our [PySpark Online Compiler](../pyspark-online-compiler)!
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("Arithmetic Example").getOrCreate()

# Sample data
data = [(1, 10, 3), (2, 20, 5), (3, 15, 4)]
df = spark.createDataFrame(data, ["id", "value1", "value2"])

# Perform arithmetic operations
df = df.withColumn("sum", col("value1") + col("value2")) \
       .withColumn("difference", col("value1") - col("value2")) \
       .withColumn("product", col("value1") * col("value2")) \
       .withColumn("quotient", col("value1") / col("value2")) \
       .withColumn("remainder", col("value1") % col("value2"))

df.show()
```

**Output:**
```
+---+------+-------+---+----------+-------+---------+---------+
| id|value1|value2 |sum|difference|product|quotient |remainder|
+---+------+-------+---+----------+-------+---------+---------+
|  1|    10|      3| 13|         7|     30|      3.3|        1|
|  2|    20|      5| 25|        15|    100|      4.0|        0|
|  3|    15|      4| 19|        11|     60|      3.8|        3|
+---+------+-------+---+----------+-------+---------+---------+
```

---

#### Example 2: Using Math Functions
```python
from pyspark.sql.functions import abs, round, ceil, sqrt

# Apply math functions
df = df.withColumn("absolute_value1", abs(col("value1"))) \
       .withColumn("rounded_value2", round(col("value2"), 1)) \
       .withColumn("ceil_value1", ceil(col("value1"))) \
       .withColumn("sqrt_value2", sqrt(col("value2")))

df.show()
```

**Output:**
```
+---+------+-------+----------------+---------------+----------+---------+
| id|value1|value2 |absolute_value1|rounded_value2 |ceil_value1|sqrt_value2|
+---+------+-------+----------------+---------------+----------+---------+
|  1|    10|      3|              10|              3|        10|      1.73|
|  2|    20|      5|              20|              5|        20|      2.23|
|  3|    15|      4|              15|              4|        15|      2.00|
+---+------+-------+----------------+---------------+----------+---------+
```

---

#### Example 3: Combining Arithmetic and Math Functions
```python
from pyspark.sql.functions import pow, log10

# Combining functions
df = df.withColumn("power_value1", pow(col("value1"), 2)) \
       .withColumn("log10_value2", log10(col("value2")))

df.show()
```

**Output:**
```
+---+------+-------+------------+--------------+
| id|value1|value2 |power_value1|log10_value2  |
+---+------+-------+------------+--------------+
|  1|    10|      3|         100|          0.48|
|  2|    20|      5|         400|          0.70|
|  3|    15|      4|         225|          0.60|
+---+------+-------+------------+--------------+
```

---

### **Conclusion**
This tutorial demonstrated how to use arithmetic and math functions in PySpark for data manipulation. By combining these functions, you can perform a variety of mathematical operations efficiently. 


Copy the code and try it out in our [PySpark Online Compiler](../pyspark-online-compiler)! to explore further.