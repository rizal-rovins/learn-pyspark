Pivoting is a data-reshaping operation where you convert rows into columns - turning a “long” format into a “wide” one. In PySpark, you generally use `.groupBy()` + `.pivot()` + an aggregation to accomplish this.

---

## Syntax

```python
pivot_df = df.groupBy("grouping_column") \
             .pivot("pivot_column", values=[ …optional list… ]) \
             .agg({"agg_column": "agg_function"})
```

* `grouping_column`: the column(s) to group by.
* `pivot_column`: the column whose **distinct values** will become new columns.
* `values`: an optional list of values in the pivot column to restrict to (otherwise Spark infers distinct values).
* `agg_column / agg_function`: the column to aggregate and the aggregation function (sum, avg, count, etc.).

---

## Example: Basic Pivot

Let’s create a sample DataFrame and run a simple pivot. Copy the code and try it out in our [PySpark Online Compiler](../pyspark-online-compiler)!

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("PySpark Pivot Example") \
        .getOrCreate()

data = [
    ("A", "East", 100),
    ("A", "West", 150),
    ("B", "East", 200),
    ("B", "West", 250),
    ("C", "East", 300),
    ("C", "West", 350)
]
columns = ["Product", "Region", "Sales"]

df = spark.createDataFrame(data, columns)
df.show()
```

Output:

```
+-------+------+-----+
|Product|Region|Sales|
+-------+------+-----+
|      A|  East|  100|
|      A|  West|  150|
|      B|  East|  200|
|      B|  West|  250|
|      C|  East|  300|
|      C|  West|  350|
+-------+------+-----+
```

Now pivot by `Region`, grouping by `Product`, aggregating `sum(Sales)`:

```python
pivot_df = df.groupBy("Product") \
             .pivot("Region") \
             .sum("Sales")

pivot_df.show()
```

Expected result:

```
+-------+----+----+
|Product|East|West|
+-------+----+----+
|      A| 100| 150|
|      B| 200| 250|
|      C| 300| 350|
+-------+----+----+
```

This matches the “wide” layout: each region becomes a column, and the values are the sum of sales.

---

## Specifying Values List

If the `pivot_column` has many distinct values but you only want a subset, you can provide the `values=` parameter:

```python
pivot_df2 = df.groupBy("Product") \
              .pivot("Region", ["East"]) \
              .sum("Sales")

pivot_df2.show()
```

This will only produce the “East” column (ignoring “West”). Useful to control output width.

---

## Multiple Aggregations

You can also apply more complex aggregation(s) via `.agg()` after `.pivot()`. For example:

```python
from pyspark.sql.functions import sum, avg

agg_df = df.groupBy("Product") \
           .pivot("Region") \
           .agg(
             sum("Sales").alias("total_sales"),
             avg("Sales").alias("avg_sales")
           )

agg_df.show()
```

This will produce two columns per region (e.g., `East_total_sales`, `East_avg_sales`, `West_total_sales`, `West_avg_sales`).

---

## Use-Cases & Best Practices

**When to use pivot?**

* When you need to create cross-tabulations (e.g., totals by category across groups).
* When you want wide-format output for reporting or downstream tools.

**Things to watch out for:**

* If the pivot column has many distinct values, the resulting DataFrame becomes *very wide*, which may cause memory or performance issues.
* Providing `values=` list helps avoid unwanted wide output.
* Null values: If there is no data for a particular pivot value+grouping, you’ll see `null` in that column. Handle accordingly (e.g., `.fillna(0)`).
* Pivot causes shuffling of data; ensure proper partitioning / resource planning when handling large datasets.