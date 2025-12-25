

Working with missing values is one of the most common tasks in data
engineering. PySpark provides several useful functions to clean,
replace, or drop null values.

------------------------------------------------------------------------

## ✅ 1. `na.fill()`

Fill missing values with a specific value.

### **Example**

``` python
df = spark.createDataFrame([
    (1, None, "A"),
    (2, 20, None),
    (3, None, "C")
], ["id", "age", "category"])

df_filled = df.na.fill({
    "age": 0,
    "category": "Unknown"
})
df_filled.show()
```

### **Output**

    +---+---+--------+
    | id|age|category|
    +---+---+--------+
    |  1|  0|       A|
    |  2| 20| Unknown|
    |  3|  0|       C|
    +---+---+--------+

------------------------------------------------------------------------

## ✅ 2. `dropna()`

Drop rows containing missing values.

### **Example**

``` python
df_dropped = df.dropna()   # drops rows with ANY null
df_dropped.show()
```

### **Output**

    +---+---+--------+
    | id|age|category|
    +---+---+--------+
    |  2| 20|    None|
    +---+---+--------+

### **Drop only if ALL values are null**

``` python
df.dropna(how="all")
```

### **Drop if specific columns contain null**

``` python
df.dropna(subset=["age"])
```

------------------------------------------------------------------------

## ✅ 3. `na.replace()`

Replace specific values (not only nulls).

### **Example**

``` python
df_replace = df.na.replace({
    "A": "Category-A",
    "C": "Category-C"
})
df_replace.show()
```

### **Output**

    +---+----+-----------+
    | id| age|   category|
    +---+----+-----------+
    |  1|null| Category-A|
    |  2|  20|       null|
    |  3|null| Category-C|
    +---+----+-----------+

------------------------------------------------------------------------

## 🎯 Summary

  Function         Purpose
  ---------------- --------------------------------------
  `na.fill()`      Fill missing values with constants
  `dropna()`       Remove rows with null values
  `na.replace()`   Replace specific values in DataFrame

------------------------------------------------------------------------

## 📌 Best Practices

-   Use `dropna()` carefully --- you may lose important data.
-   Use `na.fill()` for numeric columns → fill with 0 or mean.
-   Use `na.fill()` for string columns → fill with `"Unknown"`.
-   Use `na.replace()` for value corrections (not only nulls).

------------------------------------------------------------------------

