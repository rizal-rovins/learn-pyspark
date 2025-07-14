When working with date and time in PySpark, the `pyspark.sql.functions` module provides a range of functions to manipulate, format, and query date and time values effectively.

---

### **Commonly Used Date Functions in PySpark**

Here are some important date functions and their usage:

#### **1. Parsing and Converting Dates**
- **`to_date(column, format)`**: Converts a string to a date using the specified format.
- **`to_timestamp(column, format)`**: Converts a string to a timestamp.

Example:
```python
from pyspark.sql.functions import to_date, to_timestamp
df.select(
    to_date(df["date_string"], "yyyy-M-d").alias("parsed_date"),
    to_timestamp(df["timestamp_string"], "yyyy-M-d HH:mm:ss").alias("parsed_timestamp")
)
```

---


#### **2. Formatting Dates**
- **`date_format(date, format)`**: Formats a date into a string with the specified pattern (e.g., `"yyyy-M-d"`, `"d/M/yyyy"`).

Example:
```python
from pyspark.sql.functions import date_format
df.select(
    date_format(df["date"], "d-M-yyyy").alias("formatted_date")
)
```

---

#### **3. Current Date and Time**
- **`current_date()`**: Returns the current date.
- **`current_timestamp()`**: Returns the current timestamp.

Example:
```python
from pyspark.sql.functions import current_date, current_timestamp
df.select(
    current_date().alias("today"),
    current_timestamp().alias("now")
)
```

---

#### **4. Extracting Parts of Dates**
- **`year()`**: Extracts the year from a date.
- **`month()`**: Extracts the month.
- **`dayofmonth()`**: Extracts the day of the month.
- **`dayofweek()`**: Returns the day of the week as an integer (1 = Sunday, 7 = Saturday).
- **`weekofyear()`**: Extracts the week number of the year.

Example:
```python
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear
df.select(
    year(df["date"]).alias("year"),
    month(df["date"]).alias("month"),
    dayofmonth(df["date"]).alias("day"),
    dayofweek(df["date"]).alias("weekday"),
    weekofyear(df["date"]).alias("week_number")
)
```

---

#### **5. Date Arithmetic**
- **`datediff(end, start)`**: Returns the number of days between two dates.
- **`add_months(date, numMonths)`**: Adds or subtracts months from a date.
- **`date_add(date, days)`**: Adds a specific number of days to a date.
- **`date_sub(date, days)`**: Subtracts a specific number of days from a date.

Example:
```python
from pyspark.sql.functions import datediff, add_months, date_add, date_sub
df.select(
    datediff(df["end_date"], df["start_date"]).alias("days_diff"),
    add_months(df["date"], 3).alias("plus_3_months"),
    date_add(df["date"], 7).alias("plus_7_days"),
    date_sub(df["date"], 7).alias("minus_7_days")
)
```

---

#### **6. Truncating Dates**
- **`trunc(date, format)`**: Truncates a date to the specified format (`"year"`, `"month"`, etc.).
- **`date_trunc(format, timestamp)`**: Truncates a timestamp to the specified unit (`"hour"`, `"day"`, `"month"`, etc.).

Example:
```python
from pyspark.sql.functions import trunc, date_trunc
df.select(
    trunc(df["date"], "month").alias("start_of_month"),
    date_trunc("hour", df["timestamp"]).alias("start_of_hour")
)
```

---

### **Sample Code**
The following code demonstrates the usage of these functions and can be executed directly:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_add, date_sub, datediff, months_between, current_date, current_timestamp, year, month, dayofmonth, date_format

# Create a Spark session
spark = SparkSession.builder.master("local").appName("Date Functions").getOrCreate()

# Sample data
data = [
    (1, "2024-11-10", "2024-11-20"),
    (2, "2023-10-05", "2023-10-15"),
    (3, "2022-05-01", "2022-05-10")
]

# Create a DataFrame
df = spark.createDataFrame(data, ["id", "order_date", "delivery_date"])

# Convert strings to date format
df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
df = df.withColumn("delivery_date", to_date(col("delivery_date"), "yyyy-MM-dd"))

# Add current date and timestamp cols
df = df.withColumn("current_date", current_date())
df = df.withColumn("current_timestamp", current_timestamp())

# Calculate the difference in days between delivery and order dates
df = df.withColumn("days_to_delivery", datediff(col("delivery_date"), col("order_date")))

# Add 5 days to the delivery date
df = df.withColumn("delivery_plus_5", date_add(col("delivery_date"), 5))

# Subtract 5 days from the delivery date
df = df.withColumn("delivery_minus_5", date_sub(col("delivery_date"), 5))

# Calculate the months between order and delivery dates
df = df.withColumn("months_between_order_delivery", months_between(col("delivery_date"), col("order_date")))

# Calculate the difference in days between current date and delivery date
df = df.withColumn("days_from_today_to_delivery", datediff(current_date(), col("delivery_date")))

# Extract year, month, and day
df = df.withColumn("year", year(col("order_date")))
df = df.withColumn("month", month(col("order_date")))
df = df.withColumn("day", dayofmonth(col("order_date")))

# Format date as string
df = df.withColumn("formatted_date", date_format(col("order_date"), "MMMM dd, yyyy"))

# Show results
df.show(truncate=False)
```

This code demonstrates how to apply various date functions in PySpark. Copy the code and try it out in our [PySpark Online Compiler](../pyspark-online-compiler)!