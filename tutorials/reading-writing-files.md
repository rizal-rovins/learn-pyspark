PySpark provides powerful and flexible APIs to **read and write data** from a variety of sources - including **CSV, JSON, Parquet, ORC, and databases** - using the Spark DataFrame interface.
These operations form the backbone of most ETL (Extract, Transform, Load) pipelines, enabling you to process data at scale in a distributed environment.

In this tutorial, you’ll learn the **general patterns for reading and writing files in PySpark**, understand the meaning of common parameters, and see examples for different data formats.
By the end, you’ll be comfortable handling input and output operations in PySpark using clean, reusable code.


Let's start with reading files. This is the general format for reading files in PySpark. It works for all file formats.

```python
from pyspark.sql import SparkSession

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read File Example") \
    .getOrCreate()

# Step 2: Read data
df = spark.read \
    .format("<file_format>") \
    .option("<option_name>", "<option_value>") \
    .load("<path_to_file_or_folder>")

# Step 3: Inspect data
df.show(5)
df.printSchema()
```

---

### Parameters Explained

| Parameter              | Description                        | Example                                                         |
| ---------------------- | ---------------------------------- | --------------------------------------------------------------- |
| **`<file_format>`**    | Type of data source                | `"csv"`, `"json"`, `"parquet"`, `"orc"`, `"jdbc"`               |
| **`option()`**         | Used to configure reading options  | `.option("header", "true")` or `.option("inferSchema", "true")` |
| **`load()`**           | Path to the file or directory      | `"/datasets/customers.csv"`                                     |                                                        |

---

#### Example 1: Reading a CSV File

```python
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/datasets/customers.csv")
```

#### Example 2: Reading a Parquet File

```python
df = spark.read \
    .format("parquet") \
    .load("/datasets/sales.parquet")
```

#### Example 3: Reading a JSON File

```python
df = spark.read \
    .format("json") \
    .load("/datasets/events.json")
```

#### Example 4: Reading from a Hive Table

You can also read data from Hive tables using `spark.read.table()` or SQL queries.

```python
# Reading a Hive table customers in default schema into a DataFrame
df_hive = spark.read.table("default.customers")

df_hive.show(5)
df_hive.printSchema()
```

Or using Spark SQL:

```python
# Querying a Hive table with SQL
df_hive_sql = spark.sql("SELECT id, name, email FROM default.customers WHERE country='US'")
df_hive_sql.show(5)
```

#### Example 5: Reading from a Database (JDBC)

```python
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "public.customers") \
    .option("user", "postgres") \
    .option("password", "mypassword") \
    .load()
```

---

### Shortcut for Common Formats

For CSV, JSON, and Parquet, you can skip `.format()` and directly use built-in methods. You will see this sytax being used commonly.

```python
df_csv = spark.read.csv(path, header=True, inferSchema=True)
df_json = spark.read.json(path)
df_parquet = spark.read.parquet(path)
```

---

### Read Modes Explained

Read modes handle malformed or corrupt records during data loading from formats like JSON, CSV, or Parquet. These modes determine whether to process bad data leniently, skip it, or fail the job.

| Mode            | Description                                                                 |
|-----------------|-----------------------------------------------------------------------------|
| **`PERMISSIVE`** | Default mode; sets corrupt record fields to null and stores the raw data in `_corrupt_record` column.  |
| **`DROPMALFORMED`** | Drops entire rows with any schema mismatch or corruption, loading only valid records.  |
| **`FAILFAST`**  | Throws an exception immediately upon encountering the first malformed record.  |

Set these via `.option("mode", "PERMISSIVE")` or `.mode("DROPMALFORMED")` on the DataFrameReader.

---

This format - using `.read.format().options().load()` - is the **most universal and flexible pattern** for reading files in PySpark, especially useful when building reusable code for different data sources.

The **general takeaway** is:

1. **Specify the format** → CSV, JSON, Parquet, ORC, JDBC, etc.
2. **Provide the path** → can be a single file, a folder, or a wildcard pattern.
3. **Set any options** → like `header`, `inferSchema`, `sep`, `quote`, or JDBC-specific options like `url`, `dbtable`, `user`, `password`.

For **JDBC**, yes, there are a few extra options you often need:

* `url` → the JDBC connection string
* `dbtable` → the table or query to read
* `user` / `password` → credentials
* Optional: `fetchsize`, `driver`, `partitionColumn` for parallel reads

Other than that, **the general pattern `.read.format().options().load()` covers almost everything**, and for CSV/JSON/Parquet you can even use the shortcut methods like `.read.csv()` or `.read.parquet()` for convenience.

---

## Writing Files in PySpark

This is the general format for writing files in PySpark. It works for all file formats.

```python
from pyspark.sql import SparkSession

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("Write File Example") \
    .getOrCreate()

# Step 2: Write data
df.write \
    .format("<file_format>") \
    .option("<option_name>", "<option_value>") \
    .mode("<save_mode>") \
    .save("<path_to_output_file_or_folder>")
```

---

### Parameters Explained

| Parameter           | Description                              | Example                                                         |
| ------------------- | ---------------------------------------- | --------------------------------------------------------------- |
| **`<file_format>`** | Type of data output                      | `"csv"`, `"json"`, `"parquet"`, `"orc"`, `"jdbc"`               |
| **`option()`**      | Used to configure writing options        | `.option("header", "true")` or `.option("compression", "gzip")` |
| **`mode()`**        | Save behavior when output already exists | `"overwrite"`, `"append"`, `"ignore"`, `"error"`                |
| **`save()`**        | Path to the file or directory            | `"/output/customers_parquet"`                                   |

---

### Save Modes Explained

| Mode                               | Description                             |
| ---------------------------------- | --------------------------------------- |
| **`overwrite`**                    | Replaces existing data                  |
| **`append`**                       | Adds data to existing files or tables   |
| **`ignore`**                       | Skips writing if the destination exists |
| **`error`** or **`errorifexists`** | Fails if the destination exists         |

---

#### Example 1: Writing a CSV File

```python
df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("/output/customers_csv")
```

#### Example 2: Writing a Parquet File

```python
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("/output/sales_parquet")
```

#### Example 3: Writing a JSON File

```python
df.write \
    .format("json") \
    .mode("overwrite") \
    .save("/output/events_json")
```

#### Example 4: Writing to a Hive Table

You can write a DataFrame to a Hive table using `saveAsTable()`:

```python
# Write DataFrame to a Hive table in the default schema
df.write \
    .mode("overwrite") \
    .saveAsTable("default.customers_copy")
```

#### Example 5: Writing to a Database (JDBC)

```python
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "public.customers") \
    .option("user", "postgres") \
    .option("password", "mypassword") \
    .mode("append") \
    .save()
```

As you can see, it follows the same structure as reading files - you just specify the `format`, `options`, and `mode`, then call `.save()`.

---

### Shortcut for Common Formats

For CSV, JSON, and Parquet, you can skip `.format()` and use built-in write methods directly.
This syntax is very common in practice.

```python
df.write.csv(path="/output/customers", header=True, mode="overwrite")
df.write.json(path="/output/events", mode="overwrite")
df.write.parquet(path="/output/sales", mode="overwrite")
```

---

This format - using `.write.format().options().mode().save()` - is the **most universal and flexible pattern** for writing files in PySpark, making it ideal for reusable ETL pipelines and exporting data to multiple destinations.


The **general takeaway** is:

1. **Structure is consistent**: Just like reading files, writing files follows a universal pattern:

   ```python
   df.write.format(<file_format>).option(...).mode(<save_mode>).save(<path>)
   ```

   This works for all file types - CSV, JSON, Parquet, ORC, and JDBC.

2. **Specify the format** → `"csv"`, `"json"`, `"parquet"`, `"orc"`, `"jdbc"`, etc.

3. **Set options** → configure output-specific settings like:

   * `header=True` for CSV
   * `compression="gzip"` for Parquet/CSV
   * JDBC credentials and table name for databases

4. **Choose a save mode** → controls behavior if the target already exists:

   * `"overwrite"` → replace existing data
   * `"append"` → add to existing data
   * `"ignore"` → skip writing if data exists
   * `"error"` / `"errorifexists"` → fail if target exists

5. **Path** → can be a single file, a directory, or a database table, depending on the format.

6. **Shortcut for common formats** → for CSV, JSON, and Parquet, you can skip `.format()` and use:

   ```python
   df.write.csv(path, header=True, mode="overwrite")
   df.write.json(path, mode="overwrite")
   df.write.parquet(path, mode="overwrite")
   ```

7. **Flexibility & Reusability** → this pattern is ideal for building reusable ETL pipelines, because you can easily switch formats, destinations, or save modes without changing the main logic.

---
