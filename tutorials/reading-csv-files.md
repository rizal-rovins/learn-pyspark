When it comes to handling CSV files, PySpark offers a range of flexible options. Whether you're reading from or writing to a CSV, PySpark's built-in capabilities make it easy to configure these operations exactly how you want.

### Practice Question
Read the tutorial below and try solving this problem to get hands-on practice [here](../pyspark-coding-interview-questions/load-and-transform-data).

### Step 1: Start with a `SparkSession`

As always, start by setting up your `SparkSession`. This is your connection to PySpark’s powerful data processing engine.

```python
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CSV Handling in PySpark") \
    .getOrCreate()

print("Spark is ready! 🚀")
```

### Step 2: Reading a CSV File 📥

To read a CSV file, use `spark.read.csv()`. PySpark offers various options to customize how the CSV is read, so you can handle headers, delimiters, schemas, and more.

#### Basic CSV Reading

Here’s a simple example of loading a CSV with default settings:

```python
# Basic CSV read
df = spark.read.csv("/path/to/your/data.csv")
df.show(5)
```

#### Common CSV Reading Options

PySpark allows you to customize your CSV read operation with these commonly used options:

- **Header**: If your CSV has a header row, set `header=True` to use it as column names.
- **InferSchema**: To automatically determine the data types of each column, set `inferSchema=True`.
- **Delimiter**: To use a custom delimiter (such as `;` or `|`), set the `sep` option.

Example of reading a CSV with these options:

```python
# Reading CSV with options
df = spark.read.option("header", True) \
               .option("inferSchema", True) \
               .option("sep", ";") \
               .csv("/path/to/your/data.csv")

df.show(5)
```

#### Additional Reading Options

- **Null Values**: Use `nullValue` to specify a string to interpret as `null`, such as `"N/A"` or `"NULL"`.
- **Date Format**: Set `dateFormat` to specify the date format for date fields.
- **Mode**: Determines how Spark handles corrupt records. Options include:
  - `PERMISSIVE` (default): Puts corrupt records in a separate column.
  - `DROPMALFORMED`: Drops rows with bad records.
  - `FAILFAST`: Throws an error for corrupt records.

Example with null values, date format, and mode options:

```python
# Reading CSV with more options
df = spark.read.option("header", True) \
               .option("inferSchema", True) \
               .option("nullValue", "N/A") \
               .option("dateFormat", "MM/dd/yyyy") \
               .option("mode", "DROPMALFORMED") \
               .csv("/path/to/your/data.csv")

df.show(5)
```

### Step 3: Writing a DataFrame to a CSV File 📤

Writing to a CSV is equally straightforward with `df.write.csv()`, and PySpark gives you several options for customizing how the output is saved.

#### Basic CSV Writing

Here’s the simplest way to save a DataFrame as a CSV:

```python
# Basic CSV write
df.write.csv("/path/to/output_data")
```

#### Common CSV Writing Options

- **Header**: Set `header=True` to include column names in the first row.
- **Mode**: Controls the behavior if the output file or directory already exists.
  - `overwrite`: Replaces existing data.
  - `append`: Adds data to the existing file.
  - `ignore`: Skips writing if the file exists.
  - `error` or `errorifexists`: Throws an error if the file exists.

Example of writing a CSV with header and mode options:

```python
# Writing CSV with options
df.write.option("header", True) \
        .mode("overwrite") \
        .csv("/path/to/output_data")
```

#### Additional Writing Options

- **Delimiter**: Set `sep` to specify a custom delimiter.
- **Compression**: Choose a compression method such as `gzip`, `bzip2`, `lz4`, `snappy`, or `deflate`.
  
Example with custom delimiter and compression:

```python
# Writing CSV with delimiter and compression
df.write.option("header", True) \
        .option("sep", ";") \
        .option("compression", "gzip") \
        .csv("/path/to/compressed_data")
```

### Wrapping Up: Why Use PySpark for CSV Files?

PySpark’s CSV handling options let you quickly adjust settings for large-scale data reads and writes, whether you need headers, delimiters, schema inference, or data compression.