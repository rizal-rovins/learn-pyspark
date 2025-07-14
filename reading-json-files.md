JSON files are a popular choice for structured data. PySpark offers flexible methods to read from and write to JSON files, with various options to handle different data structures, formatting, and file organization needs. Let's dive into how to work with JSON files in PySpark.

### Practice Question
Read the tutorial below and try solving this problem to get hands-on practice [here](../pyspark-coding-interview-questions/load-and-transform-data-json).

### Step 1: Start with a `SparkSession`

As always, begin by creating a `SparkSession`. This is your gateway to all PySpark functions.

```python
from pyspark.sql import SparkSession

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("JSON Handling in PySpark") \
    .getOrCreate()

print("Spark is ready to handle JSON files! 🚀")
```

### Step 2: Reading a JSON File 📥

To read a JSON file, use `spark.read.json()`. PySpark allows you to configure multiple options to manage JSON structures, handling everything from multi-line formatting to schema inference.

#### Basic JSON Reading

For basic JSON files, simply specify the file path:

```python
# Basic JSON read
df = spark.read.json("/path/to/your/data.json")
df.show(5)
```

#### Common JSON Reading Options

PySpark allows several options to manage common JSON features:

- **Multiline**: Set `multiline=True` if each JSON record is spread across multiple lines.
- **InferSchema**: JSON files often include structured data, so PySpark will infer the schema automatically. You can manually specify schema if needed.
- **SamplingRatio**: When inferring schema, PySpark reads a sample of the data to determine types. `samplingRatio` allows you to set the fraction of rows to sample (between `0` and `1`).

Example of reading JSON with these options:

```python
# Reading a JSON with multiline and schema sampling
df = spark.read.option("multiline", True) \
               .option("samplingRatio", 0.5) \
               .json("/path/to/your/multiline_data.json")

df.show(5)
```

#### Additional Reading Options

- **Mode**: Determines behavior for corrupt or malformed JSON data. Options include:
  - `PERMISSIVE` (default): Allows malformed records, placing them in a `_corrupt_record` column.
  - `DROPMALFORMED`: Drops records with corrupt JSON.
  - `FAILFAST`: Stops reading and throws an error on the first corrupt record.

Example using the mode option:

```python
# Handling corrupt JSON records
df = spark.read.option("mode", "FAILFAST") \
               .json("/path/to/your/data.json")

df.show(5)
```

### Step 3: Writing a DataFrame to a JSON File 📤

Writing a DataFrame to JSON is straightforward with `df.write.json()`. PySpark provides several options for customizing how JSON data is saved, allowing you to control things like file compression and data partitioning.

#### Basic JSON Writing

Here’s the simplest way to save a DataFrame as a JSON file:

```python
# Basic JSON write
df.write.json("/path/to/output_data")
```

#### Common JSON Writing Options

- **Mode**: Controls how PySpark handles files that already exist at the specified path. Options include:
  - `overwrite`: Replaces existing files.
  - `append`: Appends data to existing files.
  - `ignore`: Skips writing if files exist.
  - `error` or `errorifexists`: Throws an error if files exist.

- **Compression**: PySpark allows you to compress JSON output with methods like `gzip`, `bzip2`, `lz4`, `snappy`, and `deflate`.

Example using mode and compression:

```python
# Writing JSON with overwrite mode and gzip compression
df.write.mode("overwrite") \
        .option("compression", "gzip") \
        .json("/path/to/compressed_output")
```

#### Partitioning Output Files

For large JSON data, partitioning output files by specific columns can be useful. Partitioning splits data based on column values, creating subfolders named after each partitioned column, making large datasets easier to manage and access.

Example of partitioned JSON writing:

```python
# Writing JSON with partitioning by year
df.write.option("header", True) \
        .partitionBy("year") \
        .json("/path/to/partitioned_output")
```

### Wrapping Up: Why Use PySpark for JSON Files?

PySpark provides flexible options for working with JSON files, making it easy to load complex structured data, handle various file formats, and save data in an organized, efficient way. Whether you’re handling multiline JSON, compressing files, or partitioning data, PySpark simplifies the process, even with large datasets.