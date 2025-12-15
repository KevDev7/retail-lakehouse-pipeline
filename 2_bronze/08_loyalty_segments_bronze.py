# Databricks notebook source
# Bronze ingestion config
SOURCE_PATH = "dbfs:/databricks-datasets/retail-org/loyalty_segments/"
TARGET_TABLE = "retail_project.bronze.loyalty_segments"

# COMMAND ----------

# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# Detect file format (standardized, Spark-safe)

files = dbutils.fs.ls(SOURCE_PATH)

# Ignore Spark metadata files and directories
data_files = [
    f.name.lower()
    for f in files
    if not f.name.startswith("_") and "." in f.name
]

if not data_files:
    raise ValueError(f"No data files found under {SOURCE_PATH}")

# Collect unique file extensions
extensions = {name.split(".")[-1] for name in data_files}

# Enforce single-format sources
if len(extensions) != 1:
    raise ValueError(
        f"Mixed or unsupported file types under {SOURCE_PATH}: {extensions}"
    )

FILE_FORMAT = extensions.pop()

# Allow only known formats
if FILE_FORMAT not in {"parquet", "csv", "json", "xml"}:
    raise ValueError(
        f"Unsupported file format '{FILE_FORMAT}' under {SOURCE_PATH}"
    )

print("Detected format:", FILE_FORMAT)

# COMMAND ----------

# Read raw CSV data
reader = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("mode", "PERMISSIVE")
)

df_raw = reader.load(SOURCE_PATH)

# Bronze enrichment (standard)
df_bronze = (
    df_raw
    .withColumn("_read_timestamp", F.current_timestamp())
    .withColumn("_source_path", F.col("_metadata.file_path"))
    .withColumn("_file_size", F.col("_metadata.file_size"))
)

display(df_bronze.limit(10))
df_bronze.printSchema()

# COMMAND ----------

# Write to Delta Bronze table
(
    df_bronze.write
        .format("delta")
        .option("overwriteSchema", "true")  # Bronze schema is authoritative
        .mode("overwrite")                  # Full refresh
        .saveAsTable(TARGET_TABLE)
)

print(f"Wrote Bronze table: {TARGET_TABLE}")

# COMMAND ----------

# Quick validation
spark.sql(f"SELECT COUNT(*) AS row_count FROM {TARGET_TABLE}").show()