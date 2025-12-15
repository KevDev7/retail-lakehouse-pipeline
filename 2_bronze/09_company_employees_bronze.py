# Databricks notebook source
# Bronze ingestion config
SOURCE_PATH = "dbfs:/databricks-datasets/retail-org/company_employees/"
TARGET_TABLE = "retail_project.bronze.company_employees"

# COMMAND ----------

# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# Detect file format
files = dbutils.fs.ls(SOURCE_PATH)
file_names = [f.name for f in files if not f.name.endswith("/")]

if not file_names:
    raise ValueError(f"No files found under {SOURCE_PATH}")

# pick a representative file
sample = file_names[0].lower()

if sample.endswith(".parquet"):
    FILE_FORMAT = "parquet"
elif sample.endswith(".csv"):
    FILE_FORMAT = "csv"
elif sample.endswith(".json"):
    FILE_FORMAT = "json"
else:
    raise ValueError(f"Unsupported/unknown file type in {SOURCE_PATH}: {sample}")

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