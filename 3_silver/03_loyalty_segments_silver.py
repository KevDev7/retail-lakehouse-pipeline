# Databricks notebook source
# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# Read Bronze customers
df_loyalty_segments_bronze = spark.table("retail_project.bronze.loyalty_segments")

# COMMAND ----------

display(df_loyalty_segments_bronze.limit(10))
df_loyalty_segments_bronze.printSchema()

# COMMAND ----------

# validate loyalty_segment_id
(
    df_loyalty_segments_bronze
    .groupBy("loyalty_segment_id")
    .count()
    .filter("count > 1")
    .display()
)

# COMMAND ----------

# Discovery check: nulls & data quality
df_loyalty_segments_bronze.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_loyalty_segments_bronze.columns
]).display()

# COMMAND ----------

df_loyalty_segments_deduped = df_loyalty_segments_bronze.dropDuplicates(["loyalty_segment_id"])


# COMMAND ----------

# Silver cleaning & standardization
df_loyalty_segments_silver = (
    df_loyalty_segments_deduped
    .select(
        F.col("loyalty_segment_id").cast("int"),
        F.col("loyalty_segment_description").cast("string"),
        F.col("unit_threshold").cast("int"),
        F.col("valid_from").cast("date"),
        F.col("valid_to").cast("date"),
        F.col("_read_timestamp").alias("bronze_read_timestamp")
    )
)


# COMMAND ----------

df_loyalty_segments_silver.display()

# COMMAND ----------

(
    df_loyalty_segments_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("retail_project.silver.loyalty_segments")
)

# COMMAND ----------

spark.table("retail_project.silver.loyalty_segments").display()