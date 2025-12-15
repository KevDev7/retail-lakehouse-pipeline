# Databricks notebook source
# Imports
from pyspark.sql import functions as F


# COMMAND ----------

# Read Bronze products
df_products_bronze = spark.table("retail_project.bronze.products")

# COMMAND ----------

display(df_products_bronze.limit(10))
df_products_bronze.printSchema()

# COMMAND ----------

# validate product_id
(
    df_products_bronze
    .groupBy("product_id")
    .count()
    .orderBy(F.desc("count"))
    .limit(10)
    .display()
)

# COMMAND ----------

# validate product_unit
(
    df_products_bronze
    .groupBy("product_unit")
    .count()
    .orderBy(F.desc("count"))
    .limit(10)
    .display()
)

# COMMAND ----------

# Discovery check: nulls & data quality
df_products_bronze.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_products_bronze.columns
]).display()

# COMMAND ----------

# None in backfill but just in case in future
df_products_deduped = df_products_bronze.dropDuplicates(["product_id"])

# COMMAND ----------

# Silver cleaning & standardization
df_products_silver = (
    df_products_deduped
    .select(
        F.col("product_id").cast("string"),
        F.col("product_category").cast("string"),
        F.col("product_name").cast("string"),

        # round + fix type
        F.round(F.col("sales_price"), 2)
            .cast("decimal(18,2)")
            .alias("sales_price"),

        F.col("EAN13").cast("long"),
        F.col("EAN5").cast("integer"),
        F.col("product_unit").cast("string"),
        F.col("_read_timestamp").alias("bronze_read_timestamp")
    )
)


# COMMAND ----------

# Create product_key (added business key)
df_products_silver = (
    df_products_silver
    .withColumn(
        "product_key",
        F.col("product_id")
    )
)

# COMMAND ----------

# column reordering
df_products_silver = df_products_silver.select(
    "product_key",
    "product_id",
    "product_category",
    "product_name",
    "sales_price",
    "EAN13",
    "EAN5",
    "product_unit",
    "bronze_read_timestamp"
)


# COMMAND ----------

# Check product_id uniqueness
df_products_bronze.groupBy("product_id").count().filter("count > 1").display()


# COMMAND ----------

(
    df_products_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("retail_project.silver.products")
)


# COMMAND ----------

spark.table("retail_project.silver.products").display()