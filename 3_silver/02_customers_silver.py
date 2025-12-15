# Databricks notebook source
# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# Read Bronze customers
df_customers_bronze = spark.table("retail_project.bronze.customers")

# COMMAND ----------

display(df_customers_bronze.limit(10))
df_customers_bronze.printSchema()

# COMMAND ----------

# validate customer_id
(
    df_customers_bronze
    .groupBy("customer_id")
    .count()
    .orderBy(F.desc("count"))
    .limit(10)
    .display()
)

# COMMAND ----------

# Discovery check: nulls & data quality
df_customers_bronze.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_customers_bronze.columns
]).display()

# COMMAND ----------

# Create customer_key as new PK
# customer_key = customer_id + '#' + valid_from
df_customers_with_key = (
    df_customers_bronze
    .withColumn(
        "customer_key",
        F.concat_ws(
            "#",
            F.col("customer_id"),
            F.col("valid_from").cast("string")
        )
    )
)

# COMMAND ----------

# df_customers_deduped = df_customers_bronze.dropDuplicates(["customer_id"])


# COMMAND ----------

# Silver cleaning & standardization
df_customers_silver = (
    df_customers_with_key
    .select(
        # Added PK: customer_key
        F.col("customer_key").cast("string"),

        F.col("valid_to"),
        F.col("tax_id").cast("string"),
        F.col("tax_code").cast("string"),
        F.col("customer_name").cast("string"),
        F.col("ship_to_address").cast("string"),

        # changed type from double -> int
        F.col("units_purchased").cast("int"),

        F.col("loyalty_segment").cast("string"),

        F.col("customer_id").cast("string"),
        F.col("_read_timestamp").alias("bronze_read_timestamp")
    )
)

# COMMAND ----------

# Check product_id uniqueness
df_customers_silver.groupBy("customer_key").count().filter("count > 1").display()


# COMMAND ----------

(
    df_customers_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("retail_project.silver.customers")
)


# COMMAND ----------

spark.table("retail_project.silver.customers").display()
