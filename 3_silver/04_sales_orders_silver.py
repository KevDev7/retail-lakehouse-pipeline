# Databricks notebook source
# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# Read bronze sales_order
df_sales_orders_bronze = spark.table("retail_project.bronze.sales_orders")

# COMMAND ----------

display(df_sales_orders_bronze.limit(10))
df_sales_orders_bronze.printSchema()

# COMMAND ----------

# Validate
(
    df_sales_orders_bronze
    .groupBy("order_number")
    .count()
    .orderBy(F.desc("count"))
    .limit(10)
    .display()
)


# COMMAND ----------

# Discovery check: nulls & data quality
df_sales_orders_bronze.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_sales_orders_bronze.columns
]).display()


# COMMAND ----------

# Count empty order_datetime values
(
    df_sales_orders_bronze
    .filter(F.trim(F.col("order_datetime")) == "")
    .count()
)

# COMMAND ----------

display(
    df_sales_orders_bronze
    .filter(F.trim(F.col("order_datetime")) == "")
    .limit(20)
)

# COMMAND ----------


df_sales_orders_with_ts = (
    df_sales_orders_bronze
    .withColumn(
        "order_timestamp",
        F.to_timestamp(
            F.from_unixtime(
                F.when(
                    F.trim(F.col("order_datetime")) == "",
                    None
                ).otherwise(
                    F.col("order_datetime").cast("long")
                )
            )
        )
    )
)

# COMMAND ----------

# Create sales_order_key
# sales_order_key = order_number + '#' + order_timestamp
df_sales_orders_with_key = (
    df_sales_orders_with_ts
    .withColumn(
        "sales_order_key",
        F.concat_ws(
            "#",
            F.col("order_number"),
            F.date_format(F.col("order_timestamp"), "yyyyMMddHHmmss")
        )
    )
)


# COMMAND ----------

# Create number_of_line_items
df_sales_orders_with_line_items = (
    df_sales_orders_with_key
    .withColumn(
        "number_of_line_items",
        F.size(F.col("order_products"))
    )
)


# COMMAND ----------

df_sales_orders_silver = (
    df_sales_orders_with_key
    .select(
        # added PK
        F.col("sales_order_key").cast("string"),

        F.col("clicked_items"),
        F.col("customer_id").cast("string"),
        F.col("customer_name").cast("string"),
        
        # number_of_line_items (added)
        F.col("number_of_line_items").cast("int"),

        F.col("ordered_products"),
        F.col("promo_info"),

        F.col("order_number").cast("bigint"),
        F.col("order_datetime").cast("string"),

        F.col("_read_timestamp").alias("bronze_read_timestamp"),
    )
)


# COMMAND ----------

df_sales_orders_silver.groupBy("sales_order_key").count().filter("count > 1").display()

# COMMAND ----------

(
    df_sales_orders_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("retail_project.silver.sales_orders")
)

# COMMAND ----------

spark.table("retail_project.silver.sales_orders").display()
