# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# Tables
PRODUCTS_SILVER = "retail_project.silver.products"
DIM_PRODUCT_GOLD = "retail_project.gold.dim_product"

# COMMAND ----------

# Read silver products
df_products = spark.table(PRODUCTS_SILVER)
display(df_products.limit(5))

# COMMAND ----------

df_products_sel = df_products.select(
    F.col("product_key").cast("string"),
    F.col("product_id").cast("string"),
    F.col("product_name").cast("string"),
    F.col("product_category").cast("string"),
    F.col("product_unit").cast("string")
)


# COMMAND ----------

df_dim_product = df_products_sel.select(
    "product_key",       # PK
    "product_id",
    "product_name",
    "product_category",
    "product_unit"
)


# COMMAND ----------

df_dim_product.groupBy("product_key").count().filter("count > 1").display()


# COMMAND ----------

(
    df_dim_product
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_PRODUCT_GOLD)
)



# COMMAND ----------

spark.table(DIM_PRODUCT_GOLD).display()
