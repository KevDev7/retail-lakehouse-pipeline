# Databricks notebook source
# Imports
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# Tables
CUSTOMERS_SILVER = "retail_project.silver.customers"
LOYALTY_SILVER = "retail_project.silver.loyalty_segments"
DIM_CUSTOMER_GOLD = "retail_project.gold.dim_customer"


# COMMAND ----------

# Read silver tables
df_customers = spark.table(CUSTOMERS_SILVER)
df_loyalty = spark.table(LOYALTY_SILVER)
display(df_customers.limit(5))
display(df_loyalty.limit(5))



# COMMAND ----------

# Select & Standardize Customer Columns
df_customers_sel = df_customers.select(
    F.col("customer_key").cast("string"),
    F.col("customer_id").cast("string"),
    F.col("customer_name").cast("string"),
    F.col("tax_id").cast("string"),
    F.col("tax_code").cast("string"),
    F.col("loyalty_segment").alias("loyalty_segment_id").cast("string"),
    F.col("ship_to_address").cast("string")
)

# COMMAND ----------

# Prepare Loyalty Segments Lookup
df_loyalty_sel = df_loyalty.select(
    F.col("loyalty_segment_id").cast("string"),
    F.col("loyalty_segment_description").cast("string")
)

# COMMAND ----------

# Join Customers → Loyalty Segments
df_dim_customer = (
    df_customers_sel
    .join(
        df_loyalty_sel,
        on="loyalty_segment_id",
        how="left"
    )
)


# COMMAND ----------

# Final Column Order
df_dim_customer_final = df_dim_customer.select(
    "customer_key",                  # PK
    "customer_id",
    "customer_name",
    "tax_id",
    "tax_code",
    "loyalty_segment_id",
    "loyalty_segment_description",
    "ship_to_address"
)


# COMMAND ----------

# Duplicate check
df_dim_customer_final.groupBy("customer_key").count().filter("count > 1").display()


# COMMAND ----------

# Overwrite gold table
(
    df_dim_customer_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_CUSTOMER_GOLD)
)


# COMMAND ----------

# Validation
spark.table(DIM_CUSTOMER_GOLD).display()
