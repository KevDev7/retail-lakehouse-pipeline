# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# Tables
SALES_ORDERS_SILVER = "retail_project.silver.sales_orders"
DIM_CUSTOMER_GOLD = "retail_project.gold.dim_customer"
FACT_SALES_ORDER_ITEM_GOLD = "retail_project.gold.fact_sales_order_item"


# COMMAND ----------

# Read Silver Sales Orders
df_sales_orders = spark.table(SALES_ORDERS_SILVER)
display(df_sales_orders.limit(5))


# COMMAND ----------

# Explode ordered_products (Preserve Array Index)
df_exploded = (
    df_sales_orders
    .select(
        F.col("sales_order_key"),
        F.col("order_number").alias("sales_order_number"),
        F.col("order_datetime"),
        F.col("customer_id"),
        F.posexplode("ordered_products").alias(
            "sales_order_item_seq",
            "ordered_product"
        )
    )
)


# COMMAND ----------

# test
df_exploded.printSchema()

# COMMAND ----------

# Extract Line-Item Fields
df_items = df_exploded.select(
    F.col("sales_order_key"),
    F.col("sales_order_number"),
    F.col("order_datetime"),
    F.col("customer_id"),  
    F.col("sales_order_item_seq"),

    # product_key comes directly from the event payload
    F.col("ordered_product.id")
        .cast("string")
        .alias("product_key"),

    F.col("ordered_product.price")
        .cast("decimal(18,2)")
        .alias("price"),

    F.col("ordered_product.curr")
        .cast("string")
        .alias("currency"),

    F.col("ordered_product.qty")
        .cast("integer")
        .alias("qty")
)


# COMMAND ----------

# test
df_items.printSchema()
display(df_items.limit(10))

# COMMAND ----------

# Create sales_order_item_key
df_fact_sales_order_item = (
    df_items
    .withColumn(
        "sales_order_item_key",
        F.concat_ws(
            "-",
            F.col("sales_order_key"),
            F.col("sales_order_item_seq").cast("string")
        )
    )
)

# COMMAND ----------

# test
df_fact_sales_order_item.select(
    "sales_order_key",
    "sales_order_item_seq",
    "sales_order_item_key"
).display()

# COMMAND ----------

# Join to dim_customer to Get customer_key
# Assumption: one active customer record per customer_id.
df_dim_customer = spark.table(DIM_CUSTOMER_GOLD).select(
    "customer_id",
    "customer_key"
)

df_fact_with_customer = (
    df_fact_sales_order_item
    .join(
        df_dim_customer,
        on="customer_id",
        how="left"
    )
)


# COMMAND ----------

# test
df_fact_with_customer.printSchema()
display(
    df_fact_with_customer.select(
        "customer_id",
        "customer_key"
    ).limit(10)
)

# COMMAND ----------

# Final Column Order (Explicit)
df_fact_sales_order_item_final = df_fact_with_customer.select(
    "sales_order_item_key",
    "sales_order_number",
    "order_datetime",
    "customer_key",
    "product_key",
    "price",
    "currency",
    "qty"
)

# COMMAND ----------

# test
df_fact_sales_order_item_final.printSchema()

# COMMAND ----------

# Uniqueness check on the fact PK.
(
    df_fact_sales_order_item_final
    .groupBy("sales_order_item_key")
    .count()
    .filter("count > 1")
    .display()
)

# COMMAND ----------

# row count check
df_fact_sales_order_item_final.count()

# COMMAND ----------

# Write Gold Fact Table
(
    df_fact_sales_order_item_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FACT_SALES_ORDER_ITEM_GOLD)
)


# COMMAND ----------

# Validate
spark.table(FACT_SALES_ORDER_ITEM_GOLD).display()