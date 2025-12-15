# Databricks notebook source
# MAGIC %md
# MAGIC # Source Exploration & Ingestion Contracts
# MAGIC
# MAGIC This notebook explores each raw data source under `databricks-datasets/retail-org`
# MAGIC to determine:
# MAGIC - file format
# MAGIC - parsing requirements
# MAGIC - structural characteristics
# MAGIC
# MAGIC The final outcome of this notebook is an explicit **source contract**
# MAGIC that will be referenced by Bronze ingestion notebooks.
# MAGIC
# MAGIC This notebook is **not** executed as part of the pipeline.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

BASE_PATH = "dbfs:/databricks-datasets/retail-org"

sources = dbutils.fs.ls(BASE_PATH)
for s in sources:
    print(s.name, s.path)

# COMMAND ----------

SALES_ORDERS_PATH = f"{BASE_PATH}/sales_orders"

dbutils.fs.ls(SALES_ORDERS_PATH)

# COMMAND ----------

df_sales = spark.read.load(SALES_ORDERS_PATH)
df_sales.printSchema()
display(df_sales.limit(5))

# COMMAND ----------

