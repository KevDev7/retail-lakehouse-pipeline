# Databricks notebook source
dbutils.fs.ls("/databricks-datasets/retail-org/")

# COMMAND ----------

spark.read.text("/databricks-datasets/retail-org/README.md").show(truncate=False)
