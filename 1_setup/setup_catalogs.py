# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS retail_project;
# MAGIC USE CATALOG retail_project;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_project.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_project.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_project.gold;