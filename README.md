# Retail Lakehouse Pipeline

End‑to‑end retail analytics pipeline built on the Databricks Lakehouse Platform using Apache Spark and the Medallion (Bronze–Silver–Gold) architecture.  
It ingests sample retail data from `databricks-datasets/retail-org`, refines it through bronze and silver layers, and exposes analytics‑ready gold tables.

## Overview

This project demonstrates how to design and implement a modern lakehouse‑style data platform for a retail organization:
- Ingest raw sales, customer, product, and related data into a **Bronze** layer.
- Clean, conform, and enrich data in a **Silver** layer.
- Publish star‑schema **Gold** tables (facts and dimensions) for BI and data science.

All transformations run on **Apache Spark** inside **Databricks**, leveraging Delta Lake and Unity Catalog (via catalog/schema organization).

## Architecture

The pipeline follows the **Medallion Architecture** on a **Lakehouse**:
- **Bronze** – Raw, minimally processed ingestions from `dbfs:/databricks-datasets/retail-org/*` into `retail_project.bronze.*` tables.
- **Silver** – Standardized, cleansed, and conformed data in `retail_project.silver.*`.
- **Gold** – Analytics‑ready dimensional model in `retail_project.gold.*` (e.g. `dim_product`, `dim_customer`, `fact_sales_order_item`).

You can run notebooks manually, orchestrate them with Databricks Workflows, or integrate into a broader CI/CD process.

## Tech Stack & Concepts

- **Platform**: Databricks Lakehouse Platform
- **Processing engine**: Apache Spark / PySpark
- **Storage & tables**: Delta Lake on DBFS / Unity Catalog
- **Design pattern**: Medallion architecture (Bronze–Silver–Gold)
- **Language**: Python + SQL (in notebooks)

## Repository Structure

- `0_exploration/`
  - `source_exploration.ipynb` – Explore `databricks-datasets/retail-org` sources, inspect schemas, and derive source contracts.  
    This is exploratory only and not part of the production pipeline.
- `1_setup/`
  - `setup_catalogs.ipynb` – Creates the `retail_project` catalog and the `bronze`, `silver`, and `gold` schemas:
    - `CREATE CATALOG IF NOT EXISTS retail_project;`
    - `CREATE SCHEMA IF NOT EXISTS retail_project.bronze;`
    - `CREATE SCHEMA IF NOT EXISTS retail_project.silver;`
    - `CREATE SCHEMA IF NOT EXISTS retail_project.gold;`
  - `source_contracts.ipynb` – (Placeholder) for documenting and enforcing ingestion/source contracts.
- `2_bronze/`
  - `01_customers_bronze.ipynb` – Ingest customers.
  - `02_products_bronze.ipynb` – Ingest products.
  - `03_suppliers_bronze.ipynb` – Ingest suppliers.
  - `04_sales_orders_bronze.ipynb` – Ingest sales orders from `dbfs:/databricks-datasets/retail-org/sales_orders/` into `retail_project.bronze.sales_orders`.
  - `05_purchase_orders_bronze.ipynb` – Ingest purchase orders.
  - `06_promotions_bronze.ipynb` – Ingest promotions.
  - `07_active_promotions_bronze.ipynb` – Ingest active promotions.
  - `08_loyalty_segments_bronze.ipynb` – Ingest loyalty segments.
  - `09_company_employees_bronze.ipynb` – Ingest company employees.
- `3_silver/`
  - `01_products_silver.ipynb` – Curate product attributes from bronze.
  - `02_customers_silver.ipynb` – Curate customer attributes from bronze.
  - `03_loyalty_segments_silver.ipynb` – Curate loyalty segment data.
  - `04_sales_orders_silver.ipynb` – Curate sales orders (clean, conform, and join as needed).
- `4_gold/`
  - `01_dim_product_gold.ipynb` – Build `retail_project.gold.dim_product`.
  - `02_dim_customer_gold.ipynb` – Build `retail_project.gold.dim_customer`.
  - `03_fact_sales_order_item_gold.ipynb` – Build `retail_project.gold.fact_sales_order_item`.

## Prerequisites

- A Databricks workspace with:
  - Access to `dbfs:/databricks-datasets/retail-org` (Databricks sample datasets).
  - Permission to create catalogs and schemas (Unity Catalog or Hive metastore, depending on your setup).
- A running Databricks cluster with:
  - A supported Databricks Runtime (e.g. DBR 13.x+ with Delta Lake).
  - Python and SQL enabled (standard Databricks runtime).

If you use a different catalog name than `retail_project`, you can update it in `1_setup/setup_catalogs.ipynb` and downstream notebooks.

## Quickstart

For experienced Databricks users, the minimal flow is:

1. Import this repository into Databricks Repos or workspace.
2. Attach a cluster to the notebooks.
3. Run `1_setup/setup_catalogs.ipynb` to create the catalog and schemas.
4. Run all notebooks in `2_bronze/`, then `3_silver/`, then `4_gold/` in order.
5. Query the gold tables (e.g. `SELECT * FROM retail_project.gold.fact_sales_order_item LIMIT 100;`).

## Getting Started (Step‑by‑Step)

1. **Clone or import the project**
   - Use Databricks Repos or workspace import to add this repo under your workspace.
2. **Attach a cluster**
   - Attach a compatible cluster (with access to `databricks-datasets`) to all notebooks in the repo.
3. **Run setup**
   - Open and run `1_setup/setup_catalogs.ipynb` to create the `retail_project` catalog and the `bronze`, `silver`, and `gold` schemas.
4. **(Optional) Explore sources**
   - Open `0_exploration/source_exploration.ipynb` to profile the raw data and review inferred source contracts.
5. **Run Bronze layer**
   - In `2_bronze/`, run the notebooks in numeric order (`01_*` → `09_*`).
   - This reads from `dbfs:/databricks-datasets/retail-org` and writes raw Delta tables into `retail_project.bronze.*`.
6. **Run Silver layer**
   - In `3_silver/`, run notebooks `01_*` → `04_*`.
   - These read from `retail_project.bronze.*` and create curated tables in `retail_project.silver.*`.
7. **Run Gold layer**
   - In `4_gold/`, run `01_dim_product_gold.ipynb`, `02_dim_customer_gold.ipynb`, then `03_fact_sales_order_item_gold.ipynb`.
   - These produce analytics‑ready `retail_project.gold.*` tables.

## Data Model (Gold Layer)

Key gold tables and their intended use:

- `retail_project.gold.dim_product`
  - **Grain**: One row per unique product.
  - **Contains**: Product identifiers, names, categories, brands, and other descriptive attributes.
  - **Use cases**: Product‑level slicing of revenue, margin, and promotion performance.

- `retail_project.gold.dim_customer`
  - **Grain**: One row per unique customer.
  - **Contains**: Customer identifiers, demographic attributes (where available), loyalty segment information.
  - **Use cases**: Customer segmentation, cohort analysis, CLV‑style reporting.

- `retail_project.gold.fact_sales_order_item`
  - **Grain**: One row per sales order line item.
  - **Contains**: Foreign keys to product and customer dimensions, order identifiers, quantities, prices, discounts, and derived revenue measures.
  - **Use cases**: Core retail analytics, including sales performance, product mix analysis, and promotion impact.
