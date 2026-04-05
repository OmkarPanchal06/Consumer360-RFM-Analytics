# SQL Analytical Database Schema

This directory contains the foundational DDL scripts for provisioning the Consumer360 testing database.

## Architecture

The underlying structure follows a strictly normalized **Star Schema** designed specifically for high-performance read aggregations typical in OLAP and Data Warehousing environments.

* **Fact Table (`Fact_Sales`)**
  * Holds the granular, atomic measurements of customer purchases. This table is keyed to dimension tables to optimize querying.
* **Dimension Tables**
  * `Dim_Customer`
  * `Dim_Product`
  * `Dim_Date`
  * `Dim_Region`

These dimension tables contain descriptive attributes allowing slicing and dicing of the Fact table metrics.

## Scripts

1. `01_create_schema.sql`: Boots up the database and constructs the table architecture.
2. `02_populate_dates.sql`: A utility for dynamically generating a 10-year dimension table calendar map.
3. `03_customer_360_view.sql`: Creates a robust SQL View joining fact and dimension tables securely for analytical queries.
4. `04_sample_data.sql`: (Deprecated/Optional) Populates the tables with randomized synthetic data for infrastructure validation without the main ETL job.
