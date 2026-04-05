# Python ETL & Analytics Engine

This directory contains the core orchestration scripts for the Consumer360 data pipeline. It leverages Pandas for fast, in-memory transformations and SQLAlchemy for transactional loading into SQL Server.

## Scripts Overview

* `config.py`
  * centralized configuration map for environment variables and ODBC drivers.
* `import_real_data.py`
  * Handles the ingestion of external retail CSV files into the SQL Server schema, mapping unstructured data to the Star Schema format.
* `main_pipeline.py`
  * The primary execution file. It orchestrates all the individual calculator modules sequentially.
* `data_extraction.py`
  * Selects historical transactions from the `Fact_Sales` and Dimension tables.
* `clv_calculator.py`
  * Predicts the remaining lifetime value (5-Year outlook) for a customer based on historical ticket sizes and transaction frequencies.
* `rfm_calculator.py`
  * Bins and scores users into Recency, Frequency, and Monetary quintiles to segment them into actionable cohorts like "Champions" or "At Risk".
* `cohort_analysis.py`
  * Tracks and computes month-over-month engagement retention.
* `market_basket_analysis.py`
  * Utilizes mlxtend's implementation of the Apriori algorithm to define associative rules between varying products.
* `generate_presentation.py` & `generate_dashboard.py`
  * Extracts the enriched analytics outputs to construct visual `.pptx` decks and HTML dashboards for stakeholders.
