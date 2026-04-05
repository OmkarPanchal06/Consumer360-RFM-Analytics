# Consumer360: Customer Segmentation & ETL Pipeline

Consumer360 is an end-to-end data engineering and analytics pipeline. It processes raw retail transaction data to calculate actionable metrics like Customer Lifetime Value (CLV), RFM (Recency, Frequency, Monetary) segmentation, Market Basket associations, and Retention Cohorts.

## Architecture & Data Flow

The project follows a standard batch processing architecture:
1. **Extraction**: Raw transaction data is read from CSV files (e.g. Kaggle datasets) and inserted into a local SQL Server testing database.
2. **Transformation**: Analytical logic is written in Python using Pandas to pull from SQL Server, calculate derived segmentation metrics, and map relationships.
3. **Load / Export**: Enriched customer groupings and cohort analytics are written back as new tables into SQL Server.
4. **Visualization**: An automated Plotly script reads this output to generate static HTML dashboards and an Executive PowerPoint summarizing churn risks.

## Technology Stack

* **Database**: SQL Server 2019+ (Star Schema design)
* **Processing**: Python 3.9+ (Pandas, SQLAlchemy, mlxtend)
* **Automation**: Windows Task Scheduler batch scripts
* **Visualization**: Plotly, Python-PPTX, Power BI Desktop (Optional)

## Local Setup

### Prerequisites
* SQL Server installed locally (with Windows Authentication)
* Python 3.9+

### Installation & Execution
1. Clone the repository and navigate into the directory.
2. Initialize and activate a Python virtual environment.
3. Install the required packages via `pip install -r requirements.txt`.
4. Configure your `.env` file with the server credentials. Run the baseline scripts in the `sql/` directory to create the tables.
5. In your command line, run the pipeline execution file: `python python/main_pipeline.py`.
6. Open the `powerbi/Consumer360_Dashboard.html` file in your browser to view the interactive insights, or open the `.pptx` deck.

## Analytical Logic Explained

### RFM Segmentation
RFM is a proven marketing model for behavior-based customer segmentation.
* **Recency**: Days since the last purchase.
* **Frequency**: Total transaction count.
* **Monetary**: Total spend across all transactions.

### Market Basket Analysis
Used exclusively on order-line transactions to find patterns. Using the Apriori algorithm, the pipeline links items frequently bought together and generates support, confidence, and metric rules.

### Cohort Analysis
Grouping customers based on their acquisition month and calculating percentage retention over subsequent months to identify lifetime drop-off points.

## Support & Maintenance

All backend execution logic outputs `.log` files to the `logs/` directory for runtime visibility. The pipeline is designed around incremental or batch processing execution which can be easily migrated to managed cron orchestration platforms like Apache Airflow in production.

## License

MIT License. See LICENSE for details.

## Maintainer

Omkar Panchal - GitHub: [@OmkarPanchal06](https://github.com/OmkarPanchal06)
