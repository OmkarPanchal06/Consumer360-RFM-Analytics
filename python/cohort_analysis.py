"""
Cohort Analysis Module

This module tracks and analyzes customer behavior through acquisition cohorts. 
It enables the business to understand retention rates and spending patterns 
based on the customer's initial registration period.
"""

import pandas as pd
import numpy as np
import logging
import os
import sys

# Add python folder to path so imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import LOGS_FOLDER, CONNECTION_STRING
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, 'cohort_analysis.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CohortAnalyzer:
    """
    Handles customer grouping and retention analysis across acquisition cohorts.
    """
    
    def __init__(self):
        """
        Initializes the cohort analyzer with the required database engine.
        """
        self.engine = create_engine(CONNECTION_STRING)
        logger.info("Cohort Analyzer successfully initialized.")
    
    def extract_cohort_data(self):
        """
        Extracts multi-dimensional customer data relative to their registration date.

        Returns:
            pd.DataFrame: A DataFrame containing cohort-level metrics for each customer.
        """
        logger.info("Extracting transactional and demographic data for cohort modeling...")
        
        query = """
        SELECT 
            c.CustomerKey,
            c.CustomerID,
            c.RegistrationDate,
            YEAR(c.RegistrationDate) AS CohortYear,
            DATEPART(QUARTER, c.RegistrationDate) AS CohortQuarter,
            FORMAT(c.RegistrationDate, 'yyyy-MM') AS CohortMonth,
            
            MAX(fs.TransactionDate) AS LastPurchaseDate,
            COUNT(DISTINCT fs.SalesID) AS TransactionCount,
            SUM(CASE WHEN fs.ReturnFlag = 'N' THEN fs.NetSalesAmount ELSE 0 END) AS TotalSpend,
            
            DATEDIFF(MONTH, c.RegistrationDate, MAX(fs.TransactionDate)) AS MonthsSinceCohort
        
        FROM Dim_Customer c
        LEFT JOIN Fact_Sales fs ON c.CustomerKey = fs.CustomerKey
        WHERE c.IsCurrentRecord = 1
        GROUP BY 
            c.CustomerKey, c.CustomerID, c.RegistrationDate,
            YEAR(c.RegistrationDate), DATEPART(QUARTER, c.RegistrationDate),
            FORMAT(c.RegistrationDate, 'yyyy-MM')
        """
        
        try:
            df = pd.read_sql_query(query, self.engine)
            logger.info(f"Successfully extracted {len(df):,} customer records for cohort modeling.")
            return df
        except Exception as e:
            logger.error(f"Cohort data extraction failed: {e}")
            raise
    
    def create_cohort_table(self, df):
        """
        Generates pivot tables for cohort retention and spending metrics.

        Args:
            df (pd.DataFrame): The raw cohort data.

        Returns:
            dict: A dictionary containing 'cohort_data', 'retention_pct', and 'spending_by_cohort'.
        """
        logger.info("Generating analytical pivot tables for cohorts...")
        
        # Calculate aggregate metrics per cohort month.
        cohort_data = df.groupby('CohortMonth').agg({
            'CustomerKey': 'count',
            'TotalSpend': ['sum', 'mean'],
            'TransactionCount': 'mean'
        }).round(2)
        
        cohort_data.columns = ['CohortSize', 'TotalRevenue', 'AvgSpend', 'AvgTransactions']
        
        logger.info("\nCohort Performance Metrics:")
        logger.info("-" * 80)
        logger.info(cohort_data.to_string())
        
        # Generate retention heatmap data.
        cohort_retention = df.groupby(['CohortMonth', 'MonthsSinceCohort']).size().unstack(fill_value=0)
        
        # Convert absolute retention counts to percentages based on the initial cohort size.
        first_col = cohort_retention.columns[0]
        retention_pct = cohort_retention.divide(cohort_retention[first_col], axis=0) * 100
        
        logger.info("\nCohort Retention Rates (%):")
        logger.info("-" * 80)
        logger.info(retention_pct.round(1).to_string())
        
        # Calculate the average spending per customer over the cohort's lifespan.
        spending_by_cohort = df.groupby(['CohortMonth', 'MonthsSinceCohort'])['TotalSpend'].mean().unstack(fill_value=0)
        
        logger.info("\nAverage Spending by Cohort Lifecycle:")
        logger.info("-" * 80)
        logger.info(spending_by_cohort.round(2).to_string())
        
        return {
            'cohort_data': cohort_data,
            'retention_pct': retention_pct,
            'spending_by_cohort': spending_by_cohort
        }
    
    def analyze_patterns(self, df):
        """
        Identifies key high-performance patterns within the cohort data.
        """
        logger.info("Identifying high-value cohort patterns...")
        
        # Identify cohorts with the highest cumulative revenue.
        cohort_ltv = df.groupby('CohortMonth')['TotalSpend'].sum().sort_values(ascending=False)
        
        logger.info("\nTop Cohorts by Lifetime Revenue:")
        logger.info("-" * 80)
        for cohort, revenue in cohort_ltv.head(5).items():
            logger.info(f"{cohort}: ${revenue:,.2f}")
        
        # Identify cohorts with the strongest retention characteristics.
        cohort_retention_rate = df.groupby('CohortMonth').apply(
            lambda x: (x['TransactionCount'] > 0).sum() / len(x) * 100
        ).sort_values(ascending=False)
        
        logger.info("\nTop Cohorts by Cumulative Retention Rate:")
        logger.info("-" * 80)
        for cohort, retention in cohort_retention_rate.head(5).items():
            logger.info(f"{cohort}: {retention:.1f}%")
    
    def export_results(self, cohort_tables, df):
        """
        Exports the summarized cohort analysis back to the SQL database.
        """
        logger.info("Persisting cohort analytics to the database...")
        
        try:
            # Export the high-level summary table.
            summary = cohort_tables['cohort_data'].reset_index()
            summary.columns = ['Cohort', 'CohortSize', 'TotalRevenue', 'AvgSpend', 'AvgTransactions']
            summary.to_sql('Cohort_Summary', self.engine, if_exists='replace', index=False)
            logger.info("Successfully persisted 'Cohort_Summary' table.")
            
            # Export the retention percentage matrix.
            retention = cohort_tables['retention_pct'].reset_index()
            retention.columns = [str(c) for c in retention.columns]
            retention.to_sql('Cohort_Retention', self.engine, if_exists='replace', index=False)
            logger.info("Successfully persisted 'Cohort_Retention' table.")
            
            # Export the cohort spending matrix.
            spending = cohort_tables['spending_by_cohort'].reset_index()
            spending.columns = [str(c) for c in spending.columns]
            spending.to_sql('Cohort_Spending', self.engine, if_exists='replace', index=False)
            logger.info("Successfully persisted 'Cohort_Spending' table.")
        
        except Exception as e:
            logger.error(f"Failed to persist cohort analytical results: {e}")
    
    def run(self):
        """
        Orchestrates the entire cohort analysis pipeline.
        """
        logger.info("-" * 80)
        logger.info("STARTING COHORT ANALYTICS PIPELINE")
        logger.info("-" * 80)
        
        try:
            # Stage 1: Data Extraction
            df = self.extract_cohort_data()
            
            # Stage 2: Pivot Matrix Construction
            cohort_tables = self.create_cohort_table(df)
            
            # Stage 3: Statistical Pattern Identification
            self.analyze_patterns(df)
            
            # Stage 4: Results Persistence
            self.export_results(cohort_tables, df)
            
            logger.info("-" * 80)
            logger.info("COHORT ANALYSIS PIPELINE COMPLETED")
            logger.info("-" * 80)
        
        except Exception as e:
            logger.error(f"Cohort analytics pipeline failure: {e}")
            raise
        
        finally:
            self.engine.dispose()


if __name__ == "__main__":
    analyzer = CohortAnalyzer()
    analyzer.run()
