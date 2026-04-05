"""
Cohort Analysis Module
Track customer retention and behavior by acquisition cohort
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
    """Analyze customer behavior by acquisition cohort"""
    
    def __init__(self):
        self.engine = create_engine(CONNECTION_STRING)
        logger.info("Cohort Analyzer initialized")
    
    def extract_cohort_data(self):
        """Extract customer cohort data"""
        logger.info("Extracting cohort data...")
        
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
            logger.info(f"✓ Extracted {len(df):,} customers for cohort analysis")
            return df
        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            raise
    
    def create_cohort_table(self, df):
        """Create cohort retention table"""
        logger.info("Creating cohort tables...")
        
        # Cohort size (customers per cohort)
        cohort_data = df.groupby('CohortMonth').agg({
            'CustomerKey': 'count',
            'TotalSpend': ['sum', 'mean'],
            'TransactionCount': 'mean'
        }).round(2)
        
        cohort_data.columns = ['CohortSize', 'TotalRevenue', 'AvgSpend', 'AvgTransactions']
        
        logger.info(f"\n{'='*80}")
        logger.info("COHORT PERFORMANCE")
        logger.info(f"{'='*80}")
        logger.info(cohort_data.to_string())
        
        # Retention table
        cohort_retention = df.groupby(['CohortMonth', 'MonthsSinceCohort']).size().unstack(fill_value=0)
        
        # Convert to retention percentage
        # Use the first column (month 0 typically) as base
        first_col = cohort_retention.columns[0]
        retention_pct = cohort_retention.divide(cohort_retention[first_col], axis=0) * 100
        
        logger.info(f"\n{'='*80}")
        logger.info("COHORT RETENTION RATE (%)")
        logger.info(f"{'='*80}")
        logger.info(retention_pct.round(1).to_string())
        
        # Spending by cohort
        spending_by_cohort = df.groupby(['CohortMonth', 'MonthsSinceCohort'])['TotalSpend'].mean().unstack(fill_value=0)
        
        logger.info(f"\n{'='*80}")
        logger.info("AVERAGE SPEND BY COHORT AND AGE")
        logger.info(f"{'='*80}")
        logger.info(spending_by_cohort.round(2).to_string())
        
        return {
            'cohort_data': cohort_data,
            'retention_pct': retention_pct,
            'spending_by_cohort': spending_by_cohort
        }
    
    def analyze_patterns(self, df):
        """Analyze cohort patterns"""
        logger.info("Analyzing cohort patterns...")
        
        # Which cohort has highest lifetime value?
        cohort_ltv = df.groupby('CohortMonth')['TotalSpend'].sum().sort_values(ascending=False)
        
        logger.info(f"\n{'='*80}")
        logger.info("TOP COHORTS BY LIFETIME REVENUE")
        logger.info(f"{'='*80}")
        for cohort, revenue in cohort_ltv.head(5).items():
            logger.info(f"{cohort}: ${revenue:,.2f}")
        
        # Which cohort has highest retention?
        cohort_retention_rate = df.groupby('CohortMonth').apply(
            lambda x: (x['TransactionCount'] > 0).sum() / len(x) * 100
        ).sort_values(ascending=False)
        
        logger.info(f"\n{'='*80}")
        logger.info("TOP COHORTS BY RETENTION RATE")
        logger.info(f"{'='*80}")
        for cohort, retention in cohort_retention_rate.head(5).items():
            logger.info(f"{cohort}: {retention:.1f}%")
    
    def export_results(self, cohort_tables, df):
        """Export cohort analysis to database"""
        logger.info("Exporting cohort analysis...")
        
        try:
            # Create summary table
            summary = cohort_tables['cohort_data'].reset_index()
            summary.columns = ['Cohort', 'CohortSize', 'TotalRevenue', 'AvgSpend', 'AvgTransactions']
            
            summary.to_sql('Cohort_Summary', self.engine, if_exists='replace', index=False)
            logger.info("✓ Exported cohort summary")
            
            # Export retention percentages
            # Because columns are integers (MonthsSinceCohort), map them to strings for SQL
            retention = cohort_tables['retention_pct'].reset_index()
            retention.columns = [str(c) for c in retention.columns]
            retention.to_sql('Cohort_Retention', self.engine, if_exists='replace', index=False)
            logger.info("✓ Exported retention table")
            
            # Export spending
            spending = cohort_tables['spending_by_cohort'].reset_index()
            spending.columns = [str(c) for c in spending.columns]
            spending.to_sql('Cohort_Spending', self.engine, if_exists='replace', index=False)
            logger.info("✓ Exported spending table")
        
        except Exception as e:
            logger.error(f"✗ Export failed: {e}")
    
    def run(self):
        """Run complete cohort analysis"""
        logger.info("="*80)
        logger.info("STARTING COHORT ANALYSIS")
        logger.info("="*80)
        
        try:
            # Step 1: Extract
            df = self.extract_cohort_data()
            
            # Step 2: Create tables
            cohort_tables = self.create_cohort_table(df)
            
            # Step 3: Analyze patterns
            self.analyze_patterns(df)
            
            # Step 4: Export
            self.export_results(cohort_tables, df)
            
            logger.info("\n" + "="*80)
            logger.info("✓ COHORT ANALYSIS COMPLETE")
            logger.info("="*80)
        
        except Exception as e:
            logger.error(f"✗ Analysis failed: {e}")
            raise
        
        finally:
            self.engine.dispose()


if __name__ == "__main__":
    analyzer = CohortAnalyzer()
    analyzer.run()
