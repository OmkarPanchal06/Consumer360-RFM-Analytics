"""
Main ETL Pipeline Orchestrator

This module serves as the primary entry point for the Consumer360 analytical 
pipeline. It orchestrates the flow of data through extraction, RFM scoring, 
CLV prediction, and advanced behavioral analysis stages.
"""

import logging
import os
from datetime import datetime
from config import LOGS_FOLDER, CONNECTION_STRING, OUTPUT_TABLE_NAME
from data_extraction import DataExtractor
from rfm_calculator import RFMCalculator
from clv_calculator import CLVCalculator
from market_basket_analysis import MarketBasketAnalyzer
from cohort_analysis import CohortAnalyzer
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, 'main_pipeline.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Consumer360Pipeline:
    """
    Orchestrates the sequential execution of all data engineering and 
    analytical modules.
    """
    
    def __init__(self):
        """
        Initializes the pipeline engine and establishes a database connection pool.
        """
        self.engine = create_engine(CONNECTION_STRING)
        logger.info("Orchestration engine initialized.")
    
    def step1_extract(self):
        """
        Stage 1: Extracts unified customer views from the transactional database.

        Returns:
            pd.DataFrame: The raw customer dataset.
        """
        logger.info("-" * 80)
        logger.info("STAGE 1: DATA EXTRACTION")
        logger.info("-" * 80)
        
        extractor = DataExtractor()
        self.df = extractor.extract_customer_360()
        extractor.close_connection()
        
        return self.df
    
    def step2_rfm(self):
        """
        Stage 2: Executes behavioral segmentation using the RFM model.

        Returns:
            pd.DataFrame: The dataset enriched with RFM scores and segments.
        """
        logger.info("-" * 80)
        logger.info("STAGE 2: RFM SEGMENTATION")
        logger.info("-" * 80)
        
        rfm = RFMCalculator(self.df)
        self.df = rfm.calculate_all()
        
        return self.df
    
    def step3_clv(self):
        """
        Stage 3: Computes projected Customer Lifetime Value (CLV).

        Returns:
            pd.DataFrame: The dataset enriched with CLV predictions.
        """
        logger.info("-" * 80)
        logger.info("STAGE 3: CLV PREDICTION MODELING")
        logger.info("-" * 80)
        
        clv = CLVCalculator(self.df)
        self.df = clv.calculate_clv()
        
        return self.df
    
    def step4_export(self):
        """
        Stage 4: Persists the enriched analytical dataset back to SQL Server.
        """
        logger.info("-" * 80)
        logger.info("STAGE 4: DATA PERSISTENCE")
        logger.info("-" * 80)
        
        # Define the subset of columns required for downstream reporting.
        export_cols = [
            'CustomerKey', 'CustomerID', 'CustomerName', 'Email',
            'RegistrationDate', 'RecencyDays', 'TransactionCount', 'TotalSpend',
            'AvgOrderValue', 'CustomerTenureDays', 'CohortYearMonth',
            'PrimaryRegion', 'Country', 'State',
            'R_Score', 'F_Score', 'M_Score', 'RFM_Score',
            'Segment', 'CLV_Predicted', 'LastPurchaseDate'
        ]
        
        export_df = self.df[[col for col in export_cols if col in self.df.columns]]
        
        # Load the transformed data into the target table.
        export_df.to_sql(OUTPUT_TABLE_NAME, self.engine, if_exists='replace', index=False)
        
        logger.info(f"Successfully persisted {len(export_df):,} records to {OUTPUT_TABLE_NAME}.")
    
    def step5_market_basket(self):
        """
        Stage 5: Executes Market Basket Analysis for cross-product insights.
        """
        logger.info("-" * 80)
        logger.info("STAGE 5: MARKET BASKET ANALYSIS")
        logger.info("-" * 80)
        
        analyzer = MarketBasketAnalyzer()
        self.basket_rules = analyzer.run()
        
        return self.basket_rules
    
    def step6_cohort_analysis(self):
        """
        Stage 6: Executes Cohort Analysis for customer retention tracking.
        """
        logger.info("-" * 80)
        logger.info("STAGE 6: COHORT ANALYSIS")
        logger.info("-" * 80)
        
        analyzer = CohortAnalyzer()
        analyzer.run()
    
    def run(self):
        """
        Executes the end-to-end analytical pipeline in proper sequence.
        """
        logger.info("-" * 80)
        logger.info("CONSUMER360 ANALYTICAL PIPELINE START")
        logger.info(f"Execution started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("-" * 80)
        
        try:
            self.step1_extract()
            self.step2_rfm()
            self.step3_clv()
            self.step4_export()
            self.step5_market_basket()
            self.step6_cohort_analysis()
            
            logger.info("-" * 80)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Execution finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("-" * 80)
            logger.info("="*70)
            
            return True
        
        except Exception as e:
            logger.error(f"\n✗ PIPELINE FAILED: {e}")
            raise
        
        finally:
            self.engine.dispose()


# RUN PIPELINE
if __name__ == "__main__":
    pipeline = Consumer360Pipeline()
    pipeline.run()