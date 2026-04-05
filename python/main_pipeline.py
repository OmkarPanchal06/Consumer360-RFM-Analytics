"""
Main ETL Pipeline
Runs everything in order
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
    """Main pipeline orchestration"""
    
    def __init__(self):
        self.engine = create_engine(CONNECTION_STRING)
        logger.info("Pipeline initialized")
    
    def step1_extract(self):
        """Step 1: Extract data"""
        logger.info("\n" + "="*70)
        logger.info("STEP 1: DATA EXTRACTION")
        logger.info("="*70)
        
        extractor = DataExtractor()
        self.df = extractor.extract_customer_360()
        extractor.close_connection()
        
        return self.df
    
    def step2_rfm(self):
        """Step 2: Calculate RFM"""
        logger.info("\n" + "="*70)
        logger.info("STEP 2: RFM SEGMENTATION")
        logger.info("="*70)
        
        rfm = RFMCalculator(self.df)
        self.df = rfm.calculate_all()
        
        return self.df
    
    def step3_clv(self):
        """Step 3: Calculate CLV"""
        logger.info("\n" + "="*70)
        logger.info("STEP 3: CLV CALCULATION")
        logger.info("="*70)
        
        clv = CLVCalculator(self.df)
        self.df = clv.calculate_clv()
        
        return self.df
    
    def step4_export(self):
        """Step 4: Export to SQL"""
        logger.info("\n" + "="*70)
        logger.info("STEP 4: EXPORT RESULTS")
        logger.info("="*70)
        
        # Select key columns
        export_cols = [
            'CustomerKey', 'CustomerID', 'CustomerName', 'Email',
            'RegistrationDate', 'RecencyDays', 'TransactionCount', 'TotalSpend',
            'AvgOrderValue', 'CustomerTenureDays', 'CohortYearMonth',
            'PrimaryRegion', 'Country', 'State',
            'R_Score', 'F_Score', 'M_Score', 'RFM_Score',
            'Segment', 'CLV_Predicted', 'LastPurchaseDate'
        ]
        
        export_df = self.df[[col for col in export_cols if col in self.df.columns]]
        
        # Export to SQL
        export_df.to_sql(OUTPUT_TABLE_NAME, self.engine, if_exists='replace', index=False)
        
        logger.info(f"✓ Exported {len(export_df):,} records to {OUTPUT_TABLE_NAME}")
        logger.info(f"  Columns: {len(export_df.columns)}")
    
    def step5_market_basket(self):
        """Step 5: Market Basket Analysis"""
        logger.info("\n" + "="*70)
        logger.info("STEP 5: MARKET BASKET ANALYSIS")
        logger.info("="*70)
        
        analyzer = MarketBasketAnalyzer()
        self.basket_rules = analyzer.run()
        
        return self.basket_rules
    
    def step6_cohort_analysis(self):
        """Step 6: Cohort Analysis"""
        logger.info("\n" + "="*70)
        logger.info("STEP 6: COHORT ANALYSIS")
        logger.info("="*70)
        
        analyzer = CohortAnalyzer()
        analyzer.run()
    
    def run(self):
        """Run complete pipeline"""
        logger.info("="*70)
        logger.info("CONSUMER360 RFM PIPELINE")
        logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
        try:
            self.step1_extract()
            self.step2_rfm()
            self.step3_clv()
            self.step4_export()
            self.step5_market_basket()
            self.step6_cohort_analysis()
            
            logger.info("\n" + "="*70)
            logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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