"""
CLV Calculator Module

This module provides the CLVCalculator class, which predicts the long-term 
financial value of a customer based on historical transaction frequency and 
average spend.
"""

import pandas as pd
import logging
import os
from config import LOGS_FOLDER

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, 'clv_calculator.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CLVCalculator:
    """
    Predicts Customer Lifetime Value (CLV) using a simplified deterministic model.
    """
    
    def __init__(self, df, lifespan_years=5):
        """
        Initializes the CLV calculator.

        Args:
            df (pd.DataFrame): Input DataFrame with individual customer aggregates.
            lifespan_years (int): Number of future years for value prediction.
        """
        self.df = df.copy()
        self.lifespan_years = lifespan_years
        logger.info(f"CLV Calculator initialized for a {lifespan_years}-year projection.")
    
    def calculate_clv(self):
        """
        Calculates projected lifetime value using transaction frequency and ticket size.
        
        Formula:
        CLV = (Average Order Value * Annual Transaction Frequency) * Lifespan Years
        """
        logger.info("Executing CLV prediction model...")
        
        # Ensure data integrity by filling missing values.
        # Minimal purchase count of 1 is assumed for registered customers.
        self.df['AvgOrderValue'] = self.df['AvgOrderValue'].fillna(0)
        self.df['PurchaseCount'] = self.df['PurchaseCount'].fillna(1)
        self.df['CustomerTenureDays'] = self.df['CustomerTenureDays'].fillna(365)
        
        # Normalize transaction frequency to an annual basis.
        self.df['AnnualRevenue'] = (
            self.df['AvgOrderValue'] * 
            (self.df['PurchaseCount'] / (self.df['CustomerTenureDays'] / 365 + 1))
        ).round(2)
        
        # Predict CLV based on the projected annual revenue and product lifespan.
        self.df['CLV_Predicted'] = (
            self.df['AnnualRevenue'] * self.lifespan_years
        ).round(2)
        
        logger.info("CLV prediction model completed.")
        logger.info(f"  Average Predicted CLV: ${self.df['CLV_Predicted'].mean():,.2f}")
        logger.info(f"  Median Predicted CLV : ${self.df['CLV_Predicted'].median():,.2f}")
        logger.info(f"  Maximum Predicted CLV: ${self.df['CLV_Predicted'].max():,.2f}")
        
        return self.df


# TEST
if __name__ == "__main__":
    from data_extraction import DataExtractor
    from rfm_calculator import RFMCalculator
    
    print("Testing CLV Calculator...\n")
    
    # Get data
    extractor = DataExtractor()
    df = extractor.extract_customer_360()
    
    # Calculate RFM
    rfm = RFMCalculator(df)
    df = rfm.calculate_all()
    
    # Calculate CLV
    clv = CLVCalculator(df, lifespan_years=5)
    df = clv.calculate_clv()
    
    # Show results
    print("\n=== RESULTS ===")
    print(df[['CustomerID', 'Segment', 'CLV_Predicted']].to_string())
    
    extractor.close_connection()