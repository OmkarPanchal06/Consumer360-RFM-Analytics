"""
CLV Calculator Module
Predicts customer lifetime value
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
    """Calculates Customer Lifetime Value"""
    
    def __init__(self, df, lifespan_years=5):
        """
        Initialize CLV Calculator
        
        Args:
            df: DataFrame with customer data
            lifespan_years: How many years to predict (default: 5 years)
        """
        self.df = df.copy()
        self.lifespan_years = lifespan_years
        logger.info(f"CLV Calculator initialized (lifespan: {lifespan_years} years)")
    
    def calculate_clv(self):
        """
        Simple CLV Formula:
        CLV = Average Order Value × Purchase Frequency per Year × Customer Lifespan
        
        Where:
        - Average Order Value = Total Spend / Number of Orders
        - Purchase Frequency = Orders per Year
        - Lifespan = Expected years as customer
        """
        logger.info("Calculating CLV...")
        
        # Fill missing values
        self.df['AvgOrderValue'] = self.df['AvgOrderValue'].fillna(0)
        self.df['PurchaseCount'] = self.df['PurchaseCount'].fillna(1)
        self.df['CustomerTenureDays'] = self.df['CustomerTenureDays'].fillna(365)
        
        # Annual revenue per customer
        self.df['AnnualRevenue'] = (
            self.df['AvgOrderValue'] * 
            (self.df['PurchaseCount'] / (self.df['CustomerTenureDays'] / 365 + 1))
        ).round(2)
        
        # CLV = Annual Revenue × Lifespan
        self.df['CLV_Predicted'] = (
            self.df['AnnualRevenue'] * self.lifespan_years
        ).round(2)
        
        logger.info(f"✓ CLV calculated")
        logger.info(f"  Average CLV: ${self.df['CLV_Predicted'].mean():,.2f}")
        logger.info(f"  Median CLV: ${self.df['CLV_Predicted'].median():,.2f}")
        logger.info(f"  Max CLV: ${self.df['CLV_Predicted'].max():,.2f}")
        
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