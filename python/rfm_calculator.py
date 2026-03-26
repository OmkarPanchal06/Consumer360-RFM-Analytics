"""
RFM Calculator Module
Scores customers on Recency, Frequency, Monetary
"""

import pandas as pd
import logging
import os
from config import LOGS_FOLDER

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, 'rfm_calculator.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RFMCalculator:
    """Calculates RFM scores for customers"""
    
    def __init__(self, df):
        """
        Initialize with customer data
        df = DataFrame with columns: RecencyDays, TransactionCount, TotalSpend
        """
        self.df = df.copy()
        logger.info("RFM Calculator initialized")
    
    def calculate_r_score(self):
        """
        Recency Score (1-5)
        
        5 = Most recent (0-20th percentile)
        4 = 20-40th percentile
        3 = 40-60th percentile
        2 = 60-80th percentile
        1 = Least recent (80-100th percentile)
        """
        logger.info("Calculating Recency Score...")
        
        # Fill missing values
        self.df['RecencyDays'] = self.df['RecencyDays'].fillna(365)
        
        # Lower recency is better (more recent is good)
        self.df['R_Score'] = pd.qcut(
            self.df['RecencyDays'].rank(method='first'),
            q=5,
            labels=[5, 4, 3, 2, 1],  # Inverted
            duplicates='drop'
        ).astype(int)
        
        self.df['R_Score'] = self.df['R_Score'].fillna(1)
        logger.info(f"✓ Recency Score distribution:\n{self.df['R_Score'].value_counts().sort_index()}")
    
    def calculate_f_score(self):
        """
        Frequency Score (1-5)
        
        5 = Most frequent (80-100th percentile)
        4 = 60-80th percentile
        3 = 40-60th percentile
        2 = 20-40th percentile
        1 = Least frequent (0-20th percentile)
        """
        logger.info("Calculating Frequency Score...")
        
        # Fill missing values
        self.df['TransactionCount'] = self.df['TransactionCount'].fillna(0)
        
        # Higher frequency is better
        self.df['F_Score'] = pd.qcut(
            self.df['TransactionCount'].rank(method='first'),
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        ).astype(int)
        
        self.df['F_Score'] = self.df['F_Score'].fillna(1)
        logger.info(f"✓ Frequency Score distribution:\n{self.df['F_Score'].value_counts().sort_index()}")
    
    def calculate_m_score(self):
        """
        Monetary Score (1-5)
        
        5 = Highest spenders (80-100th percentile)
        4 = 60-80th percentile
        3 = 40-60th percentile
        2 = 20-40th percentile
        1 = Lowest spenders (0-20th percentile)
        """
        logger.info("Calculating Monetary Score...")
        
        # Fill missing values
        self.df['TotalSpend'] = self.df['TotalSpend'].fillna(0)
        
        # Higher spend is better
        self.df['M_Score'] = pd.qcut(
            self.df['TotalSpend'].rank(method='first'),
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        ).astype(int)
        
        self.df['M_Score'] = self.df['M_Score'].fillna(1)
        logger.info(f"✓ Monetary Score distribution:\n{self.df['M_Score'].value_counts().sort_index()}")
    
    def calculate_combined_rfm_score(self):
        """Calculate combined RFM score (weighted average)"""
        logger.info("Calculating Combined RFM Score...")
        
        # Weights (M and F more important than R)
        r_weight = 1.0
        f_weight = 1.2
        m_weight = 1.5
        total_weight = r_weight + f_weight + m_weight
        
        self.df['RFM_Score'] = (
            (self.df['R_Score'] * r_weight +
             self.df['F_Score'] * f_weight +
             self.df['M_Score'] * m_weight) /
            total_weight
        ).round(2)
        
        logger.info(f"✓ RFM Score Stats:")
        logger.info(f"  Mean: {self.df['RFM_Score'].mean():.2f}")
        logger.info(f"  Min: {self.df['RFM_Score'].min():.2f}")
        logger.info(f"  Max: {self.df['RFM_Score'].max():.2f}")
    
    def segment_customers(self):
        """Assign customers to segments"""
        logger.info("Segmenting customers...")
        
        def assign_segment(row):
            r = row['R_Score']
            f = row['F_Score']
            m = row['M_Score']
            rfm = row['RFM_Score']
            
            if rfm >= 4.5:
                return 'Champions'
            elif f >= 4 and m >= 4:
                return 'Loyal Customers'
            elif r >= 4 and f >= 3:
                return 'Potential Loyalists'
            elif r >= 3 and rfm >= 3:
                return 'Customers Needing Attention'
            elif r <= 2 and f >= 3 and m >= 3:
                return 'At Risk'
            elif r <= 2 and f <= 2:
                return 'Hibernating'
            else:
                return 'New/Low Value'
        
        self.df['Segment'] = self.df.apply(assign_segment, axis=1)
        
        # Print summary
        logger.info("\n=== SEGMENT SUMMARY ===")
        for segment in self.df['Segment'].unique():
            count = len(self.df[self.df['Segment'] == segment])
            pct = (count / len(self.df)) * 100
            avg_spend = self.df[self.df['Segment'] == segment]['TotalSpend'].mean()
            logger.info(f"{segment:30s}: {count} customers ({pct:.1f}%) | Avg Spend: ${avg_spend:,.2f}")
    
    def calculate_all(self):
        """Run all calculations"""
        logger.info("=" * 70)
        logger.info("STARTING RFM CALCULATION")
        logger.info("=" * 70)
        
        self.calculate_r_score()
        self.calculate_f_score()
        self.calculate_m_score()
        self.calculate_combined_rfm_score()
        self.segment_customers()
        
        logger.info("=" * 70)
        logger.info("✓ RFM CALCULATION COMPLETE")
        logger.info("=" * 70)
        
        return self.df


# TEST
if __name__ == "__main__":
    from data_extraction import DataExtractor
    
    print("Testing RFM Calculator...\n")
    
    # Get data
    extractor = DataExtractor()
    df = extractor.extract_customer_360()
    
    # Calculate RFM
    rfm = RFMCalculator(df)
    df_results = rfm.calculate_all()
    
    # Show results
    print("\n=== RESULTS ===")
    print(df_results[['CustomerID', 'R_Score', 'F_Score', 'M_Score', 'RFM_Score', 'Segment']].to_string())
    
    extractor.close_connection()