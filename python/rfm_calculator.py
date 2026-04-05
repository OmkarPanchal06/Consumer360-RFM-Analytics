"""
RFM Calculator Module

This module implements the RFM (Recency, Frequency, Monetary) analytical model 
to score and segment customers. It transforms raw transactional aggregates 
into behavioral insights.
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
    """
    Computes RFM scores and segments customers using percentile-based ranking.
    """
    
    def __init__(self, df):
        """
        Initializes the calculator with pre-aggregated customer transaction data.

        Args:
            df (pd.DataFrame): Input DataFrame containing 'RecencyDays', 
                              'TransactionCount', and 'TotalSpend'.
        """
        self.df = df.copy()
        logger.info("RFM Calculator initialized with input data.")
    
    def calculate_r_score(self):
        """
        Calculates the Recency Score (1-5) using quintile ranking.
        
        A score of 5 represents the most recent 20% of customers, 
        indicative of high current engagement.
        """
        logger.info("Processing Recency (R) Scores...")
        
        # Default missing recency to 365 days (inactive).
        self.df['RecencyDays'] = self.df['RecencyDays'].fillna(365)
        
        # Lower days since purchase is better; quintile labels are inverted.
        self.df['R_Score'] = pd.qcut(
            self.df['RecencyDays'].rank(method='first'),
            q=5,
            labels=[5, 4, 3, 2, 1],
            duplicates='drop'
        ).astype(int)
        
        self.df['R_Score'] = self.df['R_Score'].fillna(1)
        logger.info(f"Recency score distribution:\n{self.df['R_Score'].value_counts().sort_index()}")
    
    def calculate_f_score(self):
        """
        Calculates the Frequency Score (1-5) based on total transaction count.
        
        A score of 5 represents the top 20% of customers by transaction volume.
        """
        logger.info("Processing Frequency (F) Scores...")
        
        # Default missing transaction counts to zero.
        self.df['TransactionCount'] = self.df['TransactionCount'].fillna(0)
        
        # Higher transaction count is better.
        self.df['F_Score'] = pd.qcut(
            self.df['TransactionCount'].rank(method='first'),
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        ).astype(int)
        
        self.df['F_Score'] = self.df['F_Score'].fillna(1)
        logger.info(f"Frequency score distribution:\n{self.df['F_Score'].value_counts().sort_index()}")
    
    def calculate_m_score(self):
        """
        Calculates the Monetary Score (1-5) based on total spending.
        
        A score of 5 represents the top 20% of highest-spending customers.
        """
        logger.info("Processing Monetary (M) Scores...")
        
        # Default missing spending to zero.
        self.df['TotalSpend'] = self.df['TotalSpend'].fillna(0)
        
        # Higher spend is better.
        self.df['M_Score'] = pd.qcut(
            self.df['TotalSpend'].rank(method='first'),
            q=5,
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        ).astype(int)
        
        self.df['M_Score'] = self.df['M_Score'].fillna(1)
        logger.info(f"Monetary score distribution:\n{self.df['M_Score'].value_counts().sort_index()}")
    
    def calculate_combined_rfm_score(self):
        """
        Calculates the combined RFM score using a weighted average.
        
        The weights (Monetary and Frequency prioritized over Recency) 
        reflect standard business value assessments for customer loyalty.
        """
        logger.info("Computing combined RFM scores based on weights...")
        
        # Pull weights (standard weighting for enhanced sensitivity)
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
        
        logger.info("RFM Score statistics:")
        logger.info(f"  Mean: {self.df['RFM_Score'].mean():.2f}")
        logger.info(f"  Min: {self.df['RFM_Score'].min():.2f}")
        logger.info(f"  Max: {self.df['RFM_Score'].max():.2f}")
    
    def segment_customers(self):
        """
        Assigns customers to behavioral segments based on RFM score thresholds.
        
        Segments categorize customers into actionable groups such as 'Champions' 
        for immediate rewards or 'At Risk' for re-engagement campaigns.
        """
        logger.info("Applying segmentation business logic...")
        
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
        
        # Generate summary report in the logs
        logger.info("\nSegmentation Summary:")
        for segment in self.df['Segment'].unique():
            count = len(self.df[self.df['Segment'] == segment])
            pct = (count / len(self.df)) * 100
            avg_spend = self.df[self.df['Segment'] == segment]['TotalSpend'].mean()
            logger.info(f"{segment:30s}: {count} customers ({pct:.1f}%) | Avg Spend: ${avg_spend:,.2f}")
    
    def calculate_all(self):
        """
        Executes the full RFM calculation pipeline.
        
        Returns:
            pd.DataFrame: The original DataFrame enriched with scores and segments.
        """
        logger.info("-" * 70)
        logger.info("STARTING RFM ANALYTICS ENGINE")
        logger.info("-" * 70)
        
        self.calculate_r_score()
        self.calculate_f_score()
        self.calculate_m_score()
        self.calculate_combined_rfm_score()
        self.segment_customers()
        
        logger.info("-" * 70)
        logger.info("RFM ANALYTICS COMPLETED")
        logger.info("-" * 70)
        
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