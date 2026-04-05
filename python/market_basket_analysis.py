"""
Market Basket Analysis Module

This module implements the Apriori algorithm to discover product association 
rules. This analysis identifies product sets that are frequently purchased 
together to optimize cross-selling strategies.
"""

import pandas as pd
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
        logging.FileHandler(os.path.join(LOGS_FOLDER, 'market_basket.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MarketBasketAnalyzer:
    """
    Orchestrates the Market Basket Analysis (MBA) using the Apriori algorithm.
    """
    
    def __init__(self):
        """
        Initializes the analyzer by establishing a database connection.
        """
        self.engine = create_engine(CONNECTION_STRING)
        logger.info("Market Basket Analyzer initialized.")
    
    def extract_transactions(self):
        """
        Extracts atomic transaction data consolidated with product categories.

        Returns:
            pd.DataFrame: A DataFrame containing OrderNumber and ProductName.
        """
        logger.info("Extracting transactional data from Fact_Sales...")
        
        query = """
        SELECT 
            fs.OrderNumber,
            p.ProductName,
            p.Category
        FROM Fact_Sales fs
        JOIN Dim_Product p ON fs.ProductKey = p.ProductKey
        WHERE fs.ReturnFlag = 'N'
        ORDER BY fs.OrderNumber
        """
        
        try:
            df = pd.read_sql_query(query, self.engine)
            logger.info(f"Successfully extracted {len(df):,} transaction items.")
            logger.info(f"Unique transaction identifier count: {df['OrderNumber'].nunique():,}")
            return df
        except Exception as e:
            logger.error(f"Transaction data extraction failed: {e}")
            raise
    
    def prepare_basket_data(self, df):
        """
        Transforms raw transaction data into a 'basket' format for encoding.
        
        A basket is a list of product names associated with a single OrderNumber.
        """
        logger.info("Transforming transaction data into basket sequences...")
        
        # Aggregate products by OrderNumber into nested lists.
        baskets = df.groupby('OrderNumber')['ProductName'].apply(list).values.tolist()
        
        logger.info(f"Prepared {len(baskets)} transaction baskets.")
        logger.info(f"Average product count per basket: {df.groupby('OrderNumber').size().mean():.1f}")
        
        return baskets
    
    def analyze_associations(self, baskets):
        """
        Applies the Apriori algorithm and generates association rules.
        
        Args:
            baskets (list): A list of lists containing product names.

        Returns:
            pd.DataFrame: A DataFrame containing association rules (antecedents, consequents, lift, etc.).
        """
        logger.info("Executing Apriori algorithm for association mining...")
        
        try:
            from mlxtend.frequent_patterns import apriori, association_rules
            from mlxtend.preprocessing import TransactionEncoder
        except ImportError:
            logger.error("The 'mlxtend' library is required. Please install via: pip install mlxtend")
            raise
        
        # Boolean encode the transaction baskets to fit the Apriori requirements.
        te = TransactionEncoder()
        te_ary = te.fit(baskets).transform(baskets)
        df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
        
        logger.info(f"Encoded {len(df_encoded)} transactions across {len(te.columns_)} dimensions.")
        
        # Identify frequent itemsets with a support threshold tailored for high-variance retail data.
        min_support = 0.001
        frequent_itemsets = apriori(df_encoded, min_support=min_support, use_colnames=True)
        
        logger.info(f"Identified {len(frequent_itemsets)} frequent itemsets.")
        
        if len(frequent_itemsets) > 1:
            min_confidence = 0.01
            try:
                # Generate association rules based on confidence metrics.
                rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
                
                # Transform frozenset objects into readable comma-delimited strings.
                rules['antecedents'] = rules['antecedents'].apply(lambda x: ', '.join(list(x)))
                rules['consequents'] = rules['consequents'].apply(lambda x: ', '.join(list(x)))
                
                # Rank rules by Lift to identify the strongest cross-purchase relationships.
                rules = rules.sort_values('lift', ascending=False)
                
                logger.info(f"Generated {len(rules)} association rules.")
                logger.info("\nTop 10 Association Rules by Lift:")
                logger.info("-" * 80)
                
                for idx, row in rules.head(10).iterrows():
                    logger.info(f"{row['antecedents']} -> {row['consequents']} (Lift: {row['lift']:.2f})")
                
                return rules
            except ValueError as e:
                logger.warning(f"Insufficient frequent itemsets to derive rules: {e}")
                return pd.DataFrame()
        else:
            logger.warning("Insufficient itemsets found; skipping rule generation.")
            return pd.DataFrame()
    
    def export_rules(self, rules):
        """
        Persists association rules to the application database.
        """
        logger.info("Persisting association rules to the database...")
        
        try:
            rules.to_sql(
                'Market_Basket_Analysis',
                self.engine,
                if_exists='replace',
                index=False
            )
            logger.info("Successfully persisted rules to 'Market_Basket_Analysis' table.")
        except Exception as e:
            logger.error(f"Failed to export association rules: {e}")
    
    def run(self):
        """
        Orchestrates the complete Market Basket Analysis workflow.

        Returns:
            pd.DataFrame: The generated association rules.
        """
        logger.info("-" * 80)
        logger.info("STARTING MARKET BASKET ANALYSIS PIPELINE")
        logger.info("-" * 80)
        
        try:
            # Stage 1: Extraction
            df_transactions = self.extract_transactions()
            
            # Stage 2: Pre-processing
            baskets = self.prepare_basket_data(df_transactions)
            
            # Stage 3: Mining Associative Rules
            rules = self.analyze_associations(baskets)
            
            # Stage 4: Data Persistence
            if not rules.empty:
                self.export_rules(rules)
            
            logger.info("-" * 80)
            logger.info("MARKET BASKET ANALYSIS COMPLETED SUCCESSFULLY")
            logger.info("-" * 80)
            
            return rules
        
        except Exception as e:
            logger.error(f"Market Basket Analytics pipeline failed: {e}")
            raise
        
        finally:
            self.engine.dispose()


if __name__ == "__main__":
    analyzer = MarketBasketAnalyzer()
    rules = analyzer.run()
    
    if not rules.empty:
        print("\n" + "="*80)
        print("Top 5 Rules:")
        print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(5))
