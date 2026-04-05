"""
Market Basket Analysis Module
Uses Apriori algorithm to find product associations
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
    """Market basket analysis using Apriori algorithm"""
    
    def __init__(self):
        self.engine = create_engine(CONNECTION_STRING)
        logger.info("Market Basket Analyzer initialized")
    
    def extract_transactions(self):
        """Extract transaction data from database"""
        logger.info("Extracting transaction data...")
        
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
            logger.info(f"✓ Extracted {len(df):,} transaction items")
            logger.info(f"  Unique orders: {df['OrderNumber'].nunique():,}")
            return df
        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            raise
    
    def prepare_basket_data(self, df):
        """Convert transaction data to basket format"""
        logger.info("Preparing basket data...")
        
        # Group by order to get products per transaction
        baskets = df.groupby('OrderNumber')['ProductName'].apply(list).values.tolist()
        
        logger.info(f"✓ Prepared {len(baskets)} transaction baskets")
        logger.info(f"  Average items per basket: {df.groupby('OrderNumber').size().mean():.1f}")
        
        return baskets
    
    def analyze_associations(self, baskets):
        """Analyze product associations"""
        logger.info("Analyzing product associations...")
        
        try:
            from mlxtend.frequent_patterns import apriori, association_rules
            from mlxtend.preprocessing import TransactionEncoder
        except ImportError:
            logger.error("mlxtend not installed. Run: pip install mlxtend")
            raise
        
        # Encode baskets
        te = TransactionEncoder()
        te_ary = te.fit(baskets).transform(baskets)
        df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
        
        logger.info(f"✓ Encoded {len(df_encoded)} transactions with {len(te.columns_)} products")
        
        # Apply Apriori
        min_support = 0.001  # Lowered min_support since kaggle retail dataset has random assortment of items and we want to find something
        frequent_itemsets = apriori(df_encoded, min_support=min_support, use_colnames=True)
        
        logger.info(f"✓ Found {len(frequent_itemsets)} frequent itemsets")
        
        # Generate rules
        if len(frequent_itemsets) > 1:
            min_confidence = 0.01  # Lowered confidence threshold
            try:
                rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
                
                # Convert frozensets to strings
                rules['antecedents'] = rules['antecedents'].apply(lambda x: ', '.join(list(x)))
                rules['consequents'] = rules['consequents'].apply(lambda x: ', '.join(list(x)))
                
                # Sort by lift (strength of association)
                rules = rules.sort_values('lift', ascending=False)
                
                logger.info(f"✓ Generated {len(rules)} association rules")
                logger.info("\nTop Rules (by Lift):")
                logger.info("="*80)
                
                for idx, row in rules.head(10).iterrows():
                    logger.info(f"\n{row['antecedents']} → {row['consequents']}")
                    logger.info(f"  Support: {row['support']:.2%}")
                    logger.info(f"  Confidence: {row['confidence']:.2%}")
                    logger.info(f"  Lift: {row['lift']:.2f}")
                
                return rules
            except ValueError as e:
                logger.warning(f"Not enough itemsets to generate rules: {e}")
                return pd.DataFrame()
        else:
            logger.warning("Not enough itemsets to generate rules")
            return pd.DataFrame()
    
    def export_rules(self, rules):
        """Export rules to database"""
        logger.info("Exporting rules to database...")
        
        try:
            rules.to_sql(
                'Market_Basket_Analysis',
                self.engine,
                if_exists='replace',
                index=False
            )
            logger.info(f"✓ Exported {len(rules)} rules to Market_Basket_Analysis table")
        except Exception as e:
            logger.error(f"✗ Export failed: {e}")
    
    def run(self):
        """Run complete analysis"""
        logger.info("="*80)
        logger.info("STARTING MARKET BASKET ANALYSIS")
        logger.info("="*80)
        
        try:
            # Step 1: Extract
            df_transactions = self.extract_transactions()
            
            # Step 2: Prepare
            baskets = self.prepare_basket_data(df_transactions)
            
            # Step 3: Analyze
            rules = self.analyze_associations(baskets)
            
            # Step 4: Export
            if not rules.empty:
                self.export_rules(rules)
            
            logger.info("\n" + "="*80)
            logger.info("✓ MARKET Basket ANALYSIS COMPLETE")
            logger.info("="*80)
            
            return rules
        
        except Exception as e:
            logger.error(f"✗ Analysis failed: {e}")
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
