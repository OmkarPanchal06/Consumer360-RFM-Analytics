"""
Data Extraction Module
Gets customer data from SQL Server
"""

import pandas as pd
import logging
from sqlalchemy import create_engine
from config import CONNECTION_STRING, LOGS_FOLDER
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, 'data_extraction.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts customer data from SQL Server"""

    def __init__(self):
        """Connect to database"""
        try:
            self.engine = create_engine(CONNECTION_STRING)
            logger.info("✓ Database connection established")
        except Exception as e:
            logger.error(f"✗ Connection failed: {e}")
            raise

    def extract_customer_360(self):
        """Extract Customer 360 view data"""
        query = """
        SELECT *
        FROM vw_Customer360_SingleView
        WHERE TotalSpend > 0
        ORDER BY TotalSpend DESC
        """

        try:
            df = pd.read_sql_query(query, self.engine)
            logger.info(f"✓ Extracted {len(df):,} customer records")
            logger.info(f"  Columns: {list(df.columns)}")
            return df

        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            raise

    def close_connection(self):
        """Close database connection"""
        self.engine.dispose()
        logger.info("Connection closed")


# TEST THE MODULE
if __name__ == "__main__":
    print("Testing Data Extraction...")

    extractor = DataExtractor()
    df = extractor.extract_customer_360()

    print(f"\nData shape: {df.shape}")
    print(f"\nFirst 3 rows:")
    print(df.head(3))

    extractor.close_connection()