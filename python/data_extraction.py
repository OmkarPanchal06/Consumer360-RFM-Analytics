"""
Data Extraction Module

This module provides the DataExtractor class, which handles the secure extraction 
of customer-related data from the SQL Server database. It leverages SQLAlchemy 
engines and Pandas for efficient data frame construction.
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
        """
        Initializes the SQLAlchemy engine using the project's connection string.
        
        Raises:
            Exception: If the database connection cannot be established.
        """
        try:
            self.engine = create_engine(CONNECTION_STRING)
            logger.info("Database connection established.")
        except Exception as e:
            logger.error(f"Failed to establish database connection: {e}")
            raise

    def extract_customer_360(self):
        """
        Extracts the 'Customer 360' unified view from the database.
        
        The view aggregates historical transactions, customer demographics, 
        and regional metadata into a single flat structure.

        Returns:
            pd.DataFrame: A DataFrame containing the extracted customer records.
        
        Raises:
            Exception: If the SQL query execution or data loading fails.
        """
        query = """
        SELECT *
        FROM vw_Customer360_SingleView
        WHERE TotalSpend > 0
        ORDER BY TotalSpend DESC
        """

        try:
            df = pd.read_sql_query(query, self.engine)
            logger.info(f"Extracted {len(df):,} customer records.")
            logger.info(f"Fields available: {list(df.columns)}")
            return df

        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            raise

    def close_connection(self):
        """
        Gracefully disposes of the SQLAlchemy engine and closes active connections.
        """
        self.engine.dispose()
        logger.info("Database connection pool disposed.")


# TEST THE MODULE
if __name__ == "__main__":
    print("Testing Data Extraction...")

    extractor = DataExtractor()
    df = extractor.extract_customer_360()

    print(f"\nData shape: {df.shape}")
    print(f"\nFirst 3 rows:")
    print(df.head(3))

    extractor.close_connection()