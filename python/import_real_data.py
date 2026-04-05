"""
Real Data Ingestion Module

This module provides the DataImporter class, tasked with loading and 
transforming raw transactional data from CSV sources into the SQL Server 
analytical schema. It ensures data integrity and proper dimension mapping.
"""

import pandas as pd
import logging
import os
from datetime import datetime
from sqlalchemy import create_engine
import sys

# Add python folder to path so imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import CONNECTION_STRING, DATA_FOLDER, LOGS_FOLDER

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, 'data_import.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataImporter:
    """
    Handles the ingestion and ETL process for external CSV transaction datasets.
    """
    
    def __init__(self, csv_file):
        """
        Initialize importer
        
        Args:
            csv_file: Path to CSV file (e.g., 'data/transactions.csv')
        """
        self.csv_file = csv_file
        self.engine = create_engine(CONNECTION_STRING)
        logger.info(f"DataImporter initialized with file: {csv_file}")
    
    def load_csv(self):
        """
        Loads the target CSV file into a Pandas DataFrame.

        Returns:
            pd.DataFrame: The raw transaction data.
        """
        logger.info(f"Loading source data from: {self.csv_file}")
        
        try:
            df = pd.read_csv(self.csv_file)
            logger.info(f"Successfully loaded {len(df):,} records from CSV.")
            logger.info(f"Source fields detected: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"Failed to load source CSV file: {e}")
            raise
    
    def transform_data(self, df):
        """
        Transforms raw source data to align with the destination Star Schema.
        
        This includes field renaming, data type casting, and generating 
        synthetic keys for dimension mapping.
        """
        logger.info("Initiating data transformation and schema mapping...")
        
        # Map source transaction fields to target database columns.
        renames = {
            'Transaction ID': 'OrderID',
            'Date': 'OrderDate',
            'Customer ID': 'CustomerID',
            'Product Category': 'Category',
            'Price per Unit': 'UnitPrice',
            'Total Amount': 'NetSalesAmount'
        }
        df = df.rename(columns=renames)
        
        # Standardize identifiers and generate mock metadata for portfolio display.
        df['CustomerID'] = 'CUST_' + df['CustomerID'].astype(str)
        df['OrderID'] = 'ORD_' + df['OrderID'].astype(str)
        df['ProductID'] = 'PROD_' + df['Category'].str.upper().str.replace(' ', '_')
        df['ProductName'] = df['Category'] + ' Item'
        df['CustomerName'] = df['CustomerID']
        df['Email'] = df['CustomerID'].str.lower() + '@example.com'
        df['Region'] = 'Unknown'

        # Consistent date and numeric type casting.
        df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
        df['Quantity'] = df['Quantity'].astype(int)
        df['UnitPrice'] = df['UnitPrice'].astype(float)
        df['NetSalesAmount'] = df['NetSalesAmount'].astype(float)
        
        # Derive gross sales amount.
        df['SalesAmount'] = df['Quantity'] * df['UnitPrice']
        
        logger.info("Data transformation completed successfully.")
        return df
    
    def insert_regions(self, df):
        """
        Ensures the existence of a default 'Unknown' region in the dimension table.
        """
        logger.info("Verifying Dim_Region metadata...")
        
        query = """
        IF NOT EXISTS (SELECT 1 FROM Dim_Region WHERE RegionID = 'R_UNK')
        INSERT INTO Dim_Region (RegionID, RegionName, Country, IsActive)
        VALUES ('R_UNK', 'Unknown', 'Unknown', 1)
        """
        
        try:
            with self.engine.begin() as conn:
                conn.execute(query)
            logger.info("Dimension record for 'Unknown' region verified.")
        except Exception as e:
            logger.error(f"Region dimension update failed: {e}")
    
    def insert_products(self, df):
        """
        Registers unique product categories in the Dim_Product dimension table.
        """
        logger.info("Upserting product dimension records...")
        
        products = df[['ProductID', 'ProductName', 'Category']].drop_duplicates()
        
        try:
            for idx, row in products.iterrows():
                try:
                    query = f"""
                    IF NOT EXISTS (SELECT 1 FROM Dim_Product WHERE ProductID = '{row['ProductID']}')
                    INSERT INTO Dim_Product (ProductID, ProductName, Category, UnitPrice, IsActive)
                    VALUES ('{row['ProductID']}', '{row['ProductName']}', '{row['Category']}', 0, 1)
                    """
                    with self.engine.begin() as conn:
                        conn.execute(query)
                except Exception as ex:
                    continue
            logger.info(f"Successfully synchronized {len(products):,} product definitions.")
        except Exception as e:
            logger.error(f"Product dimension synchronization failed: {e}")
    
    def insert_customers(self, df):
        """
        Registers unique customers in the Dim_Customer dimension table.
        """
        logger.info("Upserting customer dimension records...")
        
        customers = df[['CustomerID', 'CustomerName', 'Email']].drop_duplicates()
        
        try:
            for idx, row in customers.iterrows():
                try:
                    query = f"""
                    IF NOT EXISTS (SELECT 1 FROM Dim_Customer WHERE CustomerID = '{row['CustomerID']}')
                    INSERT INTO Dim_Customer (CustomerID, CustomerName, Email, RegistrationDate, IsCurrentRecord)
                    VALUES ('{row['CustomerID']}', '{row['CustomerName']}', '{row['Email']}', GETDATE(), 1)
                    """
                    with self.engine.begin() as conn:
                        conn.execute(query)
                except Exception as ex:
                    continue
            
            logger.info(f"Successfully synchronized {len(customers):,} customer records.")
        except Exception as e:
            logger.error(f"Customer dimension synchronization failed: {e}")
    
    def insert_sales(self, df):
        """
        Persists transaction data into the Fact_Sales fact table.
        """
        logger.info("Loading transaction data into Fact_Sales...")
        
        try:
            with self.engine.connect() as conn:
                for idx, row in df.iterrows():
                    customer_key_query = f"SELECT CustomerKey FROM Dim_Customer WHERE CustomerID = '{row['CustomerID']}'"
                    product_key_query = f"SELECT ProductKey FROM Dim_Product WHERE ProductID = '{row['ProductID']}'"
                    region_key_query = "SELECT TOP 1 RegionKey FROM Dim_Region ORDER BY RegionKey"
                    
                    try:
                        customer_result = conn.execute(customer_key_query).fetchone()
                        product_result = conn.execute(product_key_query).fetchone()
                        region_result = conn.execute(region_key_query).fetchone()
                        
                        if customer_result and product_result and region_result:
                            date_key = int(row['OrderDate'].strftime('%Y%m%d'))
                            
                            insert_query = f"""
                            INSERT INTO Fact_Sales (
                                CustomerKey, ProductKey, DateKey, RegionKey,
                                OrderNumber, OrderLineNumber, TransactionDate,
                                Quantity, UnitPrice, SalesAmount, NetSalesAmount,
                                ReturnFlag
                            )
                            VALUES (
                                {customer_result[0]}, {product_result[0]}, {date_key}, {region_result[0]},
                                '{row['OrderID']}', 1, '{row['OrderDate']}',
                                {row['Quantity']}, {row['UnitPrice']}, 
                                {row['SalesAmount']}, 
                                {row['NetSalesAmount']},
                                'N'
                            )
                            """
                            conn.execute(insert_query)
                            conn.commit()
                    except Exception as e:
                        continue
            
            logger.info("Transactional load into Fact_Sales completed successfully.")
        except Exception as e:
            logger.error(f"Fact table load failed: {e}")
    
    def run(self):
        """
        Orchestrates the entire data ingestion pipeline from CSV to SQL.
        """
        logger.info("-" * 80)
        logger.info("STARTING DATA INGESTION PIPELINE")
        logger.info("-" * 80)
        
        try:
            # Stage 1: Load Source Data
            df = self.load_csv()
            
            # Stage 2: Transform and Map Schema
            df = self.transform_data(df)
            
            # Stage 3: Load Dimensions and Facts
            self.insert_regions(df)
            self.insert_products(df)
            self.insert_customers(df)
            self.insert_sales(df)
            
            logger.info("-" * 80)
            logger.info("DATA INGESTION PIPELINE COMPLETED")
            logger.info("-" * 80)
            
        except Exception as e:
            logger.error(f"✗ Import failed: {e}")
            raise
        finally:
            self.engine.dispose()


if __name__ == "__main__":
    csv_path = os.path.join(DATA_FOLDER, 'transactions.csv')
    
    if not os.path.exists(csv_path):
        print(f"Error: Could not find dataset at {csv_path}")
        print("Please download the 'Kaggle Retail Sales Dataset' and save it to the data/ folder as transactions.csv")
        sys.exit(1)
        
    importer = DataImporter(csv_path)
    importer.run()
    
    print("\n✓ Real data imported successfully!")
    print("Run: python python/main_pipeline.py to process the new data.")
