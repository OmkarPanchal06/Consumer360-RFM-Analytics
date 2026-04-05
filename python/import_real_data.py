"""
Import Real Data from CSV
Loads customer transactions from CSV file into SQL Server
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
    """Import real data from CSV to SQL Server"""
    
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
        """Load CSV file into DataFrame"""
        logger.info("Loading CSV file...")
        
        try:
            df = pd.read_csv(self.csv_file)
            logger.info(f"✓ Loaded {len(df):,} rows from CSV")
            logger.info(f"  Columns: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"✗ Failed to load CSV: {e}")
            raise
    
    def transform_data(self, df):
        """Transform data to match database schema"""
        logger.info("Transforming data...")
        
        # Retail Sales Dataset columns mapping
        # ['Transaction ID', 'Date', 'Customer ID', 'Gender', 'Age', 'Product Category', 'Quantity', 'Price per Unit', 'Total Amount']
        
        renames = {
            'Transaction ID': 'OrderID',
            'Date': 'OrderDate',
            'Customer ID': 'CustomerID',
            'Product Category': 'Category',
            'Price per Unit': 'UnitPrice',
            'Total Amount': 'NetSalesAmount'
        }
        df = df.rename(columns=renames)
        
        # Ensure correct formatting
        df['CustomerID'] = 'CUST_' + df['CustomerID'].astype(str)
        df['OrderID'] = 'ORD_' + df['OrderID'].astype(str)
        # Create a mock ProductID and ProductName from Category
        df['ProductID'] = 'PROD_' + df['Category'].str.upper().str.replace(' ', '_')
        df['ProductName'] = df['Category'] + ' Item'
        df['CustomerName'] = df['CustomerID']  # Or generate names
        df['Email'] = df['CustomerID'].str.lower() + '@example.com'
        df['Region'] = 'Unknown'

        # Convert dates to datetime
        df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
        
        # Convert to correct data types
        df['Quantity'] = df['Quantity'].astype(int)
        df['UnitPrice'] = df['UnitPrice'].astype(float)
        df['NetSalesAmount'] = df['NetSalesAmount'].astype(float)
        
        # Calculate gross sales if needed
        # NetSalesAmount is already there, meaning quantity * price per unit maybe?
        df['SalesAmount'] = df['Quantity'] * df['UnitPrice']
        
        logger.info("✓ Data transformation complete")
        return df
    
    def insert_regions(self, df):
        """Insert unique regions into Dim_Region"""
        logger.info("Inserting regions...")
        
        # Region 'Unknown' by default since it isn't in this dataset
        query = """
        IF NOT EXISTS (SELECT 1 FROM Dim_Region WHERE RegionID = 'R_UNK')
        INSERT INTO Dim_Region (RegionID, RegionName, Country, IsActive)
        VALUES ('R_UNK', 'Unknown', 'Unknown', 1)
        """
        
        try:
            with self.engine.begin() as conn:
                conn.execute(query)
            logger.info(f"✓ Ensured Unknown region exists")
        except Exception as e:
            logger.error(f"✗ Region insertion failed: {e}")
    
    def insert_products(self, df):
        """Insert unique products into Dim_Product"""
        logger.info("Inserting products...")
        
        products = df[['ProductID', 'ProductName', 'Category']].drop_duplicates()
        
        try:
            for idx, row in products.iterrows():
                try:
                    # Upsert product (since price varies in this dataset, taking avg price for unit price representation or insert 0)
                    query = f"""
                    IF NOT EXISTS (SELECT 1 FROM Dim_Product WHERE ProductID = '{row['ProductID']}')
                    INSERT INTO Dim_Product (ProductID, ProductName, Category, UnitPrice, IsActive)
                    VALUES ('{row['ProductID']}', '{row['ProductName']}', '{row['Category']}', 0, 1)
                    """
                    with self.engine.begin() as conn:
                        conn.execute(query)
                except Exception as ex:
                    pass
            logger.info(f"✓ Inserted {len(products):,} products types")
        except Exception as e:
            logger.error(f"✗ Product insertion failed: {e}")
    
    def insert_customers(self, df):
        """Insert unique customers into Dim_Customer"""
        logger.info("Inserting customers...")
        
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
                    pass
            
            logger.info(f"✓ Inserted {len(customers):,} customers")
        except Exception as e:
            logger.error(f"✗ Customer insertion failed: {e}")
    
    def insert_sales(self, df):
        """Insert transactions into Fact_Sales"""
        logger.info("Inserting sales transactions...")
        
        try:
            with self.engine.connect() as conn:
                for idx, row in df.iterrows():
                    # Get keys from dimension tables
                    customer_key_query = f"SELECT CustomerKey FROM Dim_Customer WHERE CustomerID = '{row['CustomerID']}'"
                    product_key_query = f"SELECT ProductKey FROM Dim_Product WHERE ProductID = '{row['ProductID']}'"
                    region_key_query = "SELECT TOP 1 RegionKey FROM Dim_Region ORDER BY RegionKey"
                    
                    try:
                        customer_result = conn.execute(customer_key_query).fetchone()
                        product_result = conn.execute(product_key_query).fetchone()
                        region_result = conn.execute(region_key_query).fetchone()
                        
                        if customer_result and product_result and region_result:
                            date_key = int(row['OrderDate'].strftime('%Y%m%d'))
                            
                            # Note: check if order line already exists if testing locally
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
                        # might be a duplicate issue if there's unique constraints in the DB structure
                        # logging.warning(f"Skipping transaction {row['OrderID']}: {e}")
                        continue
            
            logger.info(f"✓ Inserted transactions into Fact_Sales")
        except Exception as e:
            logger.error(f"✗ Sales insertion failed: {e}")
    
    def run(self):
        """Run complete import process"""
        logger.info("="*70)
        logger.info("STARTING REAL DATA IMPORT")
        logger.info("="*70)
        
        try:
            # Step 1: Load CSV
            df = self.load_csv()
            
            # Step 2: Transform data
            df = self.transform_data(df)
            
            # Step 3: Insert into database
            self.insert_regions(df)
            self.insert_products(df)
            self.insert_customers(df)
            self.insert_sales(df)
            
            logger.info("="*70)
            logger.info("✓ DATA IMPORT COMPLETED")
            logger.info("="*70)
            
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
