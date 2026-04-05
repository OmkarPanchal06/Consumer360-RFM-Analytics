"""
Configuration Module for Consumer360 Project

This module centralizes the environment configuration for the data pipeline.
It handles database connection strings, absolute folder paths, and analytical 
constants using the py-dotenv library for security and flexibility.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file to ensure system credentials are never hardcoded.
load_dotenv()

# --- Database Connection Parameters ---
SQL_SERVER = os.getenv('SQL_SERVER', 'localhost')
DATABASE = os.getenv('DATABASE', 'Consumer360_DB')
DB_USERNAME = os.getenv('DB_USERNAME', '')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DRIVER = 'ODBC Driver 17 for SQL Server'

# Construct SQLAlchemy connection URI based on the presence of a database username.
# Windows Authentication is used as the default if no credentials are found in .env.
if DB_USERNAME:
    CONNECTION_STRING = (
        f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{SQL_SERVER}/{DATABASE}"
        f"?driver={DRIVER}"
    )
else:
    CONNECTION_STRING = (
        f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}"
        f"?driver={DRIVER}&trusted_connection=yes"
    )

# --- Filesystem Mapping ---
# Calculate the project root directory relative to this config file.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_FOLDER = os.path.join(PROJECT_ROOT, 'sql')
LOGS_FOLDER = os.path.join(PROJECT_ROOT, 'logs')
DATA_FOLDER = os.path.join(PROJECT_ROOT, 'data')

# Ensure target directories exist before pipeline execution.
os.makedirs(LOGS_FOLDER, exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# --- Analytical Hyperparameters ---
# Define scoring weights for the Recency, Frequency, and Monetary components.
RFM_WEIGHTS = {
    'recency_weight': 1.0,
    'frequency_weight': 1.2,
    'monetary_weight': 1.5
}

# --- Logging and Reporting ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
OUTPUT_TABLE_NAME = 'RFM_Results_Output'