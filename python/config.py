"""
Configuration for Consumer360 Project
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================
# DATABASE CONFIGURATION
# ============================================================

SQL_SERVER = os.getenv('SQL_SERVER', 'localhost')
DATABASE = os.getenv('DATABASE', 'Consumer360_DB')
DB_USERNAME = os.getenv('DB_USERNAME', 'sa')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DRIVER = 'ODBC Driver 17 for SQL Server'

# Connection string for SQLAlchemy
CONNECTION_STRING = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{SQL_SERVER}/{DATABASE}?driver={DRIVER}'

# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_FOLDER = os.path.join(PROJECT_ROOT, 'sql')
LOGS_FOLDER = os.path.join(PROJECT_ROOT, 'logs')
DATA_FOLDER = os.path.join(PROJECT_ROOT, 'data')

# Create folders if they don't exist
os.makedirs(LOGS_FOLDER, exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# ============================================================
# RFM PARAMETERS
# ============================================================

RFM_WEIGHTS = {
    'recency_weight': 1.0,
    'frequency_weight': 1.2,
    'monetary_weight': 1.5
}

# ============================================================
# LOGGING
# ============================================================

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# ============================================================
# EXPORT SETTINGS
# ============================================================

OUTPUT_TABLE_NAME = 'RFM_Results_Output'