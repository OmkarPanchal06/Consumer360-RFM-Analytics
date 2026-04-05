"""
ConfigurationforConsumer360Project
"""

importos
fromdotenvimportload_dotenv

#Loadenvironmentvariablesfrom.envfile
load_dotenv()

#============================================================
#DATABASECONFIGURATION
#============================================================

SQL_SERVER=os.getenv('SQL_SERVER','localhost')
DATABASE=os.getenv('DATABASE','Consumer360_DB')
DB_USERNAME=os.getenv('DB_USERNAME','')
DB_PASSWORD=os.getenv('DB_PASSWORD','')
DRIVER='ODBCDriver17forSQLServer'

#Buildconnectionstring:
#-IfDB_USERNAMEisset,useSQLServerAuthentication
#-Otherwise,useWindowsAuthentication(trusted_connection)
ifDB_USERNAME:
CONNECTION_STRING=(
f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{SQL_SERVER}/{DATABASE}"
f"?driver={DRIVER}"
)
else:
#WindowsAuthentication(defaultforlocalinstalls)
CONNECTION_STRING=(
f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}"
f"?driver={DRIVER}&trusted_connection=yes"
)

#============================================================
#PROJECTPATHS
#============================================================

PROJECT_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_FOLDER=os.path.join(PROJECT_ROOT,'sql')
LOGS_FOLDER=os.path.join(PROJECT_ROOT,'logs')
DATA_FOLDER=os.path.join(PROJECT_ROOT,'data')

#Createfoldersiftheydon'texist
os.makedirs(LOGS_FOLDER,exist_ok=True)
os.makedirs(DATA_FOLDER,exist_ok=True)

#============================================================
#RFMPARAMETERS
#============================================================

RFM_WEIGHTS={
'recency_weight':1.0,
'frequency_weight':1.2,
'monetary_weight':1.5
}

#============================================================
#LOGGING
#============================================================

LOG_LEVEL=os.getenv('LOG_LEVEL','INFO')
LOG_FORMAT='%(asctime)s-%(name)s-%(levelname)s-%(message)s'

#============================================================
#EXPORTSETTINGS
#============================================================

OUTPUT_TABLE_NAME='RFM_Results_Output'
