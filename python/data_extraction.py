"""
DataExtractionModule
GetscustomerdatafromSQLServer
"""

importpandasaspd
importlogging
fromsqlalchemyimportcreate_engine
fromconfigimportCONNECTION_STRING,LOGS_FOLDER
importos

#Setuplogging
logging.basicConfig(
level=logging.INFO,
format='%(asctime)s-%(levelname)s-%(message)s',
handlers=[
logging.FileHandler(os.path.join(LOGS_FOLDER,'data_extraction.log')),
logging.StreamHandler()
]
)
logger=logging.getLogger(__name__)


classDataExtractor:
"""ExtractscustomerdatafromSQLServer"""

def__init__(self):
"""Connecttodatabase"""
try:
self.engine=create_engine(CONNECTION_STRING)
logger.info("✓Databaseconnectionestablished")
exceptExceptionase:
logger.error(f"✗Connectionfailed:{e}")
raise

defextract_customer_360(self):
"""ExtractCustomer360viewdata"""
query="""
SELECT*
FROMvw_Customer360_SingleView
WHERETotalSpend>0
ORDERBYTotalSpendDESC
"""

try:
df=pd.read_sql_query(query,self.engine)
logger.info(f"✓Extracted{len(df):,}customerrecords")
logger.info(f"Columns:{list(df.columns)}")
returndf

exceptExceptionase:
logger.error(f"✗Extractionfailed:{e}")
raise

defclose_connection(self):
"""Closedatabaseconnection"""
self.engine.dispose()
logger.info("Connectionclosed")


#TESTTHEMODULE
if__name__=="__main__":
print("TestingDataExtraction...")

extractor=DataExtractor()
df=extractor.extract_customer_360()

print(f"\nDatashape:{df.shape}")
print(f"\nFirst3rows:")
print(df.head(3))

extractor.close_connection()
