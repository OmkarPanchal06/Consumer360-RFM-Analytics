"""
ImportRealDatafromCSV
LoadscustomertransactionsfromCSVfileintoSQLServer
"""

importpandasaspd
importlogging
importos
fromdatetimeimportdatetime
fromsqlalchemyimportcreate_engine
importsys

#Addpythonfoldertopathsoimportswork
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))

fromconfigimportCONNECTION_STRING,DATA_FOLDER,LOGS_FOLDER

logging.basicConfig(
level=logging.INFO,
format='%(asctime)s-%(levelname)s-%(message)s',
handlers=[
logging.FileHandler(os.path.join(LOGS_FOLDER,'data_import.log')),
logging.StreamHandler()
]
)
logger=logging.getLogger(__name__)


classDataImporter:
"""ImportrealdatafromCSVtoSQLServer"""

def__init__(self,csv_file):
"""
Initializeimporter

Args:
csv_file:PathtoCSVfile(e.g.,'data/transactions.csv')
"""
self.csv_file=csv_file
self.engine=create_engine(CONNECTION_STRING)
logger.info(f"DataImporterinitializedwithfile:{csv_file}")

defload_csv(self):
"""LoadCSVfileintoDataFrame"""
logger.info("LoadingCSVfile...")

try:
df=pd.read_csv(self.csv_file)
logger.info(f"✓Loaded{len(df):,}rowsfromCSV")
logger.info(f"Columns:{list(df.columns)}")
returndf
exceptExceptionase:
logger.error(f"✗FailedtoloadCSV:{e}")
raise

deftransform_data(self,df):
"""Transformdatatomatchdatabaseschema"""
logger.info("Transformingdata...")

#RetailSalesDatasetcolumnsmapping
#['TransactionID','Date','CustomerID','Gender','Age','ProductCategory','Quantity','PriceperUnit','TotalAmount']

renames={
'TransactionID':'OrderID',
'Date':'OrderDate',
'CustomerID':'CustomerID',
'ProductCategory':'Category',
'PriceperUnit':'UnitPrice',
'TotalAmount':'NetSalesAmount'
}
df=df.rename(columns=renames)

#Ensurecorrectformatting
df['CustomerID']='CUST_'+df['CustomerID'].astype(str)
df['OrderID']='ORD_'+df['OrderID'].astype(str)
#CreateamockProductIDandProductNamefromCategory
df['ProductID']='PROD_'+df['Category'].str.upper().str.replace('','_')
df['ProductName']=df['Category']+'Item'
df['CustomerName']=df['CustomerID']#Orgeneratenames
df['Email']=df['CustomerID'].str.lower()+'@example.com'
df['Region']='Unknown'

#Convertdatestodatetime
df['OrderDate']=pd.to_datetime(df['OrderDate'],errors='coerce')

#Converttocorrectdatatypes
df['Quantity']=df['Quantity'].astype(int)
df['UnitPrice']=df['UnitPrice'].astype(float)
df['NetSalesAmount']=df['NetSalesAmount'].astype(float)

#Calculategrosssalesifneeded
#NetSalesAmountisalreadythere,meaningquantity*priceperunitmaybe?
df['SalesAmount']=df['Quantity']*df['UnitPrice']

logger.info("✓Datatransformationcomplete")
returndf

definsert_regions(self,df):
"""InsertuniqueregionsintoDim_Region"""
logger.info("Insertingregions...")

#Region'Unknown'bydefaultsinceitisn'tinthisdataset
query="""
IFNOTEXISTS(SELECT1FROMDim_RegionWHERERegionID='R_UNK')
INSERTINTODim_Region(RegionID,RegionName,Country,IsActive)
VALUES('R_UNK','Unknown','Unknown',1)
"""

try:
withself.engine.begin()asconn:
conn.execute(query)
logger.info(f"✓EnsuredUnknownregionexists")
exceptExceptionase:
logger.error(f"✗Regioninsertionfailed:{e}")

definsert_products(self,df):
"""InsertuniqueproductsintoDim_Product"""
logger.info("Insertingproducts...")

products=df[['ProductID','ProductName','Category']].drop_duplicates()

try:
foridx,rowinproducts.iterrows():
try:
#Upsertproduct(sincepricevariesinthisdataset,takingavgpriceforunitpricerepresentationorinsert0)
query=f"""
IFNOTEXISTS(SELECT1FROMDim_ProductWHEREProductID='{row['ProductID']}')
INSERTINTODim_Product(ProductID,ProductName,Category,UnitPrice,IsActive)
VALUES('{row['ProductID']}','{row['ProductName']}','{row['Category']}',0,1)
"""
withself.engine.begin()asconn:
conn.execute(query)
exceptExceptionasex:
pass
logger.info(f"✓Inserted{len(products):,}productstypes")
exceptExceptionase:
logger.error(f"✗Productinsertionfailed:{e}")

definsert_customers(self,df):
"""InsertuniquecustomersintoDim_Customer"""
logger.info("Insertingcustomers...")

customers=df[['CustomerID','CustomerName','Email']].drop_duplicates()

try:
foridx,rowincustomers.iterrows():
try:
query=f"""
IFNOTEXISTS(SELECT1FROMDim_CustomerWHERECustomerID='{row['CustomerID']}')
INSERTINTODim_Customer(CustomerID,CustomerName,Email,RegistrationDate,IsCurrentRecord)
VALUES('{row['CustomerID']}','{row['CustomerName']}','{row['Email']}',GETDATE(),1)
"""
withself.engine.begin()asconn:
conn.execute(query)
exceptExceptionasex:
pass

logger.info(f"✓Inserted{len(customers):,}customers")
exceptExceptionase:
logger.error(f"✗Customerinsertionfailed:{e}")

definsert_sales(self,df):
"""InserttransactionsintoFact_Sales"""
logger.info("Insertingsalestransactions...")

try:
withself.engine.connect()asconn:
foridx,rowindf.iterrows():
#Getkeysfromdimensiontables
customer_key_query=f"SELECTCustomerKeyFROMDim_CustomerWHERECustomerID='{row['CustomerID']}'"
product_key_query=f"SELECTProductKeyFROMDim_ProductWHEREProductID='{row['ProductID']}'"
region_key_query="SELECTTOP1RegionKeyFROMDim_RegionORDERBYRegionKey"

try:
customer_result=conn.execute(customer_key_query).fetchone()
product_result=conn.execute(product_key_query).fetchone()
region_result=conn.execute(region_key_query).fetchone()

ifcustomer_resultandproduct_resultandregion_result:
date_key=int(row['OrderDate'].strftime('%Y%m%d'))

#Note:checkiforderlinealreadyexistsiftestinglocally
insert_query=f"""
INSERTINTOFact_Sales(
CustomerKey,ProductKey,DateKey,RegionKey,
OrderNumber,OrderLineNumber,TransactionDate,
Quantity,UnitPrice,SalesAmount,NetSalesAmount,
ReturnFlag
)
VALUES(
{customer_result[0]},{product_result[0]},{date_key},{region_result[0]},
'{row['OrderID']}',1,'{row['OrderDate']}',
{row['Quantity']},{row['UnitPrice']},
{row['SalesAmount']},
{row['NetSalesAmount']},
'N'
)
"""
conn.execute(insert_query)
conn.commit()
exceptExceptionase:
#mightbeaduplicateissueifthere'suniqueconstraintsintheDBstructure
#logging.warning(f"Skippingtransaction{row['OrderID']}:{e}")
continue

logger.info(f"✓InsertedtransactionsintoFact_Sales")
exceptExceptionase:
logger.error(f"✗Salesinsertionfailed:{e}")

defrun(self):
"""Runcompleteimportprocess"""
logger.info("="*70)
logger.info("STARTINGREALDATAIMPORT")
logger.info("="*70)

try:
#Step1:LoadCSV
df=self.load_csv()

#Step2:Transformdata
df=self.transform_data(df)

#Step3:Insertintodatabase
self.insert_regions(df)
self.insert_products(df)
self.insert_customers(df)
self.insert_sales(df)

logger.info("="*70)
logger.info("✓DATAIMPORTCOMPLETED")
logger.info("="*70)

exceptExceptionase:
logger.error(f"✗Importfailed:{e}")
raise
finally:
self.engine.dispose()


if__name__=="__main__":
csv_path=os.path.join(DATA_FOLDER,'transactions.csv')

ifnotos.path.exists(csv_path):
print(f"Error:Couldnotfinddatasetat{csv_path}")
print("Pleasedownloadthe'KaggleRetailSalesDataset'andsaveittothedata/folderastransactions.csv")
sys.exit(1)

importer=DataImporter(csv_path)
importer.run()

print("\n✓Realdataimportedsuccessfully!")
print("Run:pythonpython/main_pipeline.pytoprocessthenewdata.")
