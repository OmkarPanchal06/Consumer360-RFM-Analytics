"""
CLVCalculatorModule
Predictscustomerlifetimevalue
"""

importpandasaspd
importlogging
importos
fromconfigimportLOGS_FOLDER

logging.basicConfig(
level=logging.INFO,
format='%(asctime)s-%(levelname)s-%(message)s',
handlers=[
logging.FileHandler(os.path.join(LOGS_FOLDER,'clv_calculator.log')),
logging.StreamHandler()
]
)
logger=logging.getLogger(__name__)


classCLVCalculator:
"""CalculatesCustomerLifetimeValue"""

def__init__(self,df,lifespan_years=5):
"""
InitializeCLVCalculator

Args:
df:DataFramewithcustomerdata
lifespan_years:Howmanyyearstopredict(default:5years)
"""
self.df=df.copy()
self.lifespan_years=lifespan_years
logger.info(f"CLVCalculatorinitialized(lifespan:{lifespan_years}years)")

defcalculate_clv(self):
"""
SimpleCLVFormula:
CLV=AverageOrderValue×PurchaseFrequencyperYear×CustomerLifespan

Where:
-AverageOrderValue=TotalSpend/NumberofOrders
-PurchaseFrequency=OrdersperYear
-Lifespan=Expectedyearsascustomer
"""
logger.info("CalculatingCLV...")

#Fillmissingvalues
self.df['AvgOrderValue']=self.df['AvgOrderValue'].fillna(0)
self.df['PurchaseCount']=self.df['PurchaseCount'].fillna(1)
self.df['CustomerTenureDays']=self.df['CustomerTenureDays'].fillna(365)

#Annualrevenuepercustomer
self.df['AnnualRevenue']=(
self.df['AvgOrderValue']*
(self.df['PurchaseCount']/(self.df['CustomerTenureDays']/365+1))
).round(2)

#CLV=AnnualRevenue×Lifespan
self.df['CLV_Predicted']=(
self.df['AnnualRevenue']*self.lifespan_years
).round(2)

logger.info(f"✓CLVcalculated")
logger.info(f"AverageCLV:${self.df['CLV_Predicted'].mean():,.2f}")
logger.info(f"MedianCLV:${self.df['CLV_Predicted'].median():,.2f}")
logger.info(f"MaxCLV:${self.df['CLV_Predicted'].max():,.2f}")

returnself.df


#TEST
if__name__=="__main__":
fromdata_extractionimportDataExtractor
fromrfm_calculatorimportRFMCalculator

print("TestingCLVCalculator...\n")

#Getdata
extractor=DataExtractor()
df=extractor.extract_customer_360()

#CalculateRFM
rfm=RFMCalculator(df)
df=rfm.calculate_all()

#CalculateCLV
clv=CLVCalculator(df,lifespan_years=5)
df=clv.calculate_clv()

#Showresults
print("\n===RESULTS===")
print(df[['CustomerID','Segment','CLV_Predicted']].to_string())

extractor.close_connection()
