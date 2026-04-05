"""
MainETLPipeline
Runseverythinginorder
"""

importlogging
importos
fromdatetimeimportdatetime
fromconfigimportLOGS_FOLDER,CONNECTION_STRING,OUTPUT_TABLE_NAME
fromdata_extractionimportDataExtractor
fromrfm_calculatorimportRFMCalculator
fromclv_calculatorimportCLVCalculator
frommarket_basket_analysisimportMarketBasketAnalyzer
fromcohort_analysisimportCohortAnalyzer
fromsqlalchemyimportcreate_engine

logging.basicConfig(
level=logging.INFO,
format='%(asctime)s-%(levelname)s-%(message)s',
handlers=[
logging.FileHandler(os.path.join(LOGS_FOLDER,'main_pipeline.log')),
logging.StreamHandler()
]
)
logger=logging.getLogger(__name__)


classConsumer360Pipeline:
"""Mainpipelineorchestration"""

def__init__(self):
self.engine=create_engine(CONNECTION_STRING)
logger.info("Pipelineinitialized")

defstep1_extract(self):
"""Step1:Extractdata"""
logger.info("\n"+"="*70)
logger.info("STEP1:DATAEXTRACTION")
logger.info("="*70)

extractor=DataExtractor()
self.df=extractor.extract_customer_360()
extractor.close_connection()

returnself.df

defstep2_rfm(self):
"""Step2:CalculateRFM"""
logger.info("\n"+"="*70)
logger.info("STEP2:RFMSEGMENTATION")
logger.info("="*70)

rfm=RFMCalculator(self.df)
self.df=rfm.calculate_all()

returnself.df

defstep3_clv(self):
"""Step3:CalculateCLV"""
logger.info("\n"+"="*70)
logger.info("STEP3:CLVCALCULATION")
logger.info("="*70)

clv=CLVCalculator(self.df)
self.df=clv.calculate_clv()

returnself.df

defstep4_export(self):
"""Step4:ExporttoSQL"""
logger.info("\n"+"="*70)
logger.info("STEP4:EXPORTRESULTS")
logger.info("="*70)

#Selectkeycolumns
export_cols=[
'CustomerKey','CustomerID','CustomerName','Email',
'RegistrationDate','RecencyDays','TransactionCount','TotalSpend',
'AvgOrderValue','CustomerTenureDays','CohortYearMonth',
'PrimaryRegion','Country','State',
'R_Score','F_Score','M_Score','RFM_Score',
'Segment','CLV_Predicted','LastPurchaseDate'
]

export_df=self.df[[colforcolinexport_colsifcolinself.df.columns]]

#ExporttoSQL
export_df.to_sql(OUTPUT_TABLE_NAME,self.engine,if_exists='replace',index=False)

logger.info(f"✓Exported{len(export_df):,}recordsto{OUTPUT_TABLE_NAME}")
logger.info(f"Columns:{len(export_df.columns)}")

defstep5_market_basket(self):
"""Step5:MarketBasketAnalysis"""
logger.info("\n"+"="*70)
logger.info("STEP5:MARKETBASKETANALYSIS")
logger.info("="*70)

analyzer=MarketBasketAnalyzer()
self.basket_rules=analyzer.run()

returnself.basket_rules

defstep6_cohort_analysis(self):
"""Step6:CohortAnalysis"""
logger.info("\n"+"="*70)
logger.info("STEP6:COHORTANALYSIS")
logger.info("="*70)

analyzer=CohortAnalyzer()
analyzer.run()

defrun(self):
"""Runcompletepipeline"""
logger.info("="*70)
logger.info("CONSUMER360RFMPIPELINE")
logger.info(f"Started:{datetime.now().strftime('%Y-%m-%d%H:%M:%S')}")
logger.info("="*70)

try:
self.step1_extract()
self.step2_rfm()
self.step3_clv()
self.step4_export()
self.step5_market_basket()
self.step6_cohort_analysis()

logger.info("\n"+"="*70)
logger.info("✓PIPELINECOMPLETEDSUCCESSFULLY")
logger.info(f"Completed:{datetime.now().strftime('%Y-%m-%d%H:%M:%S')}")
logger.info("="*70)

returnTrue

exceptExceptionase:
logger.error(f"\n✗PIPELINEFAILED:{e}")
raise

finally:
self.engine.dispose()


#RUNPIPELINE
if__name__=="__main__":
pipeline=Consumer360Pipeline()
pipeline.run()
