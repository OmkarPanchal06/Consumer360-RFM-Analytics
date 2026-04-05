"""
CohortAnalysisModule
Trackcustomerretentionandbehaviorbyacquisitioncohort
"""

importpandasaspd
importnumpyasnp
importlogging
importos
importsys

#Addpythonfoldertopathsoimportswork
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))

fromconfigimportLOGS_FOLDER,CONNECTION_STRING
fromsqlalchemyimportcreate_engine

logging.basicConfig(
level=logging.INFO,
format='%(asctime)s-%(levelname)s-%(message)s',
handlers=[
logging.FileHandler(os.path.join(LOGS_FOLDER,'cohort_analysis.log')),
logging.StreamHandler()
]
)
logger=logging.getLogger(__name__)


classCohortAnalyzer:
"""Analyzecustomerbehaviorbyacquisitioncohort"""

def__init__(self):
self.engine=create_engine(CONNECTION_STRING)
logger.info("CohortAnalyzerinitialized")

defextract_cohort_data(self):
"""Extractcustomercohortdata"""
logger.info("Extractingcohortdata...")

query="""
SELECT
c.CustomerKey,
c.CustomerID,
c.RegistrationDate,
YEAR(c.RegistrationDate)ASCohortYear,
DATEPART(QUARTER,c.RegistrationDate)ASCohortQuarter,
FORMAT(c.RegistrationDate,'yyyy-MM')ASCohortMonth,

MAX(fs.TransactionDate)ASLastPurchaseDate,
COUNT(DISTINCTfs.SalesID)ASTransactionCount,
SUM(CASEWHENfs.ReturnFlag='N'THENfs.NetSalesAmountELSE0END)ASTotalSpend,

DATEDIFF(MONTH,c.RegistrationDate,MAX(fs.TransactionDate))ASMonthsSinceCohort

FROMDim_Customerc
LEFTJOINFact_SalesfsONc.CustomerKey=fs.CustomerKey
WHEREc.IsCurrentRecord=1
GROUPBY
c.CustomerKey,c.CustomerID,c.RegistrationDate,
YEAR(c.RegistrationDate),DATEPART(QUARTER,c.RegistrationDate),
FORMAT(c.RegistrationDate,'yyyy-MM')
"""

try:
df=pd.read_sql_query(query,self.engine)
logger.info(f"✓Extracted{len(df):,}customersforcohortanalysis")
returndf
exceptExceptionase:
logger.error(f"✗Extractionfailed:{e}")
raise

defcreate_cohort_table(self,df):
"""Createcohortretentiontable"""
logger.info("Creatingcohorttables...")

#Cohortsize(customerspercohort)
cohort_data=df.groupby('CohortMonth').agg({
'CustomerKey':'count',
'TotalSpend':['sum','mean'],
'TransactionCount':'mean'
}).round(2)

cohort_data.columns=['CohortSize','TotalRevenue','AvgSpend','AvgTransactions']

logger.info(f"\n{'='*80}")
logger.info("COHORTPERFORMANCE")
logger.info(f"{'='*80}")
logger.info(cohort_data.to_string())

#Retentiontable
cohort_retention=df.groupby(['CohortMonth','MonthsSinceCohort']).size().unstack(fill_value=0)

#Converttoretentionpercentage
#Usethefirstcolumn(month0typically)asbase
first_col=cohort_retention.columns[0]
retention_pct=cohort_retention.divide(cohort_retention[first_col],axis=0)*100

logger.info(f"\n{'='*80}")
logger.info("COHORTRETENTIONRATE(%)")
logger.info(f"{'='*80}")
logger.info(retention_pct.round(1).to_string())

#Spendingbycohort
spending_by_cohort=df.groupby(['CohortMonth','MonthsSinceCohort'])['TotalSpend'].mean().unstack(fill_value=0)

logger.info(f"\n{'='*80}")
logger.info("AVERAGESPENDBYCOHORTANDAGE")
logger.info(f"{'='*80}")
logger.info(spending_by_cohort.round(2).to_string())

return{
'cohort_data':cohort_data,
'retention_pct':retention_pct,
'spending_by_cohort':spending_by_cohort
}

defanalyze_patterns(self,df):
"""Analyzecohortpatterns"""
logger.info("Analyzingcohortpatterns...")

#Whichcohorthashighestlifetimevalue?
cohort_ltv=df.groupby('CohortMonth')['TotalSpend'].sum().sort_values(ascending=False)

logger.info(f"\n{'='*80}")
logger.info("TOPCOHORTSBYLIFETIMEREVENUE")
logger.info(f"{'='*80}")
forcohort,revenueincohort_ltv.head(5).items():
logger.info(f"{cohort}:${revenue:,.2f}")

#Whichcohorthashighestretention?
cohort_retention_rate=df.groupby('CohortMonth').apply(
lambdax:(x['TransactionCount']>0).sum()/len(x)*100
).sort_values(ascending=False)

logger.info(f"\n{'='*80}")
logger.info("TOPCOHORTSBYRETENTIONRATE")
logger.info(f"{'='*80}")
forcohort,retentionincohort_retention_rate.head(5).items():
logger.info(f"{cohort}:{retention:.1f}%")

defexport_results(self,cohort_tables,df):
"""Exportcohortanalysistodatabase"""
logger.info("Exportingcohortanalysis...")

try:
#Createsummarytable
summary=cohort_tables['cohort_data'].reset_index()
summary.columns=['Cohort','CohortSize','TotalRevenue','AvgSpend','AvgTransactions']

summary.to_sql('Cohort_Summary',self.engine,if_exists='replace',index=False)
logger.info("✓Exportedcohortsummary")

#Exportretentionpercentages
#Becausecolumnsareintegers(MonthsSinceCohort),mapthemtostringsforSQL
retention=cohort_tables['retention_pct'].reset_index()
retention.columns=[str(c)forcinretention.columns]
retention.to_sql('Cohort_Retention',self.engine,if_exists='replace',index=False)
logger.info("✓Exportedretentiontable")

#Exportspending
spending=cohort_tables['spending_by_cohort'].reset_index()
spending.columns=[str(c)forcinspending.columns]
spending.to_sql('Cohort_Spending',self.engine,if_exists='replace',index=False)
logger.info("✓Exportedspendingtable")

exceptExceptionase:
logger.error(f"✗Exportfailed:{e}")

defrun(self):
"""Runcompletecohortanalysis"""
logger.info("="*80)
logger.info("STARTINGCOHORTANALYSIS")
logger.info("="*80)

try:
#Step1:Extract
df=self.extract_cohort_data()

#Step2:Createtables
cohort_tables=self.create_cohort_table(df)

#Step3:Analyzepatterns
self.analyze_patterns(df)

#Step4:Export
self.export_results(cohort_tables,df)

logger.info("\n"+"="*80)
logger.info("✓COHORTANALYSISCOMPLETE")
logger.info("="*80)

exceptExceptionase:
logger.error(f"✗Analysisfailed:{e}")
raise

finally:
self.engine.dispose()


if__name__=="__main__":
analyzer=CohortAnalyzer()
analyzer.run()
