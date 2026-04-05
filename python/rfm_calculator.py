"""
RFMCalculatorModule
ScorescustomersonRecency,Frequency,Monetary
"""

importpandasaspd
importlogging
importos
fromconfigimportLOGS_FOLDER

logging.basicConfig(
level=logging.INFO,
format='%(asctime)s-%(levelname)s-%(message)s',
handlers=[
logging.FileHandler(os.path.join(LOGS_FOLDER,'rfm_calculator.log')),
logging.StreamHandler()
]
)
logger=logging.getLogger(__name__)


classRFMCalculator:
"""CalculatesRFMscoresforcustomers"""

def__init__(self,df):
"""
Initializewithcustomerdata
df=DataFramewithcolumns:RecencyDays,TransactionCount,TotalSpend
"""
self.df=df.copy()
logger.info("RFMCalculatorinitialized")

defcalculate_r_score(self):
"""
RecencyScore(1-5)

5=Mostrecent(0-20thpercentile)
4=20-40thpercentile
3=40-60thpercentile
2=60-80thpercentile
1=Leastrecent(80-100thpercentile)
"""
logger.info("CalculatingRecencyScore...")

#Fillmissingvalues
self.df['RecencyDays']=self.df['RecencyDays'].fillna(365)

#Lowerrecencyisbetter(morerecentisgood)
self.df['R_Score']=pd.qcut(
self.df['RecencyDays'].rank(method='first'),
q=5,
labels=[5,4,3,2,1],#Inverted
duplicates='drop'
).astype(int)

self.df['R_Score']=self.df['R_Score'].fillna(1)
logger.info(f"✓RecencyScoredistribution:\n{self.df['R_Score'].value_counts().sort_index()}")

defcalculate_f_score(self):
"""
FrequencyScore(1-5)

5=Mostfrequent(80-100thpercentile)
4=60-80thpercentile
3=40-60thpercentile
2=20-40thpercentile
1=Leastfrequent(0-20thpercentile)
"""
logger.info("CalculatingFrequencyScore...")

#Fillmissingvalues
self.df['TransactionCount']=self.df['TransactionCount'].fillna(0)

#Higherfrequencyisbetter
self.df['F_Score']=pd.qcut(
self.df['TransactionCount'].rank(method='first'),
q=5,
labels=[1,2,3,4,5],
duplicates='drop'
).astype(int)

self.df['F_Score']=self.df['F_Score'].fillna(1)
logger.info(f"✓FrequencyScoredistribution:\n{self.df['F_Score'].value_counts().sort_index()}")

defcalculate_m_score(self):
"""
MonetaryScore(1-5)

5=Highestspenders(80-100thpercentile)
4=60-80thpercentile
3=40-60thpercentile
2=20-40thpercentile
1=Lowestspenders(0-20thpercentile)
"""
logger.info("CalculatingMonetaryScore...")

#Fillmissingvalues
self.df['TotalSpend']=self.df['TotalSpend'].fillna(0)

#Higherspendisbetter
self.df['M_Score']=pd.qcut(
self.df['TotalSpend'].rank(method='first'),
q=5,
labels=[1,2,3,4,5],
duplicates='drop'
).astype(int)

self.df['M_Score']=self.df['M_Score'].fillna(1)
logger.info(f"✓MonetaryScoredistribution:\n{self.df['M_Score'].value_counts().sort_index()}")

defcalculate_combined_rfm_score(self):
"""CalculatecombinedRFMscore(weightedaverage)"""
logger.info("CalculatingCombinedRFMScore...")

#Weights(MandFmoreimportantthanR)
r_weight=1.0
f_weight=1.2
m_weight=1.5
total_weight=r_weight+f_weight+m_weight

self.df['RFM_Score']=(
(self.df['R_Score']*r_weight+
self.df['F_Score']*f_weight+
self.df['M_Score']*m_weight)/
total_weight
).round(2)

logger.info(f"✓RFMScoreStats:")
logger.info(f"Mean:{self.df['RFM_Score'].mean():.2f}")
logger.info(f"Min:{self.df['RFM_Score'].min():.2f}")
logger.info(f"Max:{self.df['RFM_Score'].max():.2f}")

defsegment_customers(self):
"""Assigncustomerstosegments"""
logger.info("Segmentingcustomers...")

defassign_segment(row):
r=row['R_Score']
f=row['F_Score']
m=row['M_Score']
rfm=row['RFM_Score']

ifrfm>=4.5:
return'Champions'
eliff>=4andm>=4:
return'LoyalCustomers'
elifr>=4andf>=3:
return'PotentialLoyalists'
elifr>=3andrfm>=3:
return'CustomersNeedingAttention'
elifr<=2andf>=3andm>=3:
return'AtRisk'
elifr<=2andf<=2:
return'Hibernating'
else:
return'New/LowValue'

self.df['Segment']=self.df.apply(assign_segment,axis=1)

#Printsummary
logger.info("\n===SEGMENTSUMMARY===")
forsegmentinself.df['Segment'].unique():
count=len(self.df[self.df['Segment']==segment])
pct=(count/len(self.df))*100
avg_spend=self.df[self.df['Segment']==segment]['TotalSpend'].mean()
logger.info(f"{segment:30s}:{count}customers({pct:.1f}%)|AvgSpend:${avg_spend:,.2f}")

defcalculate_all(self):
"""Runallcalculations"""
logger.info("="*70)
logger.info("STARTINGRFMCALCULATION")
logger.info("="*70)

self.calculate_r_score()
self.calculate_f_score()
self.calculate_m_score()
self.calculate_combined_rfm_score()
self.segment_customers()

logger.info("="*70)
logger.info("✓RFMCALCULATIONCOMPLETE")
logger.info("="*70)

returnself.df


#TEST
if__name__=="__main__":
fromdata_extractionimportDataExtractor

print("TestingRFMCalculator...\n")

#Getdata
extractor=DataExtractor()
df=extractor.extract_customer_360()

#CalculateRFM
rfm=RFMCalculator(df)
df_results=rfm.calculate_all()

#Showresults
print("\n===RESULTS===")
print(df_results[['CustomerID','R_Score','F_Score','M_Score','RFM_Score','Segment']].to_string())

extractor.close_connection()
