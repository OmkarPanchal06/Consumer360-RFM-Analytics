"""
MarketBasketAnalysisModule
UsesApriorialgorithmtofindproductassociations
"""

importpandasaspd
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
logging.FileHandler(os.path.join(LOGS_FOLDER,'market_basket.log')),
logging.StreamHandler()
]
)
logger=logging.getLogger(__name__)


classMarketBasketAnalyzer:
"""MarketbasketanalysisusingApriorialgorithm"""

def__init__(self):
self.engine=create_engine(CONNECTION_STRING)
logger.info("MarketBasketAnalyzerinitialized")

defextract_transactions(self):
"""Extracttransactiondatafromdatabase"""
logger.info("Extractingtransactiondata...")

query="""
SELECT
fs.OrderNumber,
p.ProductName,
p.Category
FROMFact_Salesfs
JOINDim_ProductpONfs.ProductKey=p.ProductKey
WHEREfs.ReturnFlag='N'
ORDERBYfs.OrderNumber
"""

try:
df=pd.read_sql_query(query,self.engine)
logger.info(f"✓Extracted{len(df):,}transactionitems")
logger.info(f"Uniqueorders:{df['OrderNumber'].nunique():,}")
returndf
exceptExceptionase:
logger.error(f"✗Extractionfailed:{e}")
raise

defprepare_basket_data(self,df):
"""Converttransactiondatatobasketformat"""
logger.info("Preparingbasketdata...")

#Groupbyordertogetproductspertransaction
baskets=df.groupby('OrderNumber')['ProductName'].apply(list).values.tolist()

logger.info(f"✓Prepared{len(baskets)}transactionbaskets")
logger.info(f"Averageitemsperbasket:{df.groupby('OrderNumber').size().mean():.1f}")

returnbaskets

defanalyze_associations(self,baskets):
"""Analyzeproductassociations"""
logger.info("Analyzingproductassociations...")

try:
frommlxtend.frequent_patternsimportapriori,association_rules
frommlxtend.preprocessingimportTransactionEncoder
exceptImportError:
logger.error("mlxtendnotinstalled.Run:pipinstallmlxtend")
raise

#Encodebaskets
te=TransactionEncoder()
te_ary=te.fit(baskets).transform(baskets)
df_encoded=pd.DataFrame(te_ary,columns=te.columns_)

logger.info(f"✓Encoded{len(df_encoded)}transactionswith{len(te.columns_)}products")

#ApplyApriori
min_support=0.001#Loweredmin_supportsincekaggleretaildatasethasrandomassortmentofitemsandwewanttofindsomething
frequent_itemsets=apriori(df_encoded,min_support=min_support,use_colnames=True)

logger.info(f"✓Found{len(frequent_itemsets)}frequentitemsets")

#Generaterules
iflen(frequent_itemsets)>1:
min_confidence=0.01#Loweredconfidencethreshold
try:
rules=association_rules(frequent_itemsets,metric="confidence",min_threshold=min_confidence)

#Convertfrozensetstostrings
rules['antecedents']=rules['antecedents'].apply(lambdax:','.join(list(x)))
rules['consequents']=rules['consequents'].apply(lambdax:','.join(list(x)))

#Sortbylift(strengthofassociation)
rules=rules.sort_values('lift',ascending=False)

logger.info(f"✓Generated{len(rules)}associationrules")
logger.info("\nTopRules(byLift):")
logger.info("="*80)

foridx,rowinrules.head(10).iterrows():
logger.info(f"\n{row['antecedents']}→{row['consequents']}")
logger.info(f"Support:{row['support']:.2%}")
logger.info(f"Confidence:{row['confidence']:.2%}")
logger.info(f"Lift:{row['lift']:.2f}")

returnrules
exceptValueErrorase:
logger.warning(f"Notenoughitemsetstogeneraterules:{e}")
returnpd.DataFrame()
else:
logger.warning("Notenoughitemsetstogeneraterules")
returnpd.DataFrame()

defexport_rules(self,rules):
"""Exportrulestodatabase"""
logger.info("Exportingrulestodatabase...")

try:
rules.to_sql(
'Market_Basket_Analysis',
self.engine,
if_exists='replace',
index=False
)
logger.info(f"✓Exported{len(rules)}rulestoMarket_Basket_Analysistable")
exceptExceptionase:
logger.error(f"✗Exportfailed:{e}")

defrun(self):
"""Runcompleteanalysis"""
logger.info("="*80)
logger.info("STARTINGMARKETBASKETANALYSIS")
logger.info("="*80)

try:
#Step1:Extract
df_transactions=self.extract_transactions()

#Step2:Prepare
baskets=self.prepare_basket_data(df_transactions)

#Step3:Analyze
rules=self.analyze_associations(baskets)

#Step4:Export
ifnotrules.empty:
self.export_rules(rules)

logger.info("\n"+"="*80)
logger.info("✓MARKETBasketANALYSISCOMPLETE")
logger.info("="*80)

returnrules

exceptExceptionase:
logger.error(f"✗Analysisfailed:{e}")
raise

finally:
self.engine.dispose()


if__name__=="__main__":
analyzer=MarketBasketAnalyzer()
rules=analyzer.run()

ifnotrules.empty:
print("\n"+"="*80)
print("Top5Rules:")
print(rules[['antecedents','consequents','support','confidence','lift']].head(5))
