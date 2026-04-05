"""
GenerateExecutivePresentationDeck
HighlightsChurnRiskcustomersandoverallsegments
"""

importos
importsys
importpandasaspd
fromsqlalchemyimportcreate_engine
frompptximportPresentation
frompptx.utilimportInches,Pt
frompptx.enum.textimportPP_ALIGN
frompptx.dml.colorimportRGBColor

#Addpythonfoldertopathsoimportswork
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
fromconfigimportCONNECTION_STRING,PROJECT_ROOT

OUTPUT_DIR=os.path.join(PROJECT_ROOT,"powerbi")
os.makedirs(OUTPUT_DIR,exist_ok=True)
PPT_PATH=os.path.join(OUTPUT_DIR,"Consumer360_Executive_Deck.pptx")

#Connectandgetdata
engine=create_engine(CONNECTION_STRING)
try:
df=pd.read_sql("SELECT*FROMRFM_Results_Output",engine)
exceptExceptionase:
print(f"Errorreadingfromdatabase:{e}")
print("Tryingtorunwithoutdatabaseinfooryoumayneedtorunthepipelinefirst.")
sys.exit(1)
finally:
engine.dispose()

#Processdata
total_customers=len(df)
total_revenue=df['TotalSpend'].sum()if'TotalSpend'indf.columnselse0
avg_clv=df['CLV_Predicted'].mean()if'CLV_Predicted'indf.columnselse0

#AtRiskCustomers
churn_risk=df[df['Segment'].isin(['AtRisk','Hibernating'])].copy()
ifchurn_risk.empty:
#Fallbacktosomelogicifthereareno"AtRisk"literallynamed
churn_risk=df[df['RFM_Score']<2.5]

churn_risk=churn_risk.sort_values(by='TotalSpend',ascending=False).head(10)

defadd_title_slide(prs):
slide_layout=prs.slide_layouts[0]
slide=prs.slides.add_slide(slide_layout)
title=slide.shapes.title
subtitle=slide.placeholders[1]

title.text="Consumer360RFMAnalytics"
subtitle.text="ExecutiveSummary&ChurnRiskAnalysis\nGeneratedAutomatically"

defadd_summary_slide(prs):
slide_layout=prs.slide_layouts[1]
slide=prs.slides.add_slide(slide_layout)
title=slide.shapes.title
title.text="ExecutiveSummary"

body_shape=slide.shapes.placeholders[1]
tf=body_shape.text_frame

p=tf.add_paragraph()
p.text=f"TotalCustomersAnalyzed:{total_customers:,}"
p.font.size=Pt(24)

p2=tf.add_paragraph()
p2.text=f"TotalRevenueProcessed:${total_revenue:,.2f}"
p2.font.size=Pt(24)

p3=tf.add_paragraph()
p3.text=f"AveragePredictedCLV(5-Year):${avg_clv:,.2f}"
p3.font.size=Pt(24)

p4=tf.add_paragraph()
p4.text=f"HighRiskCustomersIdentified:{len(churn_risk)}topcustomersatrisk"
p4.font.size=Pt(24)
p4.font.color.rgb=RGBColor(255,0,0)

defadd_churn_risk_slide(prs):
slide_layout=prs.slide_layouts[5]#blankwithtitle
slide=prs.slides.add_slide(slide_layout)
title=slide.shapes.title
title.text="ActionableList:TopChurnRiskCustomers"

rows=len(churn_risk)+1
cols=5
left=Inches(0.5)
top=Inches(2.0)
width=Inches(9.0)
height=Inches(0.8)

table=slide.shapes.add_table(rows,cols,left,top,width,height).table

#Headers
headers=["CustomerName","Segment","RFMScore","TotalSpend","PredictedCLV"]
foridx,headerinenumerate(headers):
cell=table.cell(0,idx)
cell.text=header
forparagraphincell.text_frame.paragraphs:
forruninparagraph.runs:
run.font.bold=True
run.font.size=Pt(14)

#Data
forrow_idx,(_,row)inenumerate(churn_risk.iterrows(),start=1):
table.cell(row_idx,0).text=str(row.get('CustomerName','Unknown'))
table.cell(row_idx,1).text=str(row.get('Segment','Unknown'))
table.cell(row_idx,2).text=f"{row.get('RFM_Score',0):.2f}"
table.cell(row_idx,3).text=f"${row.get('TotalSpend',0):,.2f}"
table.cell(row_idx,4).text=f"${row.get('CLV_Predicted',0):,.2f}"

forcol_idxinrange(cols):
forparagraphintable.cell(row_idx,col_idx).text_frame.paragraphs:
forruninparagraph.runs:
run.font.size=Pt(12)

#Createpresentation
prs=Presentation()
add_title_slide(prs)
add_summary_slide(prs)
add_churn_risk_slide(prs)

prs.save(PPT_PATH)
print("="*60)
print(f"SUCCESS!ExecutivePresentationDeckgeneratedat:\n{PPT_PATH}")
print("="*60)
