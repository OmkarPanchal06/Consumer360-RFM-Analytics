"""
Consumer360-DashboardGenerator
Generates:
1.InteractiveHTMLdashboard(openinanybrowser)
2.ExcelfilereadyforPowerBIDesktopimport
"""

importos
importsys
importpandasaspd
importplotly.graph_objectsasgo
importplotly.expressaspx
fromplotly.subplotsimportmake_subplots
importjson

#Addpythonfoldertopathsoimportswork
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))

fromconfigimportCONNECTION_STRING,PROJECT_ROOT
fromdata_extractionimportDataExtractor
fromrfm_calculatorimportRFMCalculator
fromclv_calculatorimportCLVCalculator
fromsqlalchemyimportcreate_engine

OUTPUT_DIR=os.path.join(PROJECT_ROOT,"powerbi")
os.makedirs(OUTPUT_DIR,exist_ok=True)

#─────────────────────────────────────────────
#COLORPALETTE
#─────────────────────────────────────────────
COLORS={
"Champions":"#6C63FF",
"LoyalCustomers":"#3ECFCF",
"PotentialLoyalists":"#56D364",
"CustomersNeedingAttention":"#F0A500",
"AtRisk":"#FF6B6B",
"Hibernating":"#9CA3AF",
"New/LowValue":"#60A5FA",
}
BG="#0F172A"
CARD_BG="#1E293B"
TEXT="#F1F5F9"
SUBTEXT="#94A3B8"
ACCENT="#6C63FF"
GREEN="#56D364"
RED="#FF6B6B"
AMBER="#F0A500"


#─────────────────────────────────────────────
#STEP1:LOADDATA
#─────────────────────────────────────────────
defload_data():
print("LoadingdatafromSQLServer...")
extractor=DataExtractor()
df=extractor.extract_customer_360()
extractor.close_connection()

rfm=RFMCalculator(df)
df=rfm.calculate_all()

clv=CLVCalculator(df)
df=clv.calculate_clv()

print(f"Loaded{len(df)}customers")
returndf


#─────────────────────────────────────────────
#STEP2:EXPORTEXCEL
#─────────────────────────────────────────────
defexport_excel(df):
path=os.path.join(OUTPUT_DIR,"Consumer360_PowerBI_Data.xlsx")
print(f"ExportingExcelto{path}...")

withpd.ExcelWriter(path,engine="openpyxl")aswriter:
#Sheet1–Fullcustomerdata
df.to_excel(writer,sheet_name="Customer_RFM_CLV",index=False)

#Sheet2–Segmentsummary
seg=(
df.groupby("Segment")
.agg(
Customers=("CustomerID","count"),
Avg_RFM_Score=("RFM_Score","mean"),
Total_Spend=("TotalSpend","sum"),
Avg_CLV=("CLV_Predicted","mean"),
)
.reset_index()
.sort_values("Avg_RFM_Score",ascending=False)
)
seg.to_excel(writer,sheet_name="Segment_Summary",index=False)

#Sheet3–RFMscoresonly
rfm_cols=[cforcin["CustomerID","CustomerName","R_Score","F_Score","M_Score",
"RFM_Score","Segment","RecencyDays",
"TransactionCount","TotalSpend"]ifcindf.columns]
df[rfm_cols].to_excel(writer,sheet_name="RFM_Scores",index=False)

#Sheet4–CLV
clv_cols=[cforcin["CustomerID","CustomerName","Segment",
"AvgOrderValue","CLV_Predicted"]ifcindf.columns]
df[clv_cols].to_excel(writer,sheet_name="CLV_Data",index=False)

print("Excelexported!")
returnseg


#─────────────────────────────────────────────
#STEP3:BUILDPLOTLYHTMLDASHBOARD
#─────────────────────────────────────────────
defbuild_dashboard(df,seg):
path=os.path.join(OUTPUT_DIR,"Consumer360_Dashboard.html")
print(f"BuildingHTMLdashboard...")

#──KPIvalues────────────────────────────
total_customers=len(df)
total_revenue=df["TotalSpend"].sum()if"TotalSpend"indf.columnselse0
avg_clv=df["CLV_Predicted"].mean()if"CLV_Predicted"indf.columnselse0
top_segment=seg.iloc[0]["Segment"]iflen(seg)>0else"N/A"
avg_rfm=df["RFM_Score"].mean()if"RFM_Score"indf.columnselse0
champ_pct=(
len(df[df["Segment"].isin(["Champions","LoyalCustomers"])])/total_customers*100
iftotal_customers>0else0
)

seg_colors=[COLORS.get(s,ACCENT)forsinseg["Segment"]]

#──Chart1:Segmentdonut─────────────────
fig_donut=go.Figure(go.Pie(
labels=seg["Segment"],
values=seg["Customers"],
hole=0.55,
marker=dict(colors=seg_colors,line=dict(color=BG,width=2)),
textinfo="label+percent",
textfont=dict(color=TEXT,size=11),
))
fig_donut.update_layout(
paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
font=dict(color=TEXT),
showlegend=False,
margin=dict(t=10,b=10,l=10,r=10),
height=300,
annotations=[dict(text=f"<b>{total_customers}</b><br>Customers",
x=0.5,y=0.5,font_size=16,font_color=TEXT,
showarrow=False)]
)

#──Chart2:RFMScorebar─────────────────
fig_rfm=go.Figure(go.Bar(
x=seg["Segment"],
y=seg["Avg_RFM_Score"].round(2),
marker=dict(color=seg_colors,line=dict(color=BG,width=1)),
text=seg["Avg_RFM_Score"].round(2),
textposition="outside",
textfont=dict(color=TEXT),
))
fig_rfm.update_layout(
paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
font=dict(color=TEXT),
xaxis=dict(showgrid=False,tickfont=dict(size=10,color=TEXT)),
yaxis=dict(showgrid=True,gridcolor="#334155",range=[0,6]),
margin=dict(t=10,b=80,l=40,r=10),
height=300,
)

#──Chart3:CLVbysegment────────────────
fig_clv=go.Figure(go.Bar(
x=seg["Segment"],
y=seg["Avg_CLV"].round(0),
marker=dict(color=seg_colors,line=dict(color=BG,width=1)),
text=["$"+f"{v:,.0f}"forvinseg["Avg_CLV"]],
textposition="outside",
textfont=dict(color=TEXT),
))
fig_clv.update_layout(
paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
font=dict(color=TEXT),
xaxis=dict(showgrid=False,tickfont=dict(size=10,color=TEXT)),
yaxis=dict(showgrid=True,gridcolor="#334155"),
margin=dict(t=10,b=80,l=60,r=10),
height=300,
)

#──Chart4:ScatterRvsMscore──────────
scatter_colors=[COLORS.get(s,ACCENT)forsindf["Segment"]]
fig_scatter=go.Figure(go.Scatter(
x=df["R_Score"],
y=df["M_Score"],
mode="markers+text",
marker=dict(size=16,color=scatter_colors,
line=dict(color=BG,width=1)),
text=df["CustomerName"]if"CustomerName"indf.columnselsedf["CustomerID"],
textposition="topcenter",
textfont=dict(color=TEXT,size=10),
customdata=df[["Segment","RFM_Score"]],
hovertemplate=(
"<b>%{text}</b><br>"
"Segment:%{customdata[0]}<br>"
"RScore:%{x}MScore:%{y}<br>"
"RFMScore:%{customdata[1]:.2f}<extra></extra>"
)
))
fig_scatter.update_layout(
paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
font=dict(color=TEXT),
xaxis=dict(title="RecencyScore",showgrid=True,gridcolor="#334155",
range=[0,6],tickvals=[1,2,3,4,5]),
yaxis=dict(title="MonetaryScore",showgrid=True,gridcolor="#334155",
range=[0,6],tickvals=[1,2,3,4,5]),
margin=dict(t=10,b=50,l=60,r=10),
height=320,
)

#──Chart5:Spendtreemap─────────────────
fig_tree=px.treemap(
seg,
path=["Segment"],
values="Total_Spend",
color="Avg_RFM_Score",
color_continuous_scale=[[0,"#334155"],[0.5,ACCENT],[1,GREEN]],
custom_data=["Customers","Avg_CLV"],
)
fig_tree.update_traces(
hovertemplate=(
"<b>%{label}</b><br>"
"TotalSpend:$%{value:,.0f}<br>"
"Customers:%{customdata[0]}<br>"
"AvgCLV:$%{customdata[1]:,.0f}<extra></extra>"
),
textfont=dict(color="white",size=13),
)
fig_tree.update_layout(
paper_bgcolor="rgba(0,0,0,0)",
margin=dict(t=5,b=5,l=5,r=5),
height=280,
coloraxis_showscale=False,
)

#──ConvertfigstoJSON───────────────────
deffig_json(fig):
returnfig.to_json()

#──Customertablerows────────────────────
table_cols=["CustomerName","Segment","R_Score","F_Score","M_Score",
"RFM_Score","TotalSpend","CLV_Predicted"]
table_cols=[cforcintable_colsifcindf.columns]
table_df=df[table_cols].copy()

defseg_badge(s):
c=COLORS.get(s,ACCENT)
returnf'<spanstyle="background:{c}22;color:{c};padding:2px10px;border-radius:12px;font-size:11px;border:1pxsolid{c}55">{s}</span>'

rows_html=""
for_,rintable_df.iterrows():
name=r.get("CustomerName",r.get("CustomerID",""))
seg_b=seg_badge(r.get("Segment",""))
r_s=int(r.get("R_Score",0))
f_s=int(r.get("F_Score",0))
m_s=int(r.get("M_Score",0))
rfm_s=f'{r.get("RFM_Score",0):.2f}'
spend=f'${r.get("TotalSpend",0):,.2f}'
clv=f'${r.get("CLV_Predicted",0):,.2f}'
rows_html+=f"""
<tr>
<td>{name}</td>
<td>{seg_b}</td>
<td><spanclass="score">{r_s}</span></td>
<td><spanclass="score">{f_s}</span></td>
<td><spanclass="score">{m_s}</span></td>
<td><bstyle="color:{ACCENT}">{rfm_s}</b></td>
<td>{spend}</td>
<tdstyle="color:{GREEN}">{clv}</td>
</tr>"""

#──Legenditems──────────────────────────
legend_html=""
fors,cinCOLORS.items():
count=len(df[df["Segment"]==s])ifsindf["Segment"].valueselse0
ifcount>0:
legend_html+=f'<divclass="legend-item"><spanstyle="width:10px;height:10px;border-radius:50%;background:{c};display:inline-block;margin-right:6px"></span>{s}<spanstyle="color:{SUBTEXT}">({count})</span></div>'

#─────────────────────────────────────────
#HTMLTEMPLATE
#─────────────────────────────────────────
html=f"""<!DOCTYPEhtml>
<htmllang="en">
<head>
<metacharset="UTF-8">
<metaname="viewport"content="width=device-width,initial-scale=1.0">
<title>Consumer360—AnalyticsDashboard</title>
<scriptsrc="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<linkhref="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:'Inter',sans-serif;background:{BG};color:{TEXT};min-height:100vh;}}

/*──HEADER──*/
.header{{
background:linear-gradient(135deg,#1E1B4B0%,#0F172A60%,#0D1B2A100%);
padding:28px40px;
border-bottom:1pxsolid#1E293B;
display:flex;align-items:center;justify-content:space-between;
}}
.header-lefth1{{font-size:24px;font-weight:700;color:{TEXT};}}
.header-leftp{{font-size:13px;color:{SUBTEXT};margin-top:4px;}}
.header-badge{{
background:{ACCENT}22;color:{ACCENT};
padding:6px16px;border-radius:20px;
font-size:12px;font-weight:600;
border:1pxsolid{ACCENT}44;
}}

/*──KPICARDS──*/
.kpi-row{{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;padding:24px40px0;}}
.kpi{{
background:{CARD_BG};border-radius:12px;
padding:20px;border:1pxsolid#334155;
transition:transform0.2s,box-shadow0.2s;
}}
.kpi:hover{{transform:translateY(-2px);box-shadow:08px30pxrgba(108,99,255,0.15);}}
.kpi-label{{font-size:11px;color:{SUBTEXT};text-transform:uppercase;letter-spacing:0.05em;}}
.kpi-value{{font-size:26px;font-weight:700;margin:6px02px;}}
.kpi-sub{{font-size:11px;color:{SUBTEXT};}}

/*──GRID──*/
.grid-2{{display:grid;grid-template-columns:1fr1fr;gap:16px;padding:20px40px0;}}
.grid-3{{display:grid;grid-template-columns:1fr1fr1fr;gap:16px;padding:040px;}}
.grid-full{{padding:16px40px;}}

/*──CARD──*/
.card{{
background:{CARD_BG};border-radius:12px;
border:1pxsolid#334155;padding:20px;
transition:box-shadow0.2s;
}}
.card:hover{{box-shadow:04px20pxrgba(0,0,0,0.3);}}
.card-title{{font-size:13px;font-weight:600;color:{SUBTEXT};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:12px;}}

/*──LEGEND──*/
.legend{{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:12px;}}
.legend-item{{font-size:12px;color:{TEXT};}}

/*──TABLE──*/
.table-wrap{{overflow-x:auto;border-radius:12px;}}
table{{width:100%;border-collapse:collapse;font-size:13px;}}
theadtr{{background:#334155;}}
theadth{{padding:12px16px;text-align:left;font-weight:600;color:{SUBTEXT};font-size:11px;text-transform:uppercase;letter-spacing:0.05em;}}
tbodytr{{border-bottom:1pxsolid#1E293B;transition:background0.15s;}}
tbodytr:hover{{background:#334155;}}
tbodytd{{padding:12px16px;}}
.score{{
display:inline-block;width:28px;height:28px;line-height:28px;
text-align:center;border-radius:6px;
background:{ACCENT}22;color:{ACCENT};font-weight:700;font-size:13px;
}}

/*──FOOTER──*/
.footer{{text-align:center;padding:30px;color:{SUBTEXT};font-size:12px;margin-top:20px;}}
.footera{{color:{ACCENT};text-decoration:none;}}
</style>
</head>
<body>

<!--HEADER-->
<divclass="header">
<divclass="header-left">
<h1>Consumer360&mdash;RFMAnalyticsDashboard</h1>
<p>CustomerLifetimeValue&amp;SegmentationEngine</p>
</div>
<spanclass="header-badge">LIVEDATA&bull;{total_customers}Customers</span>
</div>

<!--KPICARDS-->
<divclass="kpi-row">
<divclass="kpi">
<divclass="kpi-label">TotalCustomers</div>
<divclass="kpi-value"style="color:{ACCENT}">{total_customers:,}</div>
<divclass="kpi-sub">Across{len(seg)}segments</div>
</div>
<divclass="kpi">
<divclass="kpi-label">TotalRevenue</div>
<divclass="kpi-value"style="color:{GREEN}">${total_revenue:,.0f}</div>
<divclass="kpi-sub">Alltransactions</div>
</div>
<divclass="kpi">
<divclass="kpi-label">AvgPredictedCLV</div>
<divclass="kpi-value"style="color:{AMBER}">${avg_clv:,.0f}</div>
<divclass="kpi-sub">5-yearforecast</div>
</div>
<divclass="kpi">
<divclass="kpi-label">AvgRFMScore</div>
<divclass="kpi-value"style="color:{TEXT}">{avg_rfm:.2f}<spanstyle="font-size:14px;color:{SUBTEXT}">/5</span></div>
<divclass="kpi-sub">WeightedR·F·M</div>
</div>
<divclass="kpi">
<divclass="kpi-label">High-ValueRate</div>
<divclass="kpi-value"style="color:{GREEN}">{champ_pct:.0f}%</div>
<divclass="kpi-sub">Champions+Loyal</div>
</div>
</div>

<!--ROW1:Donut+RFMBar-->
<divclass="grid-2"style="margin-top:16px">
<divclass="card">
<divclass="card-title">CustomerSegments</div>
<divclass="legend">{legend_html}</div>
<divid="chart-donut"></div>
</div>
<divclass="card">
<divclass="card-title">AvgRFMScorebySegment</div>
<divid="chart-rfm"></div>
</div>
</div>

<!--ROW2:CLVBar+Scatter-->
<divclass="grid-2"style="padding:16px40px0">
<divclass="card">
<divclass="card-title">AvgPredictedCLVbySegment(5-Year)</div>
<divid="chart-clv"></div>
</div>
<divclass="card">
<divclass="card-title">RecencyvsMonetaryMap</div>
<divid="chart-scatter"></div>
</div>
</div>

<!--ROW3:Treemapfull-->
<divclass="grid-full"style="margin-top:0">
<divclass="card">
<divclass="card-title">RevenueTreemap—SegmentShareofTotalSpend</div>
<divid="chart-tree"></div>
</div>
</div>

<!--ROW4:CustomerTable-->
<divclass="grid-full">
<divclass="card">
<divclass="card-title">CustomerDetailTable</div>
<divclass="table-wrap">
<table>
<thead>
<tr>
<th>Customer</th>
<th>Segment</th>
<th>R</th><th>F</th><th>M</th>
<th>RFMScore</th>
<th>TotalSpend</th>
<th>CLV(5yr)</th>
</tr>
</thead>
<tbody>{rows_html}</tbody>
</table>
</div>
</div>
</div>

<divclass="footer">
Consumer360RFMAnalytics&bull;
<ahref="https://github.com/OmkarPanchal06/Consumer360-RFM-Analytics"target="_blank">GitHub</a>
&bull;BuiltwithPython+Plotly
</div>

<script>
varcfg={{responsive:true,displayModeBar:false}};
Plotly.newPlot('chart-donut',{fig_json(fig_donut)}.data,{fig_json(fig_donut)}.layout,cfg);
Plotly.newPlot('chart-rfm',{fig_json(fig_rfm)}.data,{fig_json(fig_rfm)}.layout,cfg);
Plotly.newPlot('chart-clv',{fig_json(fig_clv)}.data,{fig_json(fig_clv)}.layout,cfg);
Plotly.newPlot('chart-scatter',{fig_json(fig_scatter)}.data,{fig_json(fig_scatter)}.layout,cfg);
Plotly.newPlot('chart-tree',{fig_json(fig_tree)}.data,{fig_json(fig_tree)}.layout,cfg);
</script>
</body>
</html>"""

withopen(path,"w",encoding="utf-8")asf:
f.write(html)

print(f"Dashboardsaved!")
returnpath


#─────────────────────────────────────────────
#MAIN
#─────────────────────────────────────────────
if__name__=="__main__":
print("="*60)
print("CONSUMER360—DASHBOARDGENERATOR")
print("="*60)

df=load_data()
seg=export_excel(df)
html_path=build_dashboard(df,seg)

print()
print("="*60)
print("DONE!")
print(f"Dashboard:powerbi\\Consumer360_Dashboard.html")
print(f"Excel:powerbi\\Consumer360_PowerBI_Data.xlsx")
print("="*60)
print()
print("Openthedashboard:double-clickConsumer360_Dashboard.html")
print("ForPowerBI:File>GetData>Excel>Consumer360_PowerBI_Data.xlsx")
