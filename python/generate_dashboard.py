"""
Consumer360 Dashboard Generation Module

This module automates the creation of high-fidelity analytical deliverables:
1. An interactive HTML-based dashboard utilizing the Plotly library.
2. A structured Excel workbook optimized for direct import into Power BI.
"""

import os
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json

# Add python folder to path so imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import CONNECTION_STRING, PROJECT_ROOT
from data_extraction import DataExtractor
from rfm_calculator import RFMCalculator
from clv_calculator import CLVCalculator
from sqlalchemy import create_engine

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "powerbi")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# COLOR PALETTE (Premium Design System)
# ─────────────────────────────────────────────
COLORS = {
    "Champions":                "#8B5CF6",  # Violet
    "Loyal Customers":          "#06B6D4",  # Cyan
    "Potential Loyalists":      "#10B981",  # Emerald
    "Customers Needing Attention": "#F59E0B", # Amber
    "At Risk":                  "#EF4444",  # Red
    "Hibernating":              "#64748B",  # Slate
    "New/Low Value":            "#3B82F6",  # Blue
}
BG        = "#0B0F19"
CARD_BG   = "rgba(30, 41, 59, 0.5)"
TEXT      = "#F8FAFC"
SUBTEXT   = "#94A3B8"
ACCENT    = "#8B5CF6"
GREEN     = "#10B981"
RED       = "#EF4444"
AMBER     = "#F59E0B"

# Icons (SVG paths for Heroicons)
ICONS = {
    "users": '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M15 19.128a9.38 9.38 0 0 0 2.625.372 9.337 9.337 0 0 0 4.121-.952 4.125 4.125 0 0 0-7.533-2.493M15 19.128v-.003c0-1.113-.285-2.16-.786-3.07M15 19.128v.106A12.318 12.318 0 0 1 8.624 21c-2.331 0-4.512-.645-6.374-1.766l-.001-.109a6.375 6.375 0 0 1 11.964-3.07M12 6.375a3.375 3.375 0 1 1-6.75 0 3.375 3.375 0 0 1 6.75 0Zm8.25 2.25a2.625 2.625 0 1 1-5.25 0 2.625 2.625 0 0 1 5.25 0Z" /></svg>',
    "revenue": '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M12 6v12m-3-2.818.879.659c1.171.879 3.07.879 4.242 0 1.172-.879 1.172-2.303 0-3.182C13.536 12.219 12.768 12 12 12c-.725 0-1.45-.22-2.003-.659-1.106-.879-1.106-2.303 0-3.182s2.9-.879 4.006 0l.415.33M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" /></svg>',
    "clv": '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M2.25 18.75a60.07 60.07 0 0 1 15.797 2.101c.727.198 1.453-.342 1.453-1.096V18.75M3.75 4.5v.75A.75.75 0 0 1 3 6h-.75m0 0v-.375c0-.621.504-1.125 1.125-1.125H20.25M2.25 6v9m18-10.5v.75c0 .414.336.75.75.75h.75m-1.5-1.5h.375c.621 0 1.125.504 1.125 1.125v9.75c0 .621-.504 1.125-1.125 1.125h-.375m1.5-1.5H21a.75.75 0 0 0-.75.75v.75m0 0H3.75m0 0h-.375a1.125 1.125 0 0 1-1.125-1.125V15m1.5 1.5v-.75A.75.75 0 0 0 3 15h-.75M15 10.5a3 3 0 1 1-6 0 3 3 0 0 1 6 0Zm3 0h.008v.008H18V10.5Zm-12 0h.008v.008H6V10.5Z" /></svg>',
    "chart": '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" /></svg>',
}


# ─────────────────────────────────────────────
# STEP 1: LOAD DATA
# ─────────────────────────────────────────────
def load_data():
    """
    Consolidates data from SQL Server and executes the analytical pipeline.

    Returns:
        pd.DataFrame: A fully enriched customer dataset.
    """
    print("Extracting consolidated customer metrics from SQL Server...")
    extractor = DataExtractor()
    df = extractor.extract_customer_360()
    extractor.close_connection()

    # Step: Execute RFM segmentation logic.
    rfm = RFMCalculator(df)
    df = rfm.calculate_all()

    # Step: Execute CLV prediction modeling.
    clv = CLVCalculator(df)
    df = clv.calculate_clv()

    print(f"Dataset successfully loaded with {len(df)} customer records.")
    return df


# ─────────────────────────────────────────────
# STEP 2: EXPORT EXCEL
# ─────────────────────────────────────────────
def export_excel(df):
    """
    Exports the analytical results to a multi-sheet Excel workbook.
    """
    path = os.path.join(OUTPUT_DIR, "Consumer360_PowerBI_Data.xlsx")
    print(f"Generating structured Excel deliverable: {path}")

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        # Sheet 1 – Full customer data
        df.to_excel(writer, sheet_name="Customer_RFM_CLV", index=False)

        # Sheet 2 – Segment summary
        seg = (
            df.groupby("Segment")
            .agg(
                Customers=("CustomerID", "count"),
                Avg_RFM_Score=("RFM_Score", "mean"),
                Total_Spend=("TotalSpend", "sum"),
                Avg_CLV=("CLV_Predicted", "mean"),
            )
            .reset_index()
            .sort_values("Avg_RFM_Score", ascending=False)
        )
        seg.to_excel(writer, sheet_name="Segment_Summary", index=False)

        # Sheet 3 – RFM scores only
        rfm_cols = [c for c in ["CustomerID", "CustomerName", "R_Score", "F_Score", "M_Score",
                                 "RFM_Score", "Segment", "RecencyDays",
                                 "TransactionCount", "TotalSpend"] if c in df.columns]
        df[rfm_cols].to_excel(writer, sheet_name="RFM_Scores", index=False)

        # Sheet 4 – CLV
        clv_cols = [c for c in ["CustomerID", "CustomerName", "Segment",
                                  "AvgOrderValue", "CLV_Predicted"] if c in df.columns]
        df[clv_cols].to_excel(writer, sheet_name="CLV_Data", index=False)

    print("  Excel exported!")
    return seg


# ─────────────────────────────────────────────
# STEP 3: BUILD PLOTLY HTML DASHBOARD
# ─────────────────────────────────────────────
def build_dashboard(df, seg):
    path = os.path.join(OUTPUT_DIR, "Consumer360_Dashboard.html")
    print(f"Building Premium HTML dashboard...")

    # ── KPI values ──
    total_customers = len(df)
    total_revenue   = df["TotalSpend"].sum() if "TotalSpend" in df.columns else 0
    avg_clv         = df["CLV_Predicted"].mean() if "CLV_Predicted" in df.columns else 0
    avg_rfm         = df["RFM_Score"].mean() if "RFM_Score" in df.columns else 0
    champ_pct       = (len(df[df["Segment"].isin(["Champions", "Loyal Customers"])]) / total_customers * 100) if total_customers > 0 else 0

    seg_colors = [COLORS.get(s, ACCENT) for s in seg["Segment"]]

    # ── Charts ──
    # 1. Segment Donut
    fig_donut = go.Figure(go.Pie(
        labels=seg["Segment"], values=seg["Customers"], hole=0.7,
        marker=dict(colors=seg_colors, line=dict(color=BG, width=3)),
        textinfo="percent", textfont=dict(color=TEXT, size=12),
        hoverinfo="label+value+percent"
    ))
    fig_donut.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT, family="Inter"), showlegend=False,
        margin=dict(t=10, b=10, l=10, r=10), height=320,
        annotations=[dict(text=f"<b>{total_customers}</b><br>Total", x=0.5, y=0.5, font_size=18, showarrow=False)]
    )

    # 2. RFM Bar
    fig_rfm = go.Figure(go.Bar(
        x=seg["Segment"], y=seg["Avg_RFM_Score"].round(2),
        marker=dict(color=seg_colors, opacity=0.8),
        text=seg["Avg_RFM_Score"].round(2), textposition="outside", textfont=dict(color=TEXT)
    ))
    fig_rfm.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT, family="Inter"),
        xaxis=dict(showgrid=False, tickangle=-45), yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        margin=dict(t=30, b=100, l=40, r=10), height=350
    )

    # 3. CLV Bar
    fig_clv = go.Figure(go.Bar(
        x=seg["Segment"], y=seg["Avg_CLV"].round(0),
        marker=dict(color=seg_colors, opacity=0.8),
        text=["$" + f"{v:,.0f}" for v in seg["Avg_CLV"]], textposition="outside"
    ))
    fig_clv.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT, family="Inter"),
        xaxis=dict(showgrid=False, tickangle=-45), yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        margin=dict(t=30, b=100, l=60, r=10), height=350
    )

    # 4. RFM Scatter
    scatter_colors = [COLORS.get(s, ACCENT) for s in df["Segment"]]
    fig_scatter = go.Figure(go.Scatter(
        x=df["R_Score"], y=df["M_Score"], mode="markers",
        marker=dict(size=12, color=scatter_colors, opacity=0.6, line=dict(color=BG, width=0.5)),
        text=df["CustomerName"], hovertemplate="<b>%{text}</b><br>R: %{x}, M: %{y}<extra></extra>"
    ))
    fig_scatter.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT, family="Inter"),
        xaxis=dict(title="Recency Score", gridcolor="rgba(255,255,255,0.05)", range=[0.5, 5.5], tickvals=[1,2,3,4,5]),
        yaxis=dict(title="Monetary Score", gridcolor="rgba(255,255,255,0.05)", range=[0.5, 5.5], tickvals=[1,2,3,4,5]),
        margin=dict(t=40, b=40, l=60, r=40), height=400
    )

    # 5. Spend Treemap
    fig_tree = px.treemap(
        seg, path=["Segment"], values="Total_Spend",
        color="Avg_RFM_Score", color_continuous_scale=[[0, "#334155"], [1, ACCENT]]
    )
    fig_tree.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", margin=dict(t=5, b=5, l=5, r=5),
        height=320, coloraxis_showscale=False
    )

    # ── HTML Components ──
    def fig_json(fig): return fig.to_json()

    def seg_badge(s):
        c = COLORS.get(s, ACCENT)
        return f'<span class="badge" style="background:{c}22;color:{c};border:1px solid {c}55">{s}</span>'

    rows_html = ""
    # Sort by RFM Score to show top customers first by default
    for _, r in df.sort_values("RFM_Score", ascending=False).head(100).iterrows():
        name  = r.get("CustomerName", r.get("CustomerID", ""))
        seg_b = seg_badge(r.get("Segment", ""))
        rows_html += f"""
        <tr>
          <td class="font-medium">{name}</td>
          <td>{seg_b}</td>
          <td><div class="score-pill">{int(r.get('R_Score',0))}</div></td>
          <td><div class="score-pill">{int(r.get('F_Score',0))}</div></td>
          <td><div class="score-pill">{int(r.get('M_Score',0))}</div></td>
          <td class="text-accent font-bold">{r.get('RFM_Score',0):.2f}</td>
          <td>${r.get('TotalSpend',0):,.2f}</td>
          <td class="text-green font-medium">${r.get('CLV_Predicted',0):,.2f}</td>
        </tr>"""

    legend_html = ""
    for s, c in COLORS.items():
        if s in seg["Segment"].values:
            legend_html += f'<div class="legend-item"><span class="dot" style="background:{c}"></span>{s}</div>'

    # ── Main Template ──
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Consumer360 | Analytical Suite</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg: {BG}; --card-bg: {CARD_BG}; --text: {TEXT}; --subtext: {SUBTEXT};
            --accent: {ACCENT}; --green: {GREEN}; --red: {RED}; --amber: {AMBER};
            --glass: rgba(255, 255, 255, 0.03); --border: rgba(255, 255, 255, 0.08);
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ 
            font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); 
            line-height: 1.5; overflow-x: hidden; 
        }}
        h1, h2, h3 {{ font-family: 'Outfit', sans-serif; }}

        /* Sidebar & Layout */
        .app-container {{ display: flex; min-height: 100vh; }}
        .sidebar {{
            width: 260px; background: rgba(15, 23, 42, 0.8); backdrop-filter: blur(20px);
            border-right: 1px solid var(--border); padding: 32px 24px;
            display: flex; flex-direction: column; position: fixed; height: 100vh;
        }}
        .main-content {{ flex: 1; margin-left: 260px; padding: 40px; }}

        /* Branding */
        .brand {{ display: flex; align-items: center; gap: 12px; margin-bottom: 48px; }}
        .logo-box {{ 
            width: 40px; height: 40px; background: var(--accent); border-radius: 10px;
            display: grid; place-items: center; color: white; box-shadow: 0 0 20px rgba(139, 92, 246, 0.4);
        }}
        .brand-text h2 {{ font-size: 18px; letter-spacing: -0.5px; }}
        .brand-text p {{ font-size: 11px; color: var(--subtext); text-transform: uppercase; letter-spacing: 1px; }}

        /* Navigation */
        .nav-list {{ list-style: none; }}
        .nav-item {{
            padding: 12px 16px; border-radius: 10px; margin-bottom: 8px; cursor: pointer;
            display: flex; align-items: center; gap: 12px; transition: 0.2s; color: var(--subtext);
        }}
        .nav-item svg {{ width: 20px; opacity: 0.7; }}
        .nav-item:hover {{ background: var(--glass); color: var(--text); }}
        .nav-item.active {{ background: var(--accent); color: white; box-shadow: 0 4px 15px rgba(139, 92, 246, 0.3); }}
        .nav-item.active svg {{ opacity: 1; }}

        /* Header */
        .top-bar {{ display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 32px; }}
        .top-bar h1 {{ font-size: 28px; font-weight: 700; }}
        .export-btn {{
            display: flex; align-items: center; gap: 8px; padding: 10px 20px;
            background: var(--glass); border: 1px solid var(--border); border-radius: 8px;
            font-size: 13px; font-weight: 500; cursor: pointer; transition: 0.2s;
        }}
        .export-btn:hover {{ background: rgba(255,255,255,0.08); }}

        /* KPIs */
        .kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 24px; margin-bottom: 32px; }}
        .kpi-card {{
            background: var(--card-bg); border: 1px solid var(--border); border-radius: 16px;
            padding: 24px; backdrop-filter: blur(10px);
        }}
        .kpi-head {{ display: flex; justify-content: space-between; align-items: flex-start; color: var(--subtext); margin-bottom: 16px; }}
        .kpi-icon {{ color: var(--accent); opacity: 0.8; height: 20px; width: 20px; }}
        .kpi-value {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
        .kpi-label {{ font-size: 13px; font-weight: 500; color: var(--subtext); }}

        /* Dashboard Sections */
        .tab-content {{ display: none; animation: fadeIn 0.4s ease; }}
        .tab-content.active {{ display: block; }}
        @keyframes fadeIn {{ from {{ opacity: 0; transform: translateY(10px); }} to {{ opacity: 1; transform: translateY(0); }} }}

        .chart-grid {{ display: grid; grid-template-columns: 1fr 1.5fr; gap: 24px; margin-bottom: 24px; }}
        .card {{
            background: var(--card-bg); border: 1px solid var(--border); border-radius: 16px;
            padding: 24px; backdrop-filter: blur(10px);
        }}
        .card-title {{ font-size: 14px; font-weight: 600; color: var(--subtext); margin-bottom: 24px; text-transform: uppercase; letter-spacing: 1px; }}

        /* Legend */
        .legend {{ display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 24px; }}
        .legend-item {{ display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--subtext); }}
        .dot {{ width: 8px; height: 8px; border-radius: 50%; }}

        /* Table */
        .table-wrap {{ overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; text-align: left; }}
        th {{ padding: 16px; color: var(--subtext); font-size: 11px; text-transform: uppercase; border-bottom: 1px solid var(--border); }}
        td {{ padding: 16px; font-size: 13px; border-bottom: 1px solid rgba(255,255,255,0.03); }}
        tr:hover td {{ background: rgba(255,255,255,0.02); }}
        .badge {{ padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600; white-space: nowrap; }}
        .score-pill {{ 
            background: var(--glass); width: 24px; height: 24px; border-radius: 6px;
            display: grid; place-items: center; font-weight: 700; font-size: 11px;
        }}
        .font-medium {{ font-weight: 500; }}
        .font-bold {{ font-weight: 700; }}
        .text-accent {{ color: var(--accent); }}
        .text-green {{ color: var(--green); }}

    </style>
</head>
<body>

<div class="app-container">
    <aside class="sidebar">
        <div class="brand">
            <div class="logo-box">
                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path><polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline><line x1="12" y1="22.08" x2="12" y2="12"></line></svg>
            </div>
            <div class="brand-text">
                <h2>Consumer360</h2>
                <p>Advanced Analytics</p>
            </div>
        </div>

        <ul class="nav-list" id="nav-tabs">
            <li class="nav-item active" data-tab="tab-overview">{ICONS['chart']} Overview</li>
            <li class="nav-item" data-tab="tab-analysis">{ICONS['users']} Customer Analysis</li>
            <li class="nav-item" data-tab="tab-data">{ICONS['clv']} Data Registry</li>
        </ul>

        <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid var(--border);">
            <p style="font-size: 11px; color: var(--subtext);">Cycle Date</p>
            <p style="font-size: 12px; font-weight: 500;">Q2 FY 2026</p>
        </div>
    </aside>

    <main class="main-content">
        <div class="top-bar">
            <div>
                <h1>Business Intelligence</h1>
                <p style="color: var(--subtext); font-size: 13px;">Market segmentation and predictive lifetime valuation engine.</p>
            </div>
            <button class="export-btn">
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
                Download Data
            </button>
        </div>

        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-head"><span class="kpi-label">Market Reach</span><span class="kpi-icon">{ICONS['users']}</span></div>
                <div class="kpi-value">{total_customers:,}</div>
                <div style="font-size: 11px; color: var(--green); font-weight: 600;">Active Accounts</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-head"><span class="kpi-label">Stored Equity</span><span class="kpi-icon">{ICONS['revenue']}</span></div>
                <div class="kpi-value" style="color: var(--green);">${total_revenue:,.0f}</div>
                <div style="font-size: 11px; color: var(--subtext); font-weight: 500;">Gross Transaction Value</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-head"><span class="kpi-label">Lifetime Valuation</span><span class="kpi-icon">{ICONS['clv']}</span></div>
                <div class="kpi-value" style="color: var(--amber);">${avg_clv:,.0f}</div>
                <div style="font-size: 11px; color: var(--subtext); font-weight: 500;">Avg Predicted Yield (5yr)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-head"><span class="kpi-label">Customer Quality</span><span class="kpi-icon">{ICONS['chart']}</span></div>
                <div class="kpi-value">{avg_rfm:.2f}</div>
                <div style="font-size: 11px; color: var(--accent); font-weight: 600;">Composite RFM Index</div>
            </div>
        </div>

        <!-- TAB 1: OVERVIEW -->
        <div id="tab-overview" class="tab-content active">
            <div class="chart-grid">
                <div class="card">
                    <div class="card-title">Portfolio Composition</div>
                    <div id="chart-donut"></div>
                    <div class="legend" style="margin-top: 16px;">{legend_html}</div>
                </div>
                <div class="card">
                    <div class="card-title">Revenue Share Treemap</div>
                    <div id="chart-tree"></div>
                </div>
            </div>
        </div>

        <!-- TAB 2: ANALYSIS -->
        <div id="tab-analysis" class="tab-content">
            <div class="chart-grid">
                <div class="card">
                    <div class="card-title">Segment Behavioral Ranking</div>
                    <div id="chart-rfm"></div>
                </div>
                <div class="card">
                    <div class="card-title">Projected Value per Segment</div>
                    <div id="chart-clv"></div>
                </div>
            </div>
            <div class="card">
                <div class="card-title">Behavioral Scatter Mapping (Recency vs Monetary)</div>
                <div id="chart-scatter"></div>
            </div>
        </div>

        <!-- TAB 3: DATA -->
        <div id="tab-data" class="tab-content">
            <div class="card">
                <div class="card-title">Customer Behavioral Registry (Top 100)</div>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Identity</th>
                                <th>Segment</th>
                                <th>R</th><th>F</th><th>M</th>
                                <th>RFM Index</th>
                                <th>Gross Volume</th>
                                <th>LTV Projection</th>
                            </tr>
                        </thead>
                        <tbody>{rows_html}</tbody>
                    </table>
                </div>
            </div>
        </div>
    </main>
</div>

<script>
    const cfg = {{responsive: true, displayModeBar: false}};
    const plots = [
        {{id: 'chart-donut', data: {fig_json(fig_donut)}}},
        {{id: 'chart-rfm', data: {fig_json(fig_rfm)}}},
        {{id: 'chart-clv', data: {fig_json(fig_clv)}}},
        {{id: 'chart-scatter', data: {fig_json(fig_scatter)}}},
        {{id: 'chart-tree', data: {fig_json(fig_tree)}}}
    ];
    
    plots.forEach(p => Plotly.newPlot(p.id, p.data.data, p.data.layout, cfg));

    // Tab Navigation Logic
    document.querySelectorAll('.nav-item').forEach(btn => {{
        btn.addEventListener('click', () => {{
            document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            
            btn.classList.add('active');
            document.getElementById(btn.dataset.tab).classList.add('active');
            window.dispatchEvent(new Event('resize'));
        }});
    }});
</script>
</body>
</html>"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  Premium Dashboard generated successfully!")
    return path


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  CONSUMER360 — DASHBOARD GENERATOR")
    print("=" * 60)

    df  = load_data()
    seg = export_excel(df)
    html_path = build_dashboard(df, seg)

    print()
    print("=" * 60)
    print("  DONE!")
    print(f"  Dashboard : powerbi\\Consumer360_Dashboard.html")
    print(f"  Excel     : powerbi\\Consumer360_PowerBI_Data.xlsx")
    print("=" * 60)
    print()
    print("  Open the dashboard: double-click Consumer360_Dashboard.html")
    print("  For Power BI: File > Get Data > Excel > Consumer360_PowerBI_Data.xlsx")
