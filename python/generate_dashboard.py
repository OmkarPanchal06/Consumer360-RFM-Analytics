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
# COLOR PALETTE
# ─────────────────────────────────────────────
COLORS = {
    "Champions":                "#6C63FF",
    "Loyal Customers":          "#3ECFCF",
    "Potential Loyalists":      "#56D364",
    "Customers Needing Attention": "#F0A500",
    "At Risk":                  "#FF6B6B",
    "Hibernating":              "#9CA3AF",
    "New/Low Value":            "#60A5FA",
}
BG        = "#0F172A"
CARD_BG   = "#1E293B"
TEXT      = "#F1F5F9"
SUBTEXT   = "#94A3B8"
ACCENT    = "#6C63FF"
GREEN     = "#56D364"
RED       = "#FF6B6B"
AMBER     = "#F0A500"


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
    print(f"Building HTML dashboard...")

    # ── KPI values ────────────────────────────
    total_customers = len(df)
    total_revenue   = df["TotalSpend"].sum() if "TotalSpend" in df.columns else 0
    avg_clv         = df["CLV_Predicted"].mean() if "CLV_Predicted" in df.columns else 0
    top_segment     = seg.iloc[0]["Segment"] if len(seg) > 0 else "N/A"
    avg_rfm         = df["RFM_Score"].mean() if "RFM_Score" in df.columns else 0
    champ_pct       = (
        len(df[df["Segment"].isin(["Champions", "Loyal Customers"])]) / total_customers * 100
        if total_customers > 0 else 0
    )

    seg_colors = [COLORS.get(s, ACCENT) for s in seg["Segment"]]

    # ── Chart 1: Segment donut ─────────────────
    fig_donut = go.Figure(go.Pie(
        labels=seg["Segment"],
        values=seg["Customers"],
        hole=0.55,
        marker=dict(colors=seg_colors, line=dict(color=BG, width=2)),
        textinfo="label+percent",
        textfont=dict(color=TEXT, size=11),
    ))
    fig_donut.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT),
        showlegend=False,
        margin=dict(t=10, b=10, l=10, r=10),
        height=300,
        annotations=[dict(text=f"<b>{total_customers}</b><br>Customers",
                          x=0.5, y=0.5, font_size=16, font_color=TEXT,
                          showarrow=False)]
    )

    # ── Chart 2: RFM Score bar ─────────────────
    fig_rfm = go.Figure(go.Bar(
        x=seg["Segment"],
        y=seg["Avg_RFM_Score"].round(2),
        marker=dict(color=seg_colors, line=dict(color=BG, width=1)),
        text=seg["Avg_RFM_Score"].round(2),
        textposition="outside",
        textfont=dict(color=TEXT),
    ))
    fig_rfm.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT),
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=TEXT)),
        yaxis=dict(showgrid=True, gridcolor="#334155", range=[0, 6]),
        margin=dict(t=10, b=80, l=40, r=10),
        height=300,
    )

    # ── Chart 3: CLV by segment ────────────────
    fig_clv = go.Figure(go.Bar(
        x=seg["Segment"],
        y=seg["Avg_CLV"].round(0),
        marker=dict(color=seg_colors, line=dict(color=BG, width=1)),
        text=["$" + f"{v:,.0f}" for v in seg["Avg_CLV"]],
        textposition="outside",
        textfont=dict(color=TEXT),
    ))
    fig_clv.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT),
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=TEXT)),
        yaxis=dict(showgrid=True, gridcolor="#334155"),
        margin=dict(t=10, b=80, l=60, r=10),
        height=300,
    )

    # ── Chart 4: Scatter R vs M score ──────────
    scatter_colors = [COLORS.get(s, ACCENT) for s in df["Segment"]]
    fig_scatter = go.Figure(go.Scatter(
        x=df["R_Score"],
        y=df["M_Score"],
        mode="markers+text",
        marker=dict(size=16, color=scatter_colors,
                    line=dict(color=BG, width=1)),
        text=df["CustomerName"] if "CustomerName" in df.columns else df["CustomerID"],
        textposition="top center",
        textfont=dict(color=TEXT, size=10),
        customdata=df[["Segment", "RFM_Score"]],
        hovertemplate=(
            "<b>%{text}</b><br>"
            "Segment: %{customdata[0]}<br>"
            "R Score: %{x}  M Score: %{y}<br>"
            "RFM Score: %{customdata[1]:.2f}<extra></extra>"
        )
    ))
    fig_scatter.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT),
        xaxis=dict(title="Recency Score", showgrid=True, gridcolor="#334155",
                   range=[0, 6], tickvals=[1,2,3,4,5]),
        yaxis=dict(title="Monetary Score", showgrid=True, gridcolor="#334155",
                   range=[0, 6], tickvals=[1,2,3,4,5]),
        margin=dict(t=10, b=50, l=60, r=10),
        height=320,
    )

    # ── Chart 5: Spend treemap ─────────────────
    fig_tree = px.treemap(
        seg,
        path=["Segment"],
        values="Total_Spend",
        color="Avg_RFM_Score",
        color_continuous_scale=[[0, "#334155"], [0.5, ACCENT], [1, GREEN]],
        custom_data=["Customers", "Avg_CLV"],
    )
    fig_tree.update_traces(
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Total Spend: $%{value:,.0f}<br>"
            "Customers: %{customdata[0]}<br>"
            "Avg CLV: $%{customdata[1]:,.0f}<extra></extra>"
        ),
        textfont=dict(color="white", size=13),
    )
    fig_tree.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=5, b=5, l=5, r=5),
        height=280,
        coloraxis_showscale=False,
    )

    # ── Convert figs to JSON ───────────────────
    def fig_json(fig):
        return fig.to_json()

    # ── Customer table rows ────────────────────
    table_cols = ["CustomerName", "Segment", "R_Score", "F_Score", "M_Score",
                  "RFM_Score", "TotalSpend", "CLV_Predicted"]
    table_cols = [c for c in table_cols if c in df.columns]
    table_df = df[table_cols].copy()

    def seg_badge(s):
        c = COLORS.get(s, ACCENT)
        return f'<span style="background:{c}22;color:{c};padding:2px 10px;border-radius:12px;font-size:11px;border:1px solid {c}55">{s}</span>'

    rows_html = ""
    for _, r in table_df.iterrows():
        name   = r.get("CustomerName", r.get("CustomerID", ""))
        seg_b  = seg_badge(r.get("Segment", ""))
        r_s    = int(r.get("R_Score", 0))
        f_s    = int(r.get("F_Score", 0))
        m_s    = int(r.get("M_Score", 0))
        rfm_s  = f'{r.get("RFM_Score", 0):.2f}'
        spend  = f'${r.get("TotalSpend", 0):,.2f}'
        clv    = f'${r.get("CLV_Predicted", 0):,.2f}'
        rows_html += f"""
        <tr>
          <td>{name}</td>
          <td>{seg_b}</td>
          <td><span class="score">{r_s}</span></td>
          <td><span class="score">{f_s}</span></td>
          <td><span class="score">{m_s}</span></td>
          <td><b style="color:{ACCENT}">{rfm_s}</b></td>
          <td>{spend}</td>
          <td style="color:{GREEN}">{clv}</td>
        </tr>"""

    # ── Legend items ──────────────────────────
    legend_html = ""
    for s, c in COLORS.items():
        count = len(df[df["Segment"] == s]) if s in df["Segment"].values else 0
        if count > 0:
            legend_html += f'<div class="legend-item"><span style="width:10px;height:10px;border-radius:50%;background:{c};display:inline-block;margin-right:6px"></span>{s} <span style="color:{SUBTEXT}">({count})</span></div>'

    # ─────────────────────────────────────────
    # HTML TEMPLATE
    # ─────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Consumer360 — Analytics Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Inter', sans-serif; background: {BG}; color: {TEXT}; min-height: 100vh; }}

  /* ── HEADER ── */
  .header {{
    background: linear-gradient(135deg, #1E1B4B 0%, #0F172A 60%, #0D1B2A 100%);
    padding: 28px 40px;
    border-bottom: 1px solid #1E293B;
    display: flex; align-items: center; justify-content: space-between;
  }}
  .header-left h1 {{ font-size: 24px; font-weight: 700; color: {TEXT}; }}
  .header-left p  {{ font-size: 13px; color: {SUBTEXT}; margin-top: 4px; }}
  .header-badge {{
    background: {ACCENT}22; color: {ACCENT};
    padding: 6px 16px; border-radius: 20px;
    font-size: 12px; font-weight: 600;
    border: 1px solid {ACCENT}44;
  }}

  /* ── KPI CARDS ── */
  .kpi-row {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; padding: 24px 40px 0; }}
  .kpi {{
    background: {CARD_BG}; border-radius: 12px;
    padding: 20px; border: 1px solid #334155;
    transition: transform 0.2s, box-shadow 0.2s;
  }}
  .kpi:hover {{ transform: translateY(-2px); box-shadow: 0 8px 30px rgba(108,99,255,0.15); }}
  .kpi-label {{ font-size: 11px; color: {SUBTEXT}; text-transform: uppercase; letter-spacing: 0.05em; }}
  .kpi-value {{ font-size: 26px; font-weight: 700; margin: 6px 0 2px; }}
  .kpi-sub   {{ font-size: 11px; color: {SUBTEXT}; }}

  /* ── GRID ── */
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 20px 40px 0; }}
  .grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; padding: 0 40px; }}
  .grid-full {{ padding: 16px 40px; }}

  /* ── CARD ── */
  .card {{
    background: {CARD_BG}; border-radius: 12px;
    border: 1px solid #334155; padding: 20px;
    transition: box-shadow 0.2s;
  }}
  .card:hover {{ box-shadow: 0 4px 20px rgba(0,0,0,0.3); }}
  .card-title {{ font-size: 13px; font-weight: 600; color: {SUBTEXT}; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 12px; }}

  /* ── LEGEND ── */
  .legend {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 12px; }}
  .legend-item {{ font-size: 12px; color: {TEXT}; }}

  /* ── TABLE ── */
  .table-wrap {{ overflow-x: auto; border-radius: 12px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  thead tr {{ background: #334155; }}
  thead th {{ padding: 12px 16px; text-align: left; font-weight: 600; color: {SUBTEXT}; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; }}
  tbody tr {{ border-bottom: 1px solid #1E293B; transition: background 0.15s; }}
  tbody tr:hover {{ background: #334155; }}
  tbody td {{ padding: 12px 16px; }}
  .score {{
    display: inline-block; width: 28px; height: 28px; line-height: 28px;
    text-align: center; border-radius: 6px;
    background: {ACCENT}22; color: {ACCENT}; font-weight: 700; font-size: 13px;
  }}

  /* ── FOOTER ── */
  .footer {{ text-align: center; padding: 30px; color: {SUBTEXT}; font-size: 12px; margin-top: 20px; }}
  .footer a {{ color: {ACCENT}; text-decoration: none; }}
</style>
</head>
<body>

<!-- HEADER -->
<div class="header">
  <div class="header-left">
    <h1>Consumer360 &mdash; RFM Analytics Dashboard</h1>
    <p>Customer Lifetime Value &amp; Segmentation Engine</p>
  </div>
  <span class="header-badge">LIVE DATA &bull; {total_customers} Customers</span>
</div>

<!-- KPI CARDS -->
<div class="kpi-row">
  <div class="kpi">
    <div class="kpi-label">Total Customers</div>
    <div class="kpi-value" style="color:{ACCENT}">{total_customers:,}</div>
    <div class="kpi-sub">Across {len(seg)} segments</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Total Revenue</div>
    <div class="kpi-value" style="color:{GREEN}">${total_revenue:,.0f}</div>
    <div class="kpi-sub">All transactions</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Avg Predicted CLV</div>
    <div class="kpi-value" style="color:{AMBER}">${avg_clv:,.0f}</div>
    <div class="kpi-sub">5-year forecast</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Avg RFM Score</div>
    <div class="kpi-value" style="color:{TEXT}">{avg_rfm:.2f} <span style="font-size:14px;color:{SUBTEXT}">/5</span></div>
    <div class="kpi-sub">Weighted R·F·M</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">High-Value Rate</div>
    <div class="kpi-value" style="color:{GREEN}">{champ_pct:.0f}%</div>
    <div class="kpi-sub">Champions + Loyal</div>
  </div>
</div>

<!-- ROW 1: Donut + RFM Bar -->
<div class="grid-2" style="margin-top:16px">
  <div class="card">
    <div class="card-title">Customer Segments</div>
    <div class="legend">{legend_html}</div>
    <div id="chart-donut"></div>
  </div>
  <div class="card">
    <div class="card-title">Avg RFM Score by Segment</div>
    <div id="chart-rfm"></div>
  </div>
</div>

<!-- ROW 2: CLV Bar + Scatter -->
<div class="grid-2" style="padding: 16px 40px 0">
  <div class="card">
    <div class="card-title">Avg Predicted CLV by Segment (5-Year)</div>
    <div id="chart-clv"></div>
  </div>
  <div class="card">
    <div class="card-title">Recency vs Monetary Map</div>
    <div id="chart-scatter"></div>
  </div>
</div>

<!-- ROW 3: Treemap full -->
<div class="grid-full" style="margin-top:0">
  <div class="card">
    <div class="card-title">Revenue Treemap — Segment Share of Total Spend</div>
    <div id="chart-tree"></div>
  </div>
</div>

<!-- ROW 4: Customer Table -->
<div class="grid-full">
  <div class="card">
    <div class="card-title">Customer Detail Table</div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Customer</th>
            <th>Segment</th>
            <th>R</th><th>F</th><th>M</th>
            <th>RFM Score</th>
            <th>Total Spend</th>
            <th>CLV (5yr)</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>
</div>

<div class="footer">
  Consumer360 RFM Analytics &bull;
  <a href="https://github.com/OmkarPanchal06/Consumer360-RFM-Analytics" target="_blank">GitHub</a>
  &bull; Built with Python + Plotly
</div>

<script>
  var cfg = {{responsive: true, displayModeBar: false}};
  Plotly.newPlot('chart-donut',   {fig_json(fig_donut)}.data,   {fig_json(fig_donut)}.layout,   cfg);
  Plotly.newPlot('chart-rfm',     {fig_json(fig_rfm)}.data,     {fig_json(fig_rfm)}.layout,     cfg);
  Plotly.newPlot('chart-clv',     {fig_json(fig_clv)}.data,     {fig_json(fig_clv)}.layout,     cfg);
  Plotly.newPlot('chart-scatter', {fig_json(fig_scatter)}.data, {fig_json(fig_scatter)}.layout, cfg);
  Plotly.newPlot('chart-tree',    {fig_json(fig_tree)}.data,    {fig_json(fig_tree)}.layout,    cfg);
</script>
</body>
</html>"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  Dashboard saved!")
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
