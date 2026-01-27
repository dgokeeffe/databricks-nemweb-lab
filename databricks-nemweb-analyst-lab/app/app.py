"""
NEM Price Analytics Dashboard - Analyst Workshop

A simple Dash app that queries the curated_nem_prices view in Unity Catalog.
Shows how analysts can build self-service dashboards on governed data.

Deploy to Databricks Apps:
    databricks apps create nem-analytics
    databricks apps deploy nem-analytics --source-code-path /Workspace/.../app
"""

import os
from datetime import datetime, timedelta

import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Databricks SQL connector (available in Databricks Apps runtime)
try:
    from databricks import sql as databricks_sql
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

# Configuration from environment (set by Databricks Apps)
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
_host = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_HOST = _host.replace("https://", "").replace("http://", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# Data configuration
CATALOG = "workspace"
SCHEMA = "nemweb_lab"
TABLE = "curated_nem_prices"

# NEM regions and colors
REGION_COLORS = {
    "NSW1": "#1f77b4",
    "VIC1": "#ff7f0e",
    "QLD1": "#2ca02c",
    "SA1": "#d62728",
    "TAS1": "#9467bd",
}


def query_nem_data(days: int = 7) -> pd.DataFrame:
    """Query NEM price data from Unity Catalog."""
    if not DATABRICKS_AVAILABLE or not all([WAREHOUSE_ID, DATABRICKS_HOST, DATABRICKS_TOKEN]):
        return generate_sample_data(days)

    try:
        with databricks_sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            access_token=DATABRICKS_TOKEN
        ) as conn:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT
                        interval_start,
                        date,
                        region_id,
                        rrp,
                        demand_mw,
                        is_peak
                    FROM {CATALOG}.{SCHEMA}.{TABLE}
                    WHERE date >= current_date() - INTERVAL {days} DAYS
                    ORDER BY interval_start DESC
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        print(f"Query error: {e}")
        return generate_sample_data(days)


def generate_sample_data(days: int = 7) -> pd.DataFrame:
    """Generate sample data for local development."""
    import numpy as np

    timestamps = pd.date_range(
        end=datetime.now(),
        periods=days * 288,  # 5-min intervals
        freq="5min"
    )

    data = []
    for ts in timestamps:
        hour = ts.hour
        base_price = 50 + 30 * np.sin((hour - 6) * np.pi / 12)

        for region in REGION_COLORS.keys():
            factor = {"NSW1": 1.0, "VIC1": 0.95, "QLD1": 1.1, "SA1": 1.2, "TAS1": 0.85}[region]
            price = max(0, base_price * factor + np.random.normal(0, 15))
            demand = {"NSW1": 8000, "VIC1": 5500, "QLD1": 6500, "SA1": 2000, "TAS1": 1200}[region]
            demand = demand * (0.8 + 0.4 * np.sin((hour - 8) * np.pi / 12))

            data.append({
                "interval_start": ts,
                "date": ts.date(),
                "region_id": region,
                "rrp": round(price, 2),
                "demand_mw": round(demand, 0),
                "is_peak": 17 <= hour <= 20,
            })

    return pd.DataFrame(data)


# Initialize app
app = dash.Dash(__name__, title="NEM Price Analytics")
server = app.server

# Layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("NEM Price Analytics", style={"margin": 0, "color": "white"}),
        html.P("Powered by Databricks Unity Catalog", style={"margin": 0, "opacity": 0.7, "color": "white"}),
    ], style={"backgroundColor": "#1B3A4B", "padding": "20px", "marginBottom": "20px"}),

    # Controls
    html.Div([
        html.Label("Date Range:", style={"fontWeight": "bold", "marginRight": "10px"}),
        dcc.Dropdown(
            id="date-range",
            options=[
                {"label": "Last 7 Days", "value": 7},
                {"label": "Last 14 Days", "value": 14},
                {"label": "Last 30 Days", "value": 30},
            ],
            value=7,
            style={"width": "150px", "display": "inline-block"},
            clearable=False,
        ),
        html.Label("Regions:", style={"fontWeight": "bold", "marginLeft": "30px", "marginRight": "10px"}),
        dcc.Dropdown(
            id="regions",
            options=[{"label": r, "value": r} for r in REGION_COLORS.keys()],
            value=list(REGION_COLORS.keys()),
            multi=True,
            style={"width": "400px", "display": "inline-block"},
        ),
    ], style={"padding": "0 20px", "marginBottom": "20px"}),

    # KPI Cards
    html.Div(id="kpi-cards", style={
        "display": "flex",
        "gap": "20px",
        "padding": "0 20px",
        "marginBottom": "20px",
    }),

    # Charts row
    html.Div([
        html.Div([
            html.H3("Daily Average Price by Region"),
            dcc.Graph(id="price-trend"),
        ], style={"flex": 2}),
        html.Div([
            html.H3("Price Volatility"),
            dcc.Graph(id="volatility-chart"),
        ], style={"flex": 1}),
    ], style={"display": "flex", "gap": "20px", "padding": "0 20px", "marginBottom": "20px"}),

    # Peak vs Off-Peak
    html.Div([
        html.H3("Peak vs Off-Peak Prices"),
        dcc.Graph(id="peak-comparison"),
    ], style={"padding": "0 20px", "marginBottom": "20px"}),

    # Data store
    dcc.Store(id="nem-data"),

    # Footer
    html.Div([
        html.P([
            "Source: ",
            html.Code(f"{CATALOG}.{SCHEMA}.{TABLE}"),
            " | Data: Real AEMO NEMWEB",
        ], style={"margin": 0, "textAlign": "center", "opacity": 0.7}),
    ], style={"backgroundColor": "#f5f5f5", "padding": "15px"}),
])


@callback(Output("nem-data", "data"), Input("date-range", "value"))
def load_data(days):
    df = query_nem_data(days)
    return df.to_json(date_format="iso", orient="split")


@callback(Output("kpi-cards", "children"), Input("nem-data", "data"), Input("regions", "value"))
def update_kpis(data, regions):
    if not data or not regions:
        return []

    df = pd.read_json(data, orient="split")
    df = df[df["region_id"].isin(regions)]

    cards = []
    for region in regions:
        region_data = df[df["region_id"] == region]
        if region_data.empty:
            continue

        avg_price = region_data["rrp"].mean()
        max_price = region_data["rrp"].max()
        volatility = region_data["rrp"].std()

        card = html.Div([
            html.Div(region, style={"fontWeight": "bold", "fontSize": "16px", "color": REGION_COLORS[region]}),
            html.Div(f"${avg_price:.2f}", style={"fontSize": "28px", "fontWeight": "bold"}),
            html.Div(f"Avg $/MWh", style={"opacity": 0.6, "fontSize": "12px"}),
            html.Div([
                html.Span(f"Max: ${max_price:.0f}", style={"marginRight": "15px"}),
                html.Span(f"Vol: {volatility:.1f}"),
            ], style={"fontSize": "12px", "marginTop": "5px", "opacity": 0.7}),
        ], style={
            "backgroundColor": "white",
            "border": f"2px solid {REGION_COLORS[region]}",
            "borderRadius": "8px",
            "padding": "15px",
            "minWidth": "140px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
        })
        cards.append(card)

    return cards


@callback(Output("price-trend", "figure"), Input("nem-data", "data"), Input("regions", "value"))
def update_price_trend(data, regions):
    if not data or not regions:
        return go.Figure()

    df = pd.read_json(data, orient="split")
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["region_id"].isin(regions)]

    daily = df.groupby(["date", "region_id"])["rrp"].mean().reset_index()

    fig = px.line(
        daily, x="date", y="rrp", color="region_id",
        color_discrete_map=REGION_COLORS,
        labels={"rrp": "Avg Price ($/MWh)", "date": "Date", "region_id": "Region"}
    )
    fig.update_layout(margin=dict(l=40, r=20, t=20, b=40), legend=dict(orientation="h", y=1.1))
    return fig


@callback(Output("volatility-chart", "figure"), Input("nem-data", "data"), Input("regions", "value"))
def update_volatility(data, regions):
    if not data or not regions:
        return go.Figure()

    df = pd.read_json(data, orient="split")
    df = df[df["region_id"].isin(regions)]

    vol = df.groupby("region_id")["rrp"].std().reset_index()
    vol.columns = ["region_id", "volatility"]

    fig = px.bar(
        vol, x="region_id", y="volatility", color="region_id",
        color_discrete_map=REGION_COLORS,
        labels={"volatility": "Std Dev ($/MWh)", "region_id": "Region"}
    )
    fig.update_layout(margin=dict(l=40, r=20, t=20, b=40), showlegend=False)
    return fig


@callback(Output("peak-comparison", "figure"), Input("nem-data", "data"), Input("regions", "value"))
def update_peak_comparison(data, regions):
    if not data or not regions:
        return go.Figure()

    df = pd.read_json(data, orient="split")
    df = df[df["region_id"].isin(regions)]

    summary = df.groupby(["region_id", "is_peak"])["rrp"].mean().reset_index()
    summary["period"] = summary["is_peak"].map({True: "Peak (17:00-21:00)", False: "Off-Peak"})

    fig = px.bar(
        summary, x="region_id", y="rrp", color="period", barmode="group",
        color_discrete_map={"Peak (17:00-21:00)": "#d62728", "Off-Peak": "#2ca02c"},
        labels={"rrp": "Avg Price ($/MWh)", "region_id": "Region", "period": "Period"}
    )
    fig.update_layout(margin=dict(l=40, r=20, t=20, b=40), legend=dict(orientation="h", y=1.1))
    return fig


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
