"""
NEM Price Analytics Dashboard - Analyst Workshop

AGL-themed Dash app for analyst personas. It prefers the latest ML workshop
pipeline gold tables and falls back to the legacy curated view.
"""

import os
from datetime import datetime

import dash
from dash import Input, Output, callback, dcc, html
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

try:
    from databricks import sql as databricks_sql

    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
_host = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_HOST = _host.replace("https://", "").replace("http://", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

CATALOG = os.environ.get("NEMWEB_CATALOG", "workspace")
SCHEMA = os.environ.get("NEMWEB_SCHEMA", "ml_workshops")

GOLD_PRICE_TABLE = "gold_price_hourly"
GOLD_DEMAND_TABLE = "gold_demand_hourly"
LEGACY_TABLE = "curated_nem_prices"

# AGL-inspired palette
AGL_NAVY = "#002B5C"
AGL_BLUE = "#0066CC"
AGL_CYAN = "#00B5E2"
AGL_BG = "#F4F8FC"
AGL_TEXT = "#0F172A"
AGL_MUTED = "#64748B"

REGION_COLORS = {
    "NSW1": "#0066CC",
    "VIC1": "#00B5E2",
    "QLD1": "#009B77",
    "SA1": "#D94F70",
    "TAS1": "#6B5B95",
}


def _sql_query(query: str) -> pd.DataFrame | None:
    if not DATABRICKS_AVAILABLE or not all([WAREHOUSE_ID, DATABRICKS_HOST, DATABRICKS_TOKEN]):
        return None
    try:
        with databricks_sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            access_token=DATABRICKS_TOKEN,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        print(f"Query error: {e}")
        return None


def query_nem_data(days: int = 7) -> tuple[pd.DataFrame, str]:
    """Query NEM data; prefer new gold tables, then legacy curated view."""
    gold_df = _sql_query(f"""
        SELECT
            p.hour AS interval_start,
            CAST(p.hour AS DATE) AS date,
            p.region_id,
            p.avg_rrp AS rrp,
            d.avg_demand_mw AS demand_mw,
            CASE WHEN HOUR(p.hour) BETWEEN 17 AND 20 THEN TRUE ELSE FALSE END AS is_peak,
            p.stddev_rrp AS volatility
        FROM {CATALOG}.{SCHEMA}.{GOLD_PRICE_TABLE} p
        LEFT JOIN {CATALOG}.{SCHEMA}.{GOLD_DEMAND_TABLE} d
          ON p.region_id = d.region_id AND p.hour = d.hour
        WHERE p.hour >= current_timestamp() - INTERVAL {days} DAYS
        ORDER BY interval_start DESC
        LIMIT 20000
    """)
    if gold_df is not None and not gold_df.empty:
        gold_df["interval_start"] = pd.to_datetime(gold_df["interval_start"])
        gold_df["date"] = pd.to_datetime(gold_df["date"])
        gold_df["is_peak"] = gold_df["is_peak"].astype(bool)
        return gold_df, f"{CATALOG}.{SCHEMA}.{GOLD_PRICE_TABLE}/{GOLD_DEMAND_TABLE}"

    legacy_df = _sql_query(f"""
        SELECT
            interval_start,
            date,
            region_id,
            rrp,
            demand_mw,
            is_peak,
            CAST(NULL AS DOUBLE) AS volatility
        FROM {CATALOG}.{SCHEMA}.{LEGACY_TABLE}
        WHERE date >= current_date() - INTERVAL {days} DAYS
        ORDER BY interval_start DESC
        LIMIT 20000
    """)
    if legacy_df is not None and not legacy_df.empty:
        legacy_df["interval_start"] = pd.to_datetime(legacy_df["interval_start"])
        legacy_df["date"] = pd.to_datetime(legacy_df["date"])
        legacy_df["is_peak"] = legacy_df["is_peak"].astype(bool)
        return legacy_df, f"{CATALOG}.{SCHEMA}.{LEGACY_TABLE}"

    return generate_sample_data(days), "sample_data"


def generate_sample_data(days: int = 7) -> pd.DataFrame:
    """Generate sample data for local development."""
    timestamps = pd.date_range(end=datetime.now(), periods=days * 24, freq="1h")
    data = []
    for ts in timestamps:
        hour = ts.hour
        base_price = 60 + 35 * np.sin((hour - 6) * np.pi / 12)
        for region in REGION_COLORS.keys():
            factor = {"NSW1": 1.0, "VIC1": 0.95, "QLD1": 1.08, "SA1": 1.2, "TAS1": 0.9}[region]
            price = max(0, base_price * factor + np.random.normal(0, 10))
            demand = {"NSW1": 8200, "VIC1": 5600, "QLD1": 6700, "SA1": 2200, "TAS1": 1300}[region]
            demand = demand * (0.8 + 0.35 * np.sin((hour - 8) * np.pi / 12))
            data.append(
                {
                    "interval_start": ts,
                    "date": ts.date(),
                    "region_id": region,
                    "rrp": round(price, 2),
                    "demand_mw": round(demand, 0),
                    "is_peak": 17 <= hour <= 20,
                    "volatility": abs(np.random.normal(12, 4)),
                }
            )
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    return df


app = dash.Dash(
    __name__,
    title="AGL NEM Analytics",
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
    ],
)
server = app.server

app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.H1(
                            "AGL NEM Market Analytics",
                            style={"margin": 0, "color": "white", "fontWeight": "700"},
                        ),
                        html.P(
                            "Analyst view over Databricks governed NEM price and demand data",
                            style={"margin": "6px 0 0 0", "color": "#E2ECF8"},
                        ),
                    ]
                ),
                html.Div(
                    "AGL THEME",
                    style={
                        "backgroundColor": AGL_CYAN,
                        "color": AGL_NAVY,
                        "fontWeight": "700",
                        "fontSize": "11px",
                        "padding": "6px 10px",
                        "borderRadius": "20px",
                    },
                ),
            ],
            style={
                "background": f"linear-gradient(120deg, {AGL_NAVY} 0%, {AGL_BLUE} 100%)",
                "padding": "22px 28px",
                "borderBottom": f"4px solid {AGL_CYAN}",
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
            },
        ),
        html.Div(
            [
                html.Label(
                    "Date Range",
                    style={"fontWeight": "600", "marginRight": "10px", "color": AGL_TEXT},
                ),
                dcc.Dropdown(
                    id="date-range",
                    options=[
                        {"label": "Last 7 Days", "value": 7},
                        {"label": "Last 14 Days", "value": 14},
                        {"label": "Last 30 Days", "value": 30},
                    ],
                    value=7,
                    style={"width": "170px", "display": "inline-block"},
                    clearable=False,
                ),
                html.Label(
                    "Regions",
                    style={
                        "fontWeight": "600",
                        "marginLeft": "30px",
                        "marginRight": "10px",
                        "color": AGL_TEXT,
                    },
                ),
                dcc.Dropdown(
                    id="regions",
                    options=[{"label": r, "value": r} for r in REGION_COLORS.keys()],
                    value=list(REGION_COLORS.keys()),
                    multi=True,
                    style={"width": "420px", "display": "inline-block"},
                ),
            ],
            style={
                "padding": "16px 20px",
                "backgroundColor": "white",
                "borderBottom": "1px solid #E5EDF6",
            },
        ),
        html.Div(
            id="kpi-cards",
            style={
                "display": "flex",
                "gap": "14px",
                "padding": "20px",
                "flexWrap": "wrap",
            },
        ),
        html.Div(
            [
                html.Div(
                    [html.H3("Daily Average Price by Region"), dcc.Graph(id="price-trend")],
                    style={
                        "flex": 2,
                        "backgroundColor": "white",
                        "borderRadius": "10px",
                        "padding": "10px",
                        "boxShadow": "0 1px 3px rgba(0, 0, 0, 0.08)",
                    },
                ),
                html.Div(
                    [html.H3("Price Volatility"), dcc.Graph(id="volatility-chart")],
                    style={
                        "flex": 1,
                        "backgroundColor": "white",
                        "borderRadius": "10px",
                        "padding": "10px",
                        "boxShadow": "0 1px 3px rgba(0, 0, 0, 0.08)",
                    },
                ),
            ],
            style={"display": "flex", "gap": "20px", "padding": "0 20px", "marginBottom": "20px"},
        ),
        html.Div(
            [html.H3("Peak vs Off-Peak Prices"), dcc.Graph(id="peak-comparison")],
            style={
                "padding": "10px 20px",
                "margin": "0 20px 20px 20px",
                "backgroundColor": "white",
                "borderRadius": "10px",
                "boxShadow": "0 1px 3px rgba(0, 0, 0, 0.08)",
            },
        ),
        dcc.Store(id="nem-data"),
        dcc.Store(id="source-name"),
        html.Div(
            [
                html.P(
                    [
                        "Source: ",
                        html.Code(id="source-label"),
                        " | Pipeline-first (gold tables), curated fallback | ",
                        html.Span(id="last-updated"),
                    ],
                    style={"margin": 0, "textAlign": "center", "color": AGL_MUTED},
                ),
            ],
            style={"backgroundColor": "#EDF4FB", "padding": "14px"},
        ),
    ],
    style={"fontFamily": "Inter, sans-serif", "backgroundColor": AGL_BG, "minHeight": "100vh"},
)


@callback(Output("nem-data", "data"), Output("source-name", "data"), Input("date-range", "value"))
def load_data(days):
    df, source = query_nem_data(days)
    return df.to_json(date_format="iso", orient="split"), source


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

        card = html.Div(
            [
                html.Div(
                    region,
                    style={"fontWeight": "700", "fontSize": "16px", "color": REGION_COLORS[region]},
                ),
                html.Div(f"${avg_price:.2f}", style={"fontSize": "30px", "fontWeight": "700", "color": AGL_TEXT}),
                html.Div("Avg $/MWh", style={"opacity": 0.7, "fontSize": "12px", "color": AGL_MUTED}),
                html.Div(
                    [
                        html.Span(f"Max: ${max_price:.0f}", style={"marginRight": "15px"}),
                        html.Span(f"Vol: {volatility:.1f}"),
                    ],
                    style={"fontSize": "12px", "marginTop": "6px", "opacity": 0.9, "color": AGL_MUTED},
                ),
            ],
            style={
                "backgroundColor": "white",
                "borderTop": f"3px solid {REGION_COLORS[region]}",
                "borderRadius": "8px",
                "padding": "14px 16px",
                "minWidth": "170px",
                "boxShadow": "0 1px 3px rgba(0, 0, 0, 0.08)",
            },
        )
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
    fig.update_layout(
        margin=dict(l=40, r=20, t=20, b=40),
        legend=dict(orientation="h", y=1.1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Inter, sans-serif", color=AGL_TEXT),
        xaxis=dict(showgrid=True, gridcolor="#E5EDF6"),
        yaxis=dict(showgrid=True, gridcolor="#E5EDF6"),
    )
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
    fig.update_layout(
        margin=dict(l=40, r=20, t=20, b=40),
        showlegend=False,
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Inter, sans-serif", color=AGL_TEXT),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#E5EDF6"),
    )
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
        color_discrete_map={"Peak (17:00-21:00)": "#D94F70", "Off-Peak": "#009B77"},
        labels={"rrp": "Avg Price ($/MWh)", "region_id": "Region", "period": "Period"}
    )
    fig.update_layout(
        margin=dict(l=40, r=20, t=20, b=40),
        legend=dict(orientation="h", y=1.1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Inter, sans-serif", color=AGL_TEXT),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#E5EDF6"),
    )
    return fig


@callback(
    Output("source-label", "children"),
    Output("last-updated", "children"),
    Input("source-name", "data"),
)
def update_footer(source_name):
    source = source_name or "unknown"
    now_str = datetime.now().strftime("%d %b %Y %H:%M:%S")
    return source, f"Last updated: {now_str}"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
